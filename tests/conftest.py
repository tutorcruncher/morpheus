import asyncio
import re
import uuid

import pytest
from aiohttp.web import Application, HTTPForbidden, Response, json_response

from morpheus.app.es import ElasticSearch
from morpheus.app.main import create_app
from morpheus.app.settings import Settings


@pytest.fixture(scope='session')
def setup_elastic_search():
    loop = asyncio.new_event_loop()
    es = ElasticSearch(settings=Settings(auth_key='x', mandrill_key='x'), loop=loop)
    loop.run_until_complete(es.create_indices(True))
    es.close()


async def mandrill_send_view(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    to_email = data['message']['to'][0]['email']
    return json_response([
        {
            'email': to_email,
            '_id': re.sub(r'[^a-zA-Z0-9\-]', '', f'mandrill-{to_email}'),
            'status': 'queued',
        }
    ])


async def mandrill_sub_account_add(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    sa_id = data['id']
    if sa_id == 'broken':
        return json_response({'error': 'snap something unknown went wrong'}, status=500)
    elif sa_id in request.app['mandrill_subaccounts']:
        return json_response({'message': f'A subaccount with id {sa_id} already exists'}, status=500)
    else:
        request.app['mandrill_subaccounts'][sa_id] = data
        return json_response({'message': "subaccount created (this isn't the same response as mandrill)"})


async def mandrill_sub_account_info(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    sa_id = data['id']
    sa_info = request.app['mandrill_subaccounts'].get(sa_id)
    if sa_info:
        return json_response({
            'subaccount_info': sa_info,
            'sent_total': 200 if sa_id == 'lots-sent' else 42,
        })


async def messagebird_hlr_post(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    return Response(status=201)


async def messagebird_lookup(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    return json_response({
        'hlr': {
            'status': 'active',
            'network': 23430,
        }
    })


async def messagebird_send(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    data = await request.json()
    return json_response({
        'id': '6a23b2037595620ca8459a3b00026003',
        'recipients': {
            'totalCount': len(data['recipients']),
        }
    }, status=201)


async def messagebird_pricing(request):
    if not request.query.get('username') == 'mb-username':
        raise HTTPForbidden(text='bad username')
    if not request.query.get('password') == 'mb-password':
        raise HTTPForbidden(text='bad password')
    return json_response([
        {
            'mcc': '0',
            'country_name': 'Default rate',
            'rate': '0.0400',
        },
        {
            'mcc': '234',
            'country_name': 'United Kingdom',
            'rate': '0.0200',
        },
    ])


async def generate_pdf(request):
    assert request.headers['pdf_zoom'] == '1.25'
    data = await request.read()
    if not data:
        return Response(text='request was empty', status=400)
    elif b'binary' in data:
        return Response(body=b'binary-\xfe', content_type='application/pdf')
    else:
        return Response(body=data, content_type='application/pdf')


async def logging_middleware(app, handler):
    async def _handler(request):
        r = await handler(request)
        request.app['request_log'].append(f'{request.method} {request.path_qs} > {r.status}')
        return r
    return _handler


@pytest.fixture
def mock_external(loop, test_server):
    app = Application(middlewares=[logging_middleware])

    app.router.add_post('/mandrill/messages/send.json', mandrill_send_view)
    app.router.add_post('/mandrill/subaccounts/add.json', mandrill_sub_account_add)
    app.router.add_get('/mandrill/subaccounts/info.json', mandrill_sub_account_info)

    app.router.add_post('/messagebird/lookup/{number}/hlr', messagebird_hlr_post)
    app.router.add_get('/messagebird/lookup/{number}', messagebird_lookup)
    app.router.add_post('/messagebird/messages', messagebird_send)

    app.router.add_get('/messagebird-pricing', messagebird_pricing)

    app.router.add_route('*', '/generate.pdf', generate_pdf)
    app.update(
        request_log=[],
        mandrill_subaccounts={}
    )
    server = loop.run_until_complete(test_server(app))
    app['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def settings(tmpdir, mock_external):
    return Settings(
        auth_key='testing-key',
        test_output=str(tmpdir),
        pdf_generation_url=mock_external.app['server_name'] + '/generate.pdf',
        mandrill_key='good-mandrill-testing-key',
        log_level='ERROR',
        mandrill_url=mock_external.app['server_name'] + '/mandrill',
        host_name=None,
        click_host_name='click.example.com',
        s3_access_key=None,
        s3_secret_key=None,
        snapshot_repo_name='morpheus-testing',
        messagebird_key='good-messagebird-testing-key',
        messagebird_url=mock_external.app['server_name'] + '/messagebird',
        messagebird_pricing_api=mock_external.app['server_name'] + '/messagebird-pricing',
        messagebird_pricing_username='mb-username',
        messagebird_pricing_password='mb-password',
        stats_token='test-token',
        max_request_stats=10,
    )


@pytest.fixture
def cli(loop, test_client, settings, setup_elastic_search):
    async def modify_startup(app):
        app['sender']._concurrency_enabled = False
        await app['sender'].startup()
        redis_pool = await app['sender'].get_redis_pool()
        app['webhook_auth_key'] = b'testing'
        async with redis_pool.get() as redis:
            await redis.flushdb()

    async def shutdown(app):
        await app['sender'].shutdown()

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    app.on_shutdown.append(shutdown)
    cli_ = loop.run_until_complete(test_client(app))
    cli_.server.app['morpheus_api'].root = f'http://localhost:{cli_.server.port}/'
    return cli_


@pytest.fixture
def send_email(cli, **extra):
    async def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='<body>\n{{{ message }}}\n</body>',
            company_code='foobar',
            from_address='Sender Name <sender@example.com>',
            method='email-test',
            subject_template='test message',
            context={
                'message': 'this is a test'
            },
            recipients=[{'address': 'foobar@testing.com'}]
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == 201
        if len(data['recipients']) != 1:
            return NotImplemented
        else:
            return re.sub(r'[^a-zA-Z0-9\-]', '', f'{data["uid"]}-{data["recipients"][0]["address"]}')
    return _send_message


@pytest.fixture
def send_sms(cli, **extra):
    async def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='this is a test {{ variable }}',
            company_code='foobar',
            from_name='FooBar',
            method='sms-test',
            context={
                'variable': 'apples'
            },
            recipients=[{'number': '07896541236'}]
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == 201
        return data['uid'] + '-447896541236'
    return _send_message
