import asyncio
import re

import pytest
from aiohttp.web import Application, json_response

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


async def logging_middleware(app, handler):
    async def _handler(request):
        request.app['request_log'].append(f'{request.method} {request.path}')
    return _handler


@pytest.fixture
def mock_external(loop, test_server):
    app = Application()
    app.router.add_post('/mandrill/messages/send.json', mandrill_send_view)
    app.update(
        request_log=[],
    )
    server = loop.run_until_complete(test_server(app))
    app['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def settings(tmpdir, mock_external):
    return Settings(
        auth_key='testing-key',
        test_output=str(tmpdir),
        mandrill_key='good-mandrill-testing-key',
        log_level='ERROR',
        mandrill_url=mock_external.app['server_name'] + '/mandrill',
        host_name=None,
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
    return loop.run_until_complete(test_client(app))


@pytest.fixture
def message_data():
    return {
        'uid': 'x' * 20,
        'markdown_template': 'this is a test',
        'main_template': '<body>\n{{{ message }}}\n</body>',
        'company_code': 'foobar',
        'from_address': 'Sender Name <sender@example.com>',
        'method': 'email-test',
        'subject_template': 'test message',
        'recipients': [{'address': f'foobar@testing.com'}]
    }


@pytest.fixture
def message_id(loop, cli, message_data):
    r = loop.run_until_complete(cli.post('/send/', json=message_data, headers={'Authorization': 'testing-key'}))
    assert r.status == 201
    return 'x' * 20 + '-foobartestingcom'
