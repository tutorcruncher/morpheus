import asyncio
import base64
import os
import uuid
from datetime import datetime, timedelta

import pytest

from morpheus.app.worker import AuxActor


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert 'Morpheus - The Greek God' in await r.text()


async def test_index_head(cli):
    r = await cli.head('/')
    assert r.status == 200
    assert '' == await r.text()


async def test_robots(cli):
    r = await cli.get('/robots.txt')
    assert r.status == 200
    assert 'User-agent: *' in await r.text()


async def test_favicon(cli):
    r = await cli.get('/favicon.ico', allow_redirects=False)
    assert r.status == 200
    assert 'image' in r.headers['Content-Type']  # value can vary


async def test_create_repo(cli, settings):
    es = cli.server.app['es']
    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}', allowed_statuses=(200, 404))
    if r.status == 200:
        await es.delete(f'/_snapshot/{settings.snapshot_repo_name}')

    type, created = await es.create_snapshot_repo()
    assert type == 'fs'
    assert created is True

    type, created = await es.create_snapshot_repo()
    assert type == 'fs'
    assert created is False

    type, created = await es.create_snapshot_repo(delete_existing=True)
    assert type == 'fs'
    assert created is True


@pytest.mark.skipif(not os.getenv('TRAVIS'),  reason='only run on travis')
async def test_run_snapshot(cli, settings, loop):
    es = cli.server.app['es']
    await es.create_snapshot_repo()

    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}/_all?pretty=true')
    print(await r.text())
    data = await r.json()
    snapshots_before = len(data['snapshots'])

    aux = AuxActor(settings=settings, loop=loop)
    await aux.startup()
    await aux.snapshot_es.direct()
    await aux.close(shutdown=True)

    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}/_all?pretty=true')
    print(await r.text())
    data = await r.json()
    assert len(data['snapshots']) == snapshots_before + 1


async def test_stats_unauthorised(cli, caplog):
    caplog.set_loggers(log_names=['morpheus.request'])
    r = await cli.get('/stats/requests/')
    assert r.status == 403, await r.text()
    assert '403 /stats/requests/' in caplog


async def test_405(cli, caplog):
    caplog.set_loggers(log_names=['morpheus.request'])
    r = await cli.post('/')
    assert r.status == 405, await r.text()
    assert '405 /' in caplog


async def test_request_stats(cli, loop):
    redis = await cli.server.app['sender'].get_redis()
    await redis.delete(cli.server.app['stats_request_count'])
    await redis.delete(cli.server.app['stats_request_list'])
    await asyncio.gather(*(cli.get('/') for _ in range(5)))
    await cli.post('/')

    redis = await cli.server.app['sender'].get_redis()
    for i in range(10):
        if 6 == await redis.llen(cli.server.app['stats_request_list']):
            break
        await asyncio.sleep(0.1, loop=loop)
    assert 6 == await redis.llen(cli.server.app['stats_request_list'])

    r = await cli.get('/stats/requests/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 2
    good = next(d for d in data if d['status'] == '2XX')
    assert good['request_count'] == 5
    assert good['request_count_interval'] == 5
    assert good['method'] == 'GET'
    assert 'time_90' in good

    redis = await cli.server.app['sender'].get_redis()
    keys = await redis.llen(cli.server.app['stats_request_list'])
    assert keys == 0

    # used cached value
    r = await cli.get('/stats/requests/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 2


async def test_request_stats_reset(cli, loop):
    redis = await cli.server.app['sender'].get_redis()
    await redis.delete(cli.server.app['stats_request_count'])
    await redis.delete(cli.server.app['stats_request_list'])

    for _ in range(30):
        await cli.get('/')

    redis = await cli.server.app['sender'].get_redis()
    for i in range(10):
        if 10 > await redis.llen(cli.server.app['stats_request_list']):
            break
        await asyncio.sleep(0.1, loop=loop)

    r = await cli.get('/stats/requests/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 1
    assert data[0]['request_count'] == 30


async def test_message_stats(cli, send_email):
    await cli.server.app['es'].create_indices(True)
    for i in range(5):
        await send_email(uid=str(uuid.uuid4()), recipients=[{'address': f'{i}@t.com'}])
    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    # import json
    # print(json.dumps(data, indent=2))
    assert next(d for d in data if d['method'] == 'email-test' and d['status'] == 'send')['count'] == 5
    assert next(d for d in data if d['method'] == 'email-test' and d['status'] == 'open')['count'] == 0
    assert next(d for d in data if d['method'] == 'email-mandrill' and d['status'] == 'send')['count'] == 0

    await send_email()
    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data2 = await r.json()
    assert data2 == data  # last message has no effect due to caching


async def test_message_stats_old(cli, send_email):
    es = cli.server.app['es']
    await es.create_indices(True)
    expected_msg_ids = []
    for i in range(5):
        uid = str(uuid.uuid4())
        await send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    old = datetime.utcnow() - timedelta(minutes=20)
    await es.post(f'messages/email-test/{expected_msg_ids[0]}/_update', doc={'send_ts': old, 'update_ts': old})
    await es.post(f'messages/email-test/{expected_msg_ids[1]}/_update', doc={'send_ts': old, 'status': 'open'})

    await es.get('messages/_refresh')

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    # import json
    # print(json.dumps(data, indent=2))
    assert next(d for d in data if d['method'] == 'email-test' and d['status'] == 'send') == dict(
        method='email-test',
        status='send',
        count=3,
        age=0,
    )
    assert next(d for d in data if d['method'] == 'email-test' and d['status'] == 'open') == dict(
        method='email-test',
        status='open',
        count=1,
        age=1200,
    )


async def test_create_sub_account_new_few_sent(cli, mock_external):
    data = {
        'company_code': 'foobar'
    }
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert 'subaccount created\n' == await r.text()
    assert mock_external.app['request_log'] == ['POST /mandrill/subaccounts/add.json > 200']

    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'subaccount already exists with only 42 emails sent, reuse of subaccount id permitted\n' == await r.text()
    assert mock_external.app['request_log'] == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


async def test_create_sub_account_lots(cli, mock_external):
    data = {
        'company_code': 'lots-sent'
    }
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()

    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 409, await r.text()
    assert 'subaccount already exists with 200 emails sent, reuse of subaccount id not permitted\n' == await r.text()
    assert mock_external.app['request_log'] == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


async def test_create_sub_account_wrong_response(cli, mock_external):
    data = {
        'company_code': 'broken'
    }
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 400, await r.text()

    assert mock_external.app['request_log'] == ['POST /mandrill/subaccounts/add.json > 500']


async def test_create_sub_account_other_method(cli, mock_external):
    r = await cli.post('/create-subaccount/email-test/', headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'no subaccount creation required for "email-test"\n' == await r.text()

    assert mock_external.app['request_log'] == []


async def test_create_sub_account_invalid_key(cli, mock_external):
    data = {'company_code': 'foobar'}
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-keyX'})
    assert r.status == 403, await r.text()


async def test_missing_link(cli):
    r = await cli.get('/lxxx')
    assert r.status == 404, await r.text()
    text = await r.text()
    assert (f'<p>404: No redirect could be found for "http://127.0.0.1:{cli.server.port}/lxxx", '
            f'this link may have expired.</p>') in text


async def test_missing_url_with_arg(cli):
    url = 'https://example.com/foobar'
    r = await cli.get('/lxxx?u=' + base64.urlsafe_b64encode(url.encode()).decode(), allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['Location'] == url


async def test_missing_url_with_arg_bad(cli):
    r = await cli.get('/lxxx?u=xxx', allow_redirects=False)
    assert r.status == 404, await r.text()
