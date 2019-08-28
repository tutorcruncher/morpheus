import base64
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from morpheus.app.ext import ApiError, ApiSession


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


async def test_405(cli):
    r = await cli.post('/')
    assert r.status == 405, await r.text()


async def test_message_stats(cli, send_email):
    for i in range(5):
        await send_email(uid=str(uuid.uuid4()), recipients=[{'address': f'{i}@t.com'}])

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data == [{'count': 5, 'age': 0, 'method': 'email-test', 'status': 'send'}]

    await send_email()

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data2 = await r.json()
    assert data2 == data  # last message has no effect due to caching


async def test_message_stats_old(cli, send_email, db_conn):
    expected_msg_ids = []
    for i in range(5):
        uid = str(uuid.uuid4())
        await send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    old = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(minutes=20)

    await db_conn.execute(
        'update messages set send_ts=$1, update_ts=$2 where external_id=$3', old, old, expected_msg_ids[0]
    )
    await db_conn.execute(
        'update messages set send_ts=$1, status=$2 where external_id=$3', old, 'open', expected_msg_ids[1]
    )

    r = await cli.get('/stats/messages/', headers={'Authorization': 'test-token'})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data == [
        {'count': 3, 'age': 0, 'method': 'email-test', 'status': 'send'},
        {'count': 1, 'age': 1200, 'method': 'email-test', 'status': 'open'},
    ]


async def test_create_sub_account_new_few_sent(cli, dummy_server):
    data = {'company_code': 'foobar'}
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert 'subaccount created\n' == await r.text()
    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 200']

    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'subaccount already exists with only 42 emails sent, reuse of subaccount id permitted\n' == await r.text()
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


async def test_create_sub_account_lots(cli, dummy_server):
    data = {'company_code': 'lots-sent'}
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()

    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 409, await r.text()
    assert 'subaccount already exists with 200 emails sent, reuse of subaccount id not permitted\n' == await r.text()
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


async def test_create_sub_account_wrong_response(cli, dummy_server):
    data = {'company_code': 'broken'}
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 400, await r.text()

    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 500']


async def test_create_sub_account_other_method(cli, dummy_server):
    r = await cli.post('/create-subaccount/email-test/', headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'no subaccount creation required for "email-test"\n' == await r.text()

    assert dummy_server.log == []


async def test_create_sub_account_invalid_key(cli, dummy_server):
    data = {'company_code': 'foobar'}
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-keyX'})
    assert r.status == 403, await r.text()


async def _create_test_sub_account(cli, data):
    r = await cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()


async def test_delete_sub_account(cli, dummy_server):
    data = {'company_code': 'foobar'}
    await _create_test_sub_account(cli, data)

    r = await cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'subaccount deleted\n' == await r.text()
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 200',
    ]

    r = await cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 404, await r.text()
    assert f"No subaccount exists with the id 'foobar'\n" == await r.text()
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 200',
        'POST /mandrill/subaccounts/delete.json > 500',
    ]


async def test_delete_sub_account_wrong_response(cli, dummy_server):
    data = {'company_code': 'broken1'}
    await _create_test_sub_account(cli, data)

    r = await cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 400, await r.text()
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 500',
    ]


async def test_delete_sub_account_other_method(cli, dummy_server):
    r = await cli.post('/delete-subaccount/email-test/', headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    assert 'no subaccount deletion required for "email-test"\n' == await r.text()

    assert dummy_server.log == []


async def test_delete_sub_account_invalid_key(cli, dummy_server):
    data = {'company_code': 'foobar'}
    await _create_test_sub_account(cli, data)

    r = await cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-keyX'})
    assert r.status == 403, await r.text()


async def test_missing_link(cli):
    r = await cli.get('/lxxx')
    assert r.status == 404, await r.text()
    text = await r.text()
    assert (
        f'<p>404: No redirect could be found for "http://127.0.0.1:{cli.server.port}/lxxx", '
        f'this link may have expired.</p>'
    ) in text


async def test_missing_url_with_arg(cli):
    url = 'https://example.com/foobar'
    r = await cli.get('/lxxx?u=' + base64.urlsafe_b64encode(url.encode()).decode(), allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['Location'] == url


async def test_missing_url_with_arg_bad(cli):
    r = await cli.get('/lxxx?u=xxx', allow_redirects=False)
    assert r.status == 404, await r.text()


async def test_api_error(settings, loop, dummy_server):
    s = ApiSession(dummy_server.server_name, settings)
    try:
        with pytest.raises(ApiError) as exc_info:
            await s.get('/foobar')
        assert str(exc_info.value) == f'GET {dummy_server.server_name}/foobar, unexpected response 404'
    finally:
        await s.close()


def test_settings(settings):
    assert settings.pg_host == 'localhost'
    assert settings.pg_port == 5432
    assert settings.pg_name == 'morpheus_test'
