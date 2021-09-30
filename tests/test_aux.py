import base64
import pytest
from foxglove.db.helpers import SyncDb
from foxglove.test_server import DummyServer
from foxglove.testing import Client
from starlette.testclient import TestClient

from src.ext import ApiError, ApiSession
from tests.test_user_display import modify_url


def test_index(cli: TestClient):
    r = cli.get('/')
    assert r.status_code == 200
    assert 'Morpheus - The Greek God' in r.content.decode()


def test_index_head(cli: TestClient):
    r = cli.head('/')
    assert r.status_code == 200
    assert '' == r.text


def test_robots(cli: TestClient):
    r = cli.get('/robots.txt')
    assert r.status_code == 200
    assert 'User-agent: *' in r.text


def test_favicon(cli: TestClient):
    r = cli.get('/favicon.ico', allow_redirects=False)
    assert r.status_code == 200
    assert 'image' in r.headers['Content-Type']  # value can vary


def test_405(cli: TestClient):
    r = cli.post('/')
    assert r.status_code == 405, r.text


def test_create_subaccount_new_few_sent(cli: Client, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'foobar'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert r.json() == {'message': 'subaccount created'}
    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 200']

    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {
        'message': 'subaccount already exists with only 42 emails sent, reuse of subaccount id permitted'
    }
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


def test_create_subaccount_lots(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'lots-sent'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text

    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 409, r.text
    assert r.json() == {
        'message': 'subaccount already exists with 200 emails sent, reuse of subaccount id not permitted'
    }
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/add.json > 500',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


def test_create_subaccount_wrong_response(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'broken'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 400, r.text

    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 500']


def test_create_subaccount_other_method(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    r = cli.post('/create-subaccount/email-test/', headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'no subaccount creation required for "email-test"'}

    assert dummy_server.log == []


def test_create_subaccount_invalid_key(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'foobar'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-keyX'})
    assert r.status_code == 403, r.text


def test_create_subaccount_on_send_email(cli: TestClient, sync_db: SyncDb, dummy_server, send_email):
    data = {'company_code': 'foobar'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert r.json() == {'message': 'subaccount created'}
    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 200']

    assert sync_db.fetchval('select count(*) from companies') == 0

    send_email(company_code='foobar')
    assert sync_db.fetchval('select count(*) from companies') == 1


def test_create_subaccount_on_send_sms(cli: TestClient, sync_db: SyncDb, dummy_server, send_sms):
    data = {'company_code': 'foobar'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert r.json() == {'message': 'subaccount created'}
    assert dummy_server.log == ['POST /mandrill/subaccounts/add.json > 200']
    assert sync_db.fetchval('select count(*) from companies') == 0

    send_sms(company_code='foobar')
    assert sync_db.fetchval('select count(*) from companies') == 1


def test_user_list_subaccount_doesnt_exist(cli, settings, sync_db: SyncDb, dummy_server: DummyServer):
    r = cli.get(modify_url('/user/email-test/messages.json', settings))
    assert r.status_code == 404


def _create_test_subaccount(cli, data):
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text


def test_delete_subaccount(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'foobar'}
    _create_test_subaccount(cli, data)

    r = cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'deleted_messages=0 deleted_message_groups=0'}
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 200',
    ]

    r = cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 404, r.text
    assert r.json() == {'message': "No subaccount exists with the id 'foobar'"}
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 200',
        'POST /mandrill/subaccounts/delete.json > 500',
    ]


def test_delete_subaccount_multiple_branches(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'foobar'}
    sync_db.execute('insert into companies (code) values ($1)', 'foobar:1')
    sync_db.execute('insert into companies (code) values ($1)', 'foobar:2')
    sync_db.execute('insert into companies (code) values ($1)', 'notbar:1')

    r = cli.post('/delete-subaccount/email-test/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'deleted_messages=0 deleted_message_groups=0'}
    assert sync_db.fetchval('select count(*) from companies') == 1


def test_delete_subaccount_wrong_response(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'broken1'}
    _create_test_subaccount(cli, data)

    r = cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 400, r.text
    assert dummy_server.log == [
        'POST /mandrill/subaccounts/add.json > 200',
        'POST /mandrill/subaccounts/delete.json > 500',
    ]


def test_delete_subaccount_other_method(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'foobar'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'deleted_messages=0 deleted_message_groups=0'}

    assert dummy_server.log == []


def test_delete_subaccount_invalid_key(cli: TestClient, sync_db: SyncDb):
    data = {'company_code': 'foobar'}
    r = cli.post('/delete-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-keyX'})
    assert r.status_code == 403, r.text


def test_delete_subaccount_and_saved_messages(
    cli: TestClient, sync_db: SyncDb, send_email, send_sms, dummy_server: DummyServer
):
    send_email(company_code='foobar1')
    send_sms(company_code='foobar1')
    send_email(company_code='foobar2', recipients=[{'address': f'{i}@test.com'} for i in range(5)])
    assert sync_db.fetchval('select count(*) from companies') == 2
    assert sync_db.fetchval('select count(*) from message_groups') == 3
    assert sync_db.fetchval('select count(*) from messages') == 7

    fb1_data = {'company_code': 'foobar1'}
    _create_test_subaccount(cli, fb1_data)
    fb2_data = {'company_code': 'foobar2'}
    _create_test_subaccount(cli, fb2_data)

    r = cli.post('/delete-subaccount/email-mandrill/', json=fb1_data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'deleted_messages=2 deleted_message_groups=2'}

    assert sync_db.fetchval('select count(*) from message_groups') == 1
    assert sync_db.fetchval('select count(*) from messages') == 5

    r = cli.post('/delete-subaccount/email-mandrill/', json=fb2_data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'deleted_messages=5 deleted_message_groups=1'}

    assert sync_db.fetchval('select count(*) from message_groups') == 0
    assert sync_db.fetchval('select count(*) from messages') == 0

    send_email(company_code='foobar3')
    assert sync_db.fetchval('select count(*) from message_groups') == 1
    assert sync_db.fetchval('select count(*) from messages') == 1

    _create_test_subaccount(cli, {'company_code': 'foobar3'})
    with pytest.raises(TypeError):
        cli.post(
            '/delete-subaccount/email-mandrill/',
            json={'company_code': object()},
            headers={'Authorization': 'testing-key'},
        )
    assert sync_db.fetchval('select count(*) from message_groups') == 1
    assert sync_db.fetchval('select count(*) from messages') == 1


def test_missing_link(cli: TestClient):
    r = cli.get('/lxxx')
    assert r.status_code == 404, r.text
    assert (
        '<p>404: No redirect could be found for "http://testserver/lxxx", this link may have expired.</p>'
    ) in r.text


def test_missing_url_with_arg(cli: TestClient):
    url = 'https://example.com/foobar'
    r = cli.get('/lxxx?u=' + base64.urlsafe_b64encode(url.encode()).decode(), allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['Location'] == url


def test_missing_url_with_arg_bad(cli: TestClient):
    r = cli.get('/lxxx?u=xxx', allow_redirects=False)
    assert r.status_code == 404, r.text


def test_api_error(settings, loop, dummy_server: DummyServer):
    s = ApiSession(dummy_server.server_name, settings)
    with pytest.raises(ApiError) as exc_info:
        loop.run_until_complete(s.get('/foobar'))
    assert str(exc_info.value) == f'GET {dummy_server.server_name}/foobar, unexpected response 404'


def test_settings(settings):
    assert settings.pg_host == 'localhost'
    assert settings.pg_port == 5432
    assert settings.pg_name == 'morpheus_test'
