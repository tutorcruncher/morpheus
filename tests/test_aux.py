import base64

import pytest
from fastapi.testclient import TestClient

from app.ext.clients import ApiError, ApiSession
from app.messages import tasks
from tests.conftest import SyncDb
from tests.test_user_display import modify_url

DummyServer = object  # legacy type alias for fixture annotations
Client = TestClient


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
    r = cli.get('/favicon.ico', follow_redirects=False)
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
        'POST /mandrill/subaccounts/add.json > 400',
        'GET /mandrill/subaccounts/info.json > 200',
    ]


def test_create_subaccount_exists_legacy_500(cli: Client, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'legacy-500'}
    r = cli.post('/create-subaccount/email-mandrill/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text

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
        'POST /mandrill/subaccounts/add.json > 400',
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
    assert r.json() == {'message': 'queued_companies=0'}
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
        'POST /mandrill/subaccounts/delete.json > 404',
    ]


def test_delete_subaccount_multiple_branches(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    data = {'company_code': 'foobar'}
    sync_db.execute('insert into companies (code) values ($1)', 'foobar:1')
    sync_db.execute('insert into companies (code) values ($1)', 'foobar:2')
    sync_db.execute('insert into companies (code) values ($1)', 'notbar:1')

    r = cli.post('/delete-subaccount/email-test/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'queued_companies=2'}
    assert sync_db.fetchval('select count(*) from companies') == 1


def test_delete_subaccount_does_not_match_longer_codes(
    cli: TestClient, sync_db: SyncDb, send_email, dummy_server: DummyServer
):
    """Deleting 'simply-learn' must not touch 'simply-learning-tuition' (code-prefix collision)."""
    send_email(company_code='simply-learn:1')
    send_email(company_code='simply-learn')
    send_email(company_code='simply-learning-tuition:7664')
    assert sync_db.fetchval('select count(*) from companies') == 3
    assert sync_db.fetchval('select count(*) from messages') == 3

    data = {'company_code': 'simply-learn'}
    r = cli.post('/delete-subaccount/email-test/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'queued_companies=2'}

    assert sync_db.fetchval('select code from companies') == 'simply-learning-tuition:7664'
    assert sync_db.fetchval('select count(*) from messages') == 1


def test_delete_subaccount_queues_the_purge(cli: TestClient, sync_db: SyncDb, send_email, monkeypatch):
    """The purge is queued, not run inline.

    Deleting a large agency's history takes minutes, and Heroku's router closes the request at 30
    seconds, so a caller that purges inline is told the delete failed while it goes on to succeed.
    """
    send_email(company_code='slowco')
    company_id = sync_db.fetchval("select id from companies where code = 'slowco'")

    queued = []
    monkeypatch.setattr(tasks.delete_company_messages, 'delay', lambda *args: queued.append(args))
    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'slowco'}, headers={'Authorization': 'testing-key'}
    )

    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'queued_companies=1'}
    assert queued == [([company_id], 'slowco')]
    # Still here: the request handed the work to a worker rather than doing it itself.
    assert sync_db.fetchval('select count(*) from messages') == 1
    assert sync_db.fetchval('select count(*) from companies') == 1


def test_delete_subaccount_purge_spares_a_recreated_company(cli: TestClient, sync_db: SyncDb, send_email, monkeypatch):
    """A send between the response and the worker must survive the queued purge.

    /send/ finds companies by code, so a company left under its own code would hand a re-created
    subaccount the very id the purge is about to delete, taking the new agency's history with it.
    """
    send_email(company_code='slowco')
    old_id = sync_db.fetchval("select id from companies where code = 'slowco'")

    queued = []
    monkeypatch.setattr(tasks.delete_company_messages, 'delay', lambda *args: queued.append(args))
    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'slowco'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text
    # No colon, so split_part leaves it whole, and the id keeps it unique: no delete for a real
    # subaccount code can select it.
    assert sync_db.fetchval('select code from companies where id = $1', old_id) == f'deleted/{old_id}'

    # The agency signs up again and sends before the worker gets round to the purge.
    send_email(company_code='slowco')
    new_id = sync_db.fetchval("select id from companies where code = 'slowco'")
    assert new_id != old_id

    tasks.delete_company_messages(*queued[0])

    assert sync_db.fetchval('select count(*) from messages') == 1
    assert sync_db.fetchval('select code from companies') == 'slowco'


def test_purge_deleted_companies_redrives_a_lost_task(cli: TestClient, sync_db: SyncDb, send_email, monkeypatch):
    """A purge that never reached a worker leaves a row no later delete can match, so it is swept.

    The rename commits before the task is published, so losing the task in between — a broker blip,
    or a worker still on the previous release — strands the messages under the tombstoned code.
    """
    send_email(company_code='slowco')
    company_id = sync_db.fetchval("select id from companies where code = 'slowco'")

    monkeypatch.setattr(tasks.delete_company_messages, 'delay', lambda *args: None)
    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'slowco'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text
    assert sync_db.fetchval('select code from companies') == f'deleted/{company_id}'
    assert sync_db.fetchval('select count(*) from messages') == 1

    monkeypatch.undo()
    assert tasks.purge_deleted_companies() == 1

    assert sync_db.fetchval('select count(*) from messages') == 0
    assert sync_db.fetchval('select count(*) from companies') == 0


def test_purge_deleted_companies_only_takes_rows_the_rename_made(sync_db: SyncDb, send_email):
    """The sweep matches the exact code the rename writes, not a prefix of it.

    /send/ creates a company for whatever code it is handed, so a prefix match would let the sweep
    delete a live company that merely looks tombstoned.
    """
    send_email(company_code='deleted/foo')

    assert tasks.purge_deleted_companies() == 0

    assert sync_db.fetchval('select code from companies') == 'deleted/foo'
    assert sync_db.fetchval('select count(*) from messages') == 1


def test_delete_company_messages_reports_what_it_deleted(sync_db: SyncDb, send_email):
    send_email(company_code='purgeco', recipients=[{'address': f'{i}@test.com'} for i in range(3)])
    send_email(company_code='keepco')
    company_id = sync_db.fetchval("select id from companies where code = 'purgeco'")

    assert tasks.delete_company_messages([company_id], 'purgeco') == 'deleted_messages=3 deleted_message_groups=1'

    assert sync_db.fetchval('select count(*) from messages') == 1
    assert sync_db.fetchval('select code from companies') == 'keepco'


def test_delete_subaccount_unknown_to_mandrill(cli: TestClient, sync_db: SyncDb, dummy_server: DummyServer):
    """An agency whose sub-account was never created still has to read as deleted, not as an error."""
    r = cli.post(
        '/delete-subaccount/email-mandrill/',
        json={'company_code': 'never-created'},
        headers={'Authorization': 'testing-key'},
    )
    assert r.status_code == 404, r.text
    assert r.json() == {'message': "No subaccount exists with the id 'never-created'"}
    assert dummy_server.log == ['POST /mandrill/subaccounts/delete.json > 404']


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
    assert r.json() == {'message': 'queued_companies=0'}

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
    assert r.json() == {'message': 'queued_companies=1'}

    assert sync_db.fetchval('select count(*) from message_groups') == 1
    assert sync_db.fetchval('select count(*) from messages') == 5

    r = cli.post('/delete-subaccount/email-mandrill/', json=fb2_data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    assert r.json() == {'message': 'queued_companies=1'}

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
    r = cli.get('/lxxx?u=' + base64.urlsafe_b64encode(url.encode()).decode(), follow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['Location'] == url


def test_missing_url_with_arg_bad(cli: TestClient):
    r = cli.get('/lxxx?u=xxx', follow_redirects=False)
    assert r.status_code == 404, r.text


def test_api_error(settings, dummy_server: DummyServer):
    s = ApiSession(dummy_server.server_name, settings)
    with pytest.raises(ApiError) as exc_info:
        s.get('/foobar')
    assert str(exc_info.value) == f'GET {dummy_server.server_name}/foobar, unexpected response 404'


def test_settings(settings):
    assert settings.pg_host == 'localhost'
    assert settings.pg_port == 5432
    assert settings.pg_name == 'morpheus_test'
