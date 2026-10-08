"""Writes copied to the mirror database while Morpheus moves from RDS to Heroku Postgres (issue #502)."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, make_url, select, text
from sqlmodel import SQLModel

from app.core import database as db_module
from app.core.config import settings as app_settings
from app.messages import mirror as mirror_module, tasks
from app.messages.models import Company, Message
from tests.test_email import send_with_link

TABLES = ('companies', 'message_groups', 'messages', 'events', 'links')


@pytest.fixture(scope='session')
def mirror_url() -> str:
    """Create the mirror test database once per session, bootstrapped like the primary."""
    url = make_url(app_settings.database_url)
    mirror = url.set(database=f'{url.database}_mirror')
    admin = create_engine(url, isolation_level='AUTOCOMMIT')
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS {mirror.database} WITH (FORCE)'))
        conn.execute(text(f'CREATE DATABASE {mirror.database}'))
    admin.dispose()
    mirror_engine = create_engine(mirror)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(db_module, 'engine', mirror_engine)
        db_module.create_db_and_tables()
    mirror_engine.dispose()
    return mirror.render_as_string(hide_password=False)


@pytest.fixture
def mirror(settings, monkeypatch, mirror_url):
    monkeypatch.setattr(app_settings, 'mirror_database_url', mirror_url)
    monkeypatch.setattr(mirror_module, '_mirror_engine', None)
    mirror_engine = mirror_module.get_mirror_engine()
    with mirror_engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {", ".join(TABLES)} CASCADE'))
    yield mirror_engine
    mirror_engine.dispose()


def rows(engine, table_name: str) -> list[dict]:
    # The model's columns, not *: production's messages also carry legacy columns no model defines.
    table = SQLModel.metadata.tables[table_name]
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(table).order_by(table.c.id)).mappings()]


def assert_mirror_matches_primary(mirror_engine) -> None:
    for table in TABLES:
        assert rows(mirror_engine, table) == rows(db_module.engine, table), table


def test_send_email_copies_rows_with_primary_ids(send_email, tmpdir, mirror):
    send_with_link(send_email, tmpdir)

    assert_mirror_matches_primary(mirror)
    assert len(rows(mirror, 'messages')) == 1
    assert len(rows(mirror, 'links')) == 1
    # Not copied: the mirror's own create_tsvector trigger builds the same search vector.
    assert rows(mirror, 'messages')[0]['vector']


def test_webhook_event_and_cost_reach_mirror(send_sms, send_webhook, mirror):
    ext_id = send_sms()
    send_webhook(ext_id, 0.07)

    assert_mirror_matches_primary(mirror)
    message = rows(mirror, 'messages')[0]
    assert message['status'] == 'delivered'
    assert message['cost'] == 0.07
    assert len(rows(mirror, 'events')) == 1


def test_event_on_message_sent_before_mirror_brings_message_across(
    send_sms, send_webhook, monkeypatch, mirror, mirror_url
):
    monkeypatch.setattr(app_settings, 'mirror_database_url', None)
    ext_id = send_sms()
    monkeypatch.setattr(app_settings, 'mirror_database_url', mirror_url)
    assert rows(mirror, 'messages') == []

    send_webhook(ext_id, 0.07)

    assert_mirror_matches_primary(mirror)
    assert len(rows(mirror, 'events')) == 1


def test_click_reaches_mirror(cli: TestClient, send_email, tmpdir, mirror):
    token = send_with_link(send_email, tmpdir)

    r = cli.get(f'/l{token}', follow_redirects=False)
    assert r.status_code == 307, r.text

    assert_mirror_matches_primary(mirror)
    event = rows(mirror, 'events')[0]
    assert event['status'] == 'click'


def test_delete_subaccount_removes_company_from_mirror(cli: TestClient, send_email, mirror):
    send_email(company_code='foobar')
    send_email(company_code='other')

    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'foobar'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text

    assert_mirror_matches_primary(mirror)
    assert [c['code'] for c in rows(mirror, 'companies')] == ['other']
    assert len(rows(mirror, 'messages')) == 1


def test_delete_subaccount_rename_reaches_mirror(cli: TestClient, send_email, monkeypatch, mirror):
    send_email(company_code='foobar')
    monkeypatch.setattr(tasks.delete_company_messages, 'delay', lambda *args: None)

    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'foobar'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text

    assert_mirror_matches_primary(mirror)
    company = rows(mirror, 'companies')[0]
    assert company['code'] == f'deleted/{company["id"]}'


def test_orm_delete_reaches_mirror(db, send_email, mirror):
    send_email()
    assert len(rows(mirror, 'messages')) == 1

    db.delete(db.get(Message, 1))
    db.commit()

    assert_mirror_matches_primary(mirror)
    assert rows(mirror, 'messages') == []
    assert len(rows(mirror, 'message_groups')) == 1


def test_rolled_back_writes_are_not_queued(db, monkeypatch, mirror):
    queued = []
    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', lambda *args: queued.append(args))

    db.add(Company(code='rolled-back'))
    db.flush()
    db.rollback()
    db.add(Company(code='kept'))
    db.commit()

    assert queued == [('companies', [2])]


def test_queue_failure_does_not_fail_the_write(send_email, monkeypatch, caplog, mirror):
    def broker_down(*args):
        raise ConnectionError('broker down')

    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', broker_down)

    send_email()

    assert len(rows(db_module.engine, 'messages')) == 1
    assert rows(mirror, 'messages') == []
    assert 'failed to queue mirror of messages' in caplog.text


def test_nothing_queued_without_mirror_url(send_email, monkeypatch):
    queued = []
    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', lambda *args: queued.append(args))

    send_email()

    assert queued == []


def test_copy_queued_before_mirror_url_was_unset_is_dropped(monkeypatch):
    monkeypatch.setattr(mirror_module, 'get_session', lambda: pytest.fail('read the primary'))

    mirror_module.mirror_rows('companies', [1])
