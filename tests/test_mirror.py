"""Writes copied to the mirror database while Morpheus moves from RDS to Heroku Postgres (issue #502)."""

from datetime import datetime, timezone

import pytest
from celery.exceptions import Retry
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, make_url, select, text
from sqlalchemy.exc import DataError, OperationalError
from sqlmodel import SQLModel

from app.core import database as db_module
from app.core.celery import celery_app
from app.core.config import settings as app_settings
from app.messages import mirror as mirror_module, tasks
from app.messages.models import Company, Event, Message
from tests.test_email import send_with_link

TABLES = ('companies', 'message_groups', 'messages', 'events', 'links')
# check_mirror's backfill_start when the mirror has had every row since id 1.
FROM_FIRST_ID = dict.fromkeys(('message_groups', 'messages', 'events', 'links'), 1)


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
    # CI's Postgres service has no TLS; without an sslmode the mirror engine requires it.
    return mirror.update_query_dict({'sslmode': 'prefer'}).render_as_string(hide_password=False)


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


@pytest.fixture
def mirror_statements(mirror):
    """The mirror's statements, cut to their first three words, e.g. 'DELETE FROM companies'."""
    statements = []

    def record(conn, cursor, statement, *args):
        statements.append(' '.join(statement.split()[:3]))

    event.listen(mirror, 'before_cursor_execute', record)
    yield statements
    event.remove(mirror, 'before_cursor_execute', record)


def test_send_email_copies_rows_with_primary_ids(send_email, tmpdir, mirror, mirror_statements):
    send_with_link(send_email, tmpdir)

    assert_mirror_matches_primary(mirror)
    assert len(rows(mirror, 'messages')) == 1
    assert len(rows(mirror, 'links')) == 1
    # Not copied: the mirror's own create_tsvector trigger builds the same search vector.
    assert rows(mirror, 'messages')[0]['vector']
    assert not [s for s in mirror_statements if s.startswith('DELETE')]


def test_primary_connection_returned_before_mirror_statements(send_email, mirror):
    send_email()
    idle = db_module.engine.pool.checkedout()
    checked_out = []
    event.listen(mirror, 'before_cursor_execute', lambda *args: checked_out.append(db_module.engine.pool.checkedout()))

    mirror_module.mirror_rows('messages', [1])

    assert set(checked_out) == {idle}


def test_webhook_event_and_cost_reach_mirror(send_sms, send_webhook, mirror):
    ext_id = send_sms()
    send_webhook(ext_id, 0.07)

    assert_mirror_matches_primary(mirror)
    message = rows(mirror, 'messages')[0]
    assert message['status'] == 'delivered'
    assert message['cost'] == 0.07
    assert len(rows(mirror, 'events')) == 1


def test_events_with_same_ts_take_message_status_from_primary(db, send_sms, monkeypatch, mirror, mirror_url):
    send_sms()
    monkeypatch.setattr(app_settings, 'mirror_database_url', None)
    ts = datetime(2032, 6, 6, 12, tzinfo=timezone.utc)
    db.add_all([Event(message_id=1, status='delivered', ts=ts), Event(message_id=1, status='delivery_failed', ts=ts)])
    db.commit()
    monkeypatch.setattr(app_settings, 'mirror_database_url', mirror_url)

    # In the other order to the primary: the mirror's trigger alone would settle on delivery_failed.
    mirror_module.mirror_rows('events', [2])
    mirror_module.mirror_rows('events', [1])

    assert_mirror_matches_primary(mirror)
    assert rows(mirror, 'messages')[0]['status'] == 'delivered'


def test_older_message_copy_does_not_overwrite_newer(send_sms, mirror):
    send_sms()
    with mirror.begin() as conn:
        conn.execute(text("UPDATE messages SET status = 'delivered', update_ts = update_ts + interval '1 hour'"))

    mirror_module.mirror_rows('messages', [1])

    assert rows(mirror, 'messages')[0]['status'] == 'delivered'


def test_copy_read_before_cost_landed_keeps_mirror_cost(db, send_sms, monkeypatch, mirror, mirror_url):
    send_sms()
    monkeypatch.setattr(app_settings, 'mirror_database_url', None)
    db.add(Event(message_id=1, status='delivered', ts=datetime(2032, 6, 6, 12, tzinfo=timezone.utc)))
    db.commit()
    monkeypatch.setattr(app_settings, 'mirror_database_url', mirror_url)
    # The cost's copy landed first; these copies read the primary before the cost was set there.
    with mirror.begin() as conn:
        conn.execute(text('UPDATE messages SET cost = 0.07'))

    mirror_module.mirror_rows('messages', [1])
    mirror_module.mirror_rows('events', [1])

    message = rows(mirror, 'messages')[0]
    assert message['cost'] == 0.07
    assert message['status'] == 'delivered'


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


def test_delete_subaccount_removes_company_from_mirror(cli: TestClient, send_email, mirror, mirror_statements):
    send_email(company_code='foobar')
    send_email(company_code='other')

    r = cli.post(
        '/delete-subaccount/email-test/', json={'company_code': 'foobar'}, headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text

    assert_mirror_matches_primary(mirror)
    assert [c['code'] for c in rows(mirror, 'companies')] == ['other']
    assert len(rows(mirror, 'messages')) == 1
    # The cascade runs without the mirror's statement timeout.
    purge = mirror_statements.index('DELETE FROM companies')
    assert mirror_statements[purge - 1] == 'SET LOCAL statement_timeout'


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


def test_copy_queued_after_connection_returned(db, monkeypatch, mirror):
    in_transaction = []
    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', lambda *args: in_transaction.append(db.in_transaction()))

    db.add(Company(code='foobar'))
    db.commit()

    assert in_transaction == [False]


def test_queue_failure_does_not_fail_the_write(send_email, monkeypatch, caplog, mirror):
    def broker_down(*args):
        raise ConnectionError('broker down')

    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', broker_down)

    send_email()

    assert len(rows(db_module.engine, 'messages')) == 1
    assert rows(mirror, 'messages') == []
    assert 'failed to queue mirror of messages' in caplog.text


def test_copies_not_queued_past_backlog_cap(send_email, monkeypatch, caplog, mirror):
    monkeypatch.setattr(app_settings, 'mirror_max_backlog', 2)
    monkeypatch.setattr(mirror_module, '_backlog_full', False)
    redis = tasks.get_redis()
    redis.rpush('mirror', 'copy', 'copy')

    send_email()
    send_email()

    assert rows(mirror, 'messages') == []
    assert caplog.text.count('mirror queue past 2 copies') == 1

    redis.delete('mirror')
    send_email()

    assert len(rows(mirror, 'messages')) == 1
    assert 'mirror queue below 2 copies' in caplog.text


def test_nothing_queued_without_mirror_url(send_email, monkeypatch):
    queued = []
    monkeypatch.setattr(mirror_module.mirror_rows, 'delay', lambda *args: queued.append(args))

    send_email()
    mirror_module.mirror_ids('companies', [1])

    assert queued == []


def test_reconcile_recopies_companies_out_of_step(db, mirror):
    db.add_all([Company(code='kept'), Company(code='deleted/2')])
    db.commit()
    # A rename and a purge whose copies never landed.
    with mirror.begin() as conn:
        conn.execute(text("UPDATE companies SET code = 'renamed' WHERE id = 2"))
        conn.execute(text("INSERT INTO companies (id, code) VALUES (3, 'purged')"))

    assert mirror_module.reconcile_mirror_companies() == 2

    assert_mirror_matches_primary(mirror)


def test_check_mirror_requeues_rows_that_differ(send_sms, send_webhook, monkeypatch, mirror, mirror_statements):
    monkeypatch.setattr(mirror_module, 'CHECK_RANGE', 2)
    ext_id = send_sms()
    send_sms()
    send_webhook(ext_id, 0.07)
    # A cost update and an event whose copies were lost.
    with mirror.begin() as conn:
        conn.execute(text('UPDATE messages SET cost = NULL WHERE id = 1'))
        conn.execute(text('DELETE FROM events'))

    assert mirror_module.check_mirror(FROM_FIRST_ID) == {
        'companies': 0,
        'message_groups': 0,
        'messages': 1,
        'events': 1,
        'links': 0,
    }

    assert_mirror_matches_primary(mirror)
    assert set(mirror_module.check_mirror(FROM_FIRST_ID).values()) == {0}
    # Its reads on the mirror run without the copies' statement timeout.
    assert 'SET LOCAL statement_timeout' in mirror_statements


def test_check_mirror_leaves_history_before_backfill(send_sms, send_webhook, monkeypatch, mirror, mirror_url):
    monkeypatch.setattr(mirror_module, 'CHECK_RANGE', 2)
    # Messages 1 and 2: history from before the backfill's cutoff, on the primary only.
    monkeypatch.setattr(app_settings, 'mirror_database_url', None)
    old_ext_id = send_sms()
    send_sms(company_code='old')
    monkeypatch.setattr(app_settings, 'mirror_database_url', mirror_url)
    send_sms()
    # A late delivery on message 1 copies it and its group across, below the backfill's first id.
    send_webhook(old_ext_id, 0.07)

    start = {'message_groups': 3, 'messages': 3, 'events': 1, 'links': 1}
    # Only the company the backfill skipped: companies are compared whole.
    assert mirror_module.check_mirror(start) == {
        'companies': 1,
        'message_groups': 0,
        'messages': 0,
        'events': 0,
        'links': 0,
    }

    assert [m['id'] for m in rows(mirror, 'messages')] == [1, 3]
    assert [c['code'] for c in rows(mirror, 'companies')] == ['foobar', 'old']
    assert set(mirror_module.check_mirror(start).values()) == {0}


def test_reconcile_does_nothing_without_mirror_url():
    assert mirror_module.reconcile_mirror_companies() == 0


@pytest.mark.parametrize(
    'error, raised',
    [
        (OperationalError('copy', {}, Exception('mirror restarting')), Retry),
        (DataError('copy', {}, Exception('invalid enum value')), DataError),
    ],
)
def test_copy_retries_only_transient_errors(error, raised, monkeypatch, mirror):
    def copy(*args):
        raise error

    monkeypatch.setattr(mirror_module, '_copy', copy)

    with pytest.raises(raised):
        mirror_module.mirror_rows.delay('companies', [1])


def test_copies_run_on_their_own_queue():
    route = celery_app.amqp.router.route

    assert route({}, 'app.messages.mirror.mirror_rows')['queue'].name == 'mirror'
    assert route({}, 'app.messages.tasks.send_email')['queue'].name == 'celery'


def test_mirror_requires_tls_unless_url_sets_sslmode(monkeypatch):
    engine_kwargs = []
    monkeypatch.setattr(mirror_module, 'make_engine', lambda *args, **kwargs: engine_kwargs.append(kwargs))
    for url in ('postgresql://u@db.example.com/x', 'postgresql://u@localhost/x?sslmode=prefer'):
        monkeypatch.setattr(mirror_module, '_mirror_engine', None)
        monkeypatch.setattr(app_settings, 'mirror_database_url', url)
        mirror_module.get_mirror_engine()

    assert [kwargs.get('sslmode') for kwargs in engine_kwargs] == ['require', None]


def test_mirror_statements_are_bounded(mirror):
    with mirror.connect() as conn:
        values = [
            conn.execute(text(f'SHOW {name}')).scalar() for name in ('statement_timeout', 'lock_timeout', 'TimeZone')
        ]

    assert values == ['30s', '5s', 'UTC']


def test_mirror_errors_omit_row_values(mirror):
    with pytest.raises(DataError) as exc_info, mirror.connect() as conn:
        conn.execute(text('select CAST(:v AS text), 1/0'), {'v': 'marker-c41d'})

    assert 'marker-c41d' not in str(exc_info.value)


def test_copy_queued_before_mirror_url_was_unset_is_dropped(monkeypatch):
    monkeypatch.setattr(mirror_module, 'get_session', lambda: pytest.fail('read the primary'))

    mirror_module.mirror_rows('companies', [1])
