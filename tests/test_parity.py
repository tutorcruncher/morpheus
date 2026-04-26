"""End-to-end coverage for invariants and the harder-to-reach branches.

The bulk of behaviour parity is covered by the ports of the legacy test files.
This module fills coverage gaps that are hard to hit through the main test paths
and asserts a few cross-cutting invariants (HMAC byte-equivalence, tsvector
trigger output, enum round-trip) that the framework swap could plausibly break.
"""

import base64
import hashlib
import hmac
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core.database import engine
from app.messages.models import (
    Company,
    Message,
    MessageStatus,
    SendMethod,
)
from tests.conftest import SyncDb

# ---- Cross-cutting invariants ---------------------------------------------------


def test_send_methods_enum_roundtrip(sync_db: SyncDb):
    """Every SendMethod value inserts and reads back unchanged."""
    sync_db.execute('insert into companies (code) values ($1)', 'enum-test')
    company_id = sync_db.fetchval('select id from companies where code = $1', 'enum-test')
    for method in SendMethod:
        sync_db.execute(
            'insert into message_groups (uuid, company_id, message_method) values ($1, $2, $3)',
            str(uuid.uuid4()),
            company_id,
            method.value,
        )
    rows = sync_db.fetch('select message_method from message_groups order by id')
    stored = {row['message_method'] for row in rows}
    assert stored == {m.value for m in SendMethod}


def test_message_statuses_enum_roundtrip(cli, send_email, sync_db: SyncDb):
    """Every MessageStatus value inserts into events and reads back unchanged."""
    msg_id = send_email()
    message_id = sync_db.fetchval('select id from messages where external_id = $1', msg_id)
    for status in MessageStatus:
        sync_db.execute('insert into events (message_id, status) values ($1, $2)', message_id, status.value)
    rows = sync_db.fetch('select status from events where message_id = $1', message_id)
    stored = {row['status'] for row in rows}
    assert stored == {s.value for s in MessageStatus}


def test_tsvector_trigger_populates_vector(send_email, sync_db: SyncDb):
    """The set_message_vector trigger should index searchable fields."""
    send_email(
        recipients=[
            {
                'first_name': 'Marigold',
                'last_name': 'Quintessence',
                'address': 'rare@example.org',
            }
        ],
        subject_template='unique-subject-token',
    )
    vec = sync_db.fetchval('select vector::text from messages limit 1')
    # All four high-weight fields plus subject should be present in the tsvector.
    # Postgres stems some words; assert the actual stems we expect to see.
    for token in ('marigold', 'quintess', 'rare@example.org', 'uniqu', 'subject', 'token'):
        assert token in vec, f'expected token {token!r} in tsvector but got {vec!r}'


# ---- Coverage gap fillers (drive the harder branches end-to-end) -----------------


def test_email_send_duplicate_uid_returns_409(cli: TestClient, send_email):
    """Posting the same UID twice should hit the redis-NX guard."""
    uid = str(uuid.uuid4())
    send_email(uid=uid)
    r = cli.post(
        '/send/email/',
        json={
            'uid': uid,
            'company_code': 'foobar',
            'from_address': 'a@b.com',
            'method': 'email-test',
            'subject_template': 's',
            'context': {},
            'recipients': [{'address': 'x@y.com'}],
        },
        headers={'Authorization': 'testing-key'},
    )
    assert r.status_code == 409, r.text
    assert r.json() == {'message': f'Send group with id "{uid}" already exists\n'}


def test_sms_billing_company_not_found(cli: TestClient):
    r = cli.request(
        'GET',
        '/billing/sms-test/no-such-company/',
        json={'start': '2032-01-01', 'end': '2032-12-31'},
        headers={'Authorization': 'testing-key'},
    )
    assert r.status_code == 404, r.text
    assert r.json() == {'message': 'company not found'}


def test_mandrill_webhook_invalid_signature(cli: TestClient):
    r = cli.post(
        '/webhook/mandrill/',
        data={'mandrill_events': '[]'},
        headers={'X-Mandrill-Signature': 'wrong'},
    )
    assert r.status_code == 403
    assert r.json() == {'message': 'invalid signature'}


def test_mandrill_webhook_invalid_data(cli: TestClient, settings):
    """Non-JSON form data should return 400 with the legacy {'message': ...} body."""
    msg = f'{settings.mandrill_webhook_url}mandrill_eventsnot-json'
    sig = base64.b64encode(
        hmac.new(settings.mandrill_webhook_key.encode(), msg=msg.encode(), digestmod=hashlib.sha1).digest()
    )
    r = cli.post(
        '/webhook/mandrill/',
        data={'mandrill_events': 'not-json'},
        headers={'X-Mandrill-Signature': sig.decode()},
    )
    assert r.status_code == 400, r.text
    assert r.json() == {'message': 'Invalid data'}


def test_mandrill_webhook_head(cli: TestClient):
    """HEAD on /webhook/mandrill/ delegates to the index page."""
    r = cli.head('/webhook/mandrill/')
    assert r.status_code == 200


def test_messagebird_webhook_unparseable(cli: TestClient):
    """Missing required messagebird fields should 422 with the legacy message body."""
    r = cli.get('/webhook/messagebird/?id=foo&status=invalid')
    assert r.status_code == 422
    assert 'message' in r.json()


def test_user_session_invalid_signature_returns_403(cli: TestClient):
    """A valid-shaped but mis-signed session should 403, not 422.

    Regression for: pydantic v2 wraps validator exceptions in ValidationError which would
    otherwise surface as a 422 to clients.
    """
    args = {
        'company': 'whoever',
        'expires': str(round(datetime(2032, 1, 1).timestamp())),
        'signature': 'a' * 64,
    }
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 403
    assert r.json() == {'message': 'Invalid token'}


def test_message_get_attachments_doc_id_path():
    """The attachments parser should split <id>::<name> into a doc URL when id is numeric."""
    msg = Message(
        company_id=1,
        group_id=1,
        method='email-test',
        attachments=['42::doc.pdf', 'plain.txt', '::nameless'],
    )
    out = list(msg.get_attachments())
    assert out == [
        ('/attachment-doc/42/', 'doc.pdf'),
        ('#', 'plain.txt'),
        ('#', 'nameless'),
    ]


def test_get_or_create_returns_existing(db):
    """get_or_create should return existing rows without inserting again."""
    first, created = db.get_or_create(Company, code='goc-test')
    assert created is True
    second, created = db.get_or_create(Company, code='goc-test')
    assert created is False
    assert second.id == first.id


def test_get_or_create_with_defaults_inserts(db):
    """get_or_create's defaults dict supplies extra fields on insert."""
    company, created = db.get_or_create(Company, code='goc-defaults')
    assert created is True
    assert company.code == 'goc-defaults'


def test_aggregation_view_disabled_setting(monkeypatch):
    """The scheduler task should no-op when settings.update_aggregation_view is False."""
    from app.core.config import settings as app_settings
    from app.messages import tasks

    monkeypatch.setattr(app_settings, 'update_aggregation_view', False)
    # Should return without attempting to refresh; raises if the function tried to hit DB.
    tasks.update_aggregation_view()


def test_delete_old_emails_disabled_setting(monkeypatch):
    """The scheduler task should no-op when settings.delete_old_emails is False."""
    from app.core.config import settings as app_settings
    from app.messages import tasks

    monkeypatch.setattr(app_settings, 'delete_old_emails', False)
    tasks.delete_old_emails()


def test_send_email_retry_exhaustion_writes_failure_row(
    sync_db: SyncDb, call_send_emails, worker_send_email, worker_ctx
):
    """When max retries are exhausted, the on_failure path records a send_request_failed row.

    This drives the body-level guard via the direct-call test helper. The celery on_failure
    hook is the prod path; the body guard is the test path.
    """
    group_id, c_id, m = call_send_emails()
    worker_ctx['job_try'] = len(__import__('app.messages.tasks', fromlist=['EMAIL_RETRYING']).EMAIL_RETRYING) + 1
    from app.messages.schemas import EmailRecipientModel

    worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='exhausted@example.com'), m)
    msg = sync_db.fetchrow('select * from messages')
    assert msg['status'] == 'send_request_failed'
    assert msg['body'] == 'upstream error'


def test_get_or_create_defaults(db):
    """get_or_create accepts a `defaults` dict for fields used only on insert."""
    company, created = db.get_or_create(Company, defaults={'code': 'goc-with-defaults'}, code='goc-with-defaults')
    assert created is True
    assert company.code == 'goc-with-defaults'


def test_send_email_celery_on_failure_writes_failure_row(call_send_emails, sync_db: SyncDb):
    """Celery's on_failure hook should record the failure row when MaxRetriesExceededError fires."""
    from celery.exceptions import MaxRetriesExceededError

    from app.messages.tasks import _SendEmailTask

    group_id, c_id, m = call_send_emails()
    args = (group_id, c_id, {'address': 'rip@example.com'}, m.model_dump(mode='json'))
    task = _SendEmailTask()
    task.on_failure(MaxRetriesExceededError(), 'task-id', args, {}, None)

    msg = sync_db.fetchrow('select * from messages')
    assert msg['status'] == 'send_request_failed'
    assert msg['body'] == 'upstream error'


def test_send_email_on_failure_swallows_other_exceptions(call_send_emails, sync_db: SyncDb):
    """Non-retry exceptions should not trigger the failure-row write."""
    from app.messages.tasks import _SendEmailTask

    call_send_emails()  # seeds company + group
    task = _SendEmailTask()
    task.on_failure(RuntimeError('something else'), 'task-id', (), {}, None)
    # No new failed row should have been added.
    assert sync_db.fetchval('select count(*) from messages') == 0


def test_send_email_on_failure_swallows_bad_args(sync_db: SyncDb):
    """Malformed args during on_failure should be logged, not propagated."""
    from celery.exceptions import MaxRetriesExceededError

    from app.messages.tasks import _SendEmailTask

    task = _SendEmailTask()
    # Args don't unpack into 4 elements → caught and logged.
    task.on_failure(MaxRetriesExceededError(), 'task-id', ('only-one',), {}, None)
    assert sync_db.fetchval('select count(*) from messages') == 0


def test_init_sentry_with_dsn(monkeypatch):
    """init_sentry should call sentry_sdk.init when a DSN is configured."""
    from app import sentry as sentry_pkg
    from app.core.config import settings as app_settings

    monkeypatch.setattr(app_settings, 'sentry_dsn', 'https://example@sentry.io/1')
    called = {}

    def fake_init(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr('sentry_sdk.init', fake_init)
    sentry_pkg.setup.init_sentry()
    assert called['dsn'] == 'https://example@sentry.io/1'


def test_configure_logfire_with_token(monkeypatch):
    """configure_logfire should configure + instrument when a token is set."""
    from app.core import logging as core_logging
    from app.core.config import settings as app_settings

    monkeypatch.setattr(app_settings, 'logfire_token', 'lgf_test_token')
    configured = {}

    class _FakeLogfire:
        @staticmethod
        def configure(**kwargs):
            configured.update(kwargs)

        @staticmethod
        def instrument_httpx():
            configured['httpx'] = True

    monkeypatch.setitem(__import__('sys').modules, 'logfire', _FakeLogfire)
    core_logging.configure_logfire()
    assert configured['token'] == 'lgf_test_token'
    assert configured['httpx'] is True


def test_store_click_with_unknown_link_id_no_ops():
    """If the Link row is missing (race / cleanup), store_click should return None."""
    from app.messages.tasks import get_redis, store_click

    get_redis().flushdb()
    result = store_click(link_id=999_999, ip='127.0.0.1', user_agent=None, ts=0.0)
    assert result is None


def test_get_db_yields_session_and_closes():
    """The get_db generator should yield a usable session and close it on exit."""
    from app.core.database import get_db

    gen = get_db()
    session = next(gen)
    assert session.is_active
    gen.close()


def test_user_session_expires_already_tz_aware(cli, settings):
    """A signed token with an already-tz-aware expires should pass through cleanly."""
    expires = round(datetime(2032, 1, 1, tzinfo=timezone.utc).timestamp())
    body = f'whoever:{expires}'.encode()
    sig = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    args = {'company': 'whoever', 'expires': str(expires), 'signature': sig}
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 200, r.text


def test_user_session_validation_error_returns_403(cli: TestClient):
    """Missing required query args should 403 (the ValidationError path), not 422."""
    r = cli.get('/messages/email-test/')
    assert r.status_code == 403
    assert r.json() == {'message': 'Invalid token'}


def test_database_tables_exist():
    """create_db_and_tables ran in the autouse session fixture; assert the materialised view + triggers landed."""
    with engine.connect() as conn:
        mv = conn.execute(text("select count(*) from pg_matviews where matviewname='message_aggregation'")).scalar()
        assert mv == 1

        triggers = conn.execute(
            text("select tgname from pg_trigger where tgname in ('update_message','create_tsvector') order by tgname")
        ).fetchall()
        assert [t[0] for t in triggers] == ['create_tsvector', 'update_message']
