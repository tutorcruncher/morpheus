"""Regression tests for the PR #495 review findings.

Each test is written test-first and fails against the unfixed code, then passes once the
corresponding fix is applied. See the inline review comments on the PR for context.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from app.core.config import Settings
from app.core.database import engine
from app.ext.clients import ApiError
from app.messages import tasks
from app.messages.models import Company, Message, MessageGroup
from app.messages.schemas import EmailRecipientModel, EmailSendModel, SmsSendModel
from app.messages.tasks import get_redis, store_click
from tests.conftest import SyncDb
from tests.test_email import send_with_link


# --- Finding 1: store_click crashes on Heroku's millisecond X-Request-Start ---
def test_store_click_scales_heroku_millisecond_timestamp(send_email, tmpdir, sync_db: SyncDb):
    send_with_link(send_email, tmpdir)
    link = sync_db.fetchrow('select id from links')
    get_redis().flushdb()

    # Heroku's router sets X-Request-Start in epoch milliseconds, e.g. 2032-06-01 in ms.
    ms_ts = 1969660800000.0
    result = store_click(link_id=link['id'], ip='1.2.3.4', user_agent=None, ts=ms_ts)

    assert result is None
    event = sync_db.fetchrow('select * from events')
    assert event['ts'] == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)


# --- Finding 2: delete-subaccount must not rely on a CASCADE the prod schema lacks ---
def _set_company_fk_action(action: str) -> None:
    """Switch the messages/message_groups → companies FK ON DELETE action.

    The test schema is built CASCADE, but production carries the legacy ON DELETE RESTRICT
    constraints. Recreating RESTRICT here reproduces the production failure mode.
    """
    targets = [
        ('messages', 'messages_company_id_fkey'),
        ('message_groups', 'message_groups_company_id_fkey'),
    ]
    with engine.begin() as conn:
        for table, name in targets:
            conn.execute(text(f'ALTER TABLE {table} DROP CONSTRAINT {name}'))
            conn.execute(
                text(
                    f'ALTER TABLE {table} ADD CONSTRAINT {name} '
                    f'FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE {action}'
                )
            )


def test_delete_subaccount_with_restrict_fks(cli, db, sync_db: SyncDb):
    _set_company_fk_action('RESTRICT')
    try:
        company = Company(code='delsub-test')
        db.add(company)
        db.commit()
        db.refresh(company)
        group = MessageGroup(uuid=uuid.uuid4(), company_id=company.id, message_method='email-mandrill')
        db.add(group)
        db.commit()
        db.refresh(group)
        db.add(
            Message(
                group_id=group.id,
                company_id=company.id,
                method='email-mandrill',
                status='send',
                to_address='x@example.com',
            )
        )
        db.commit()

        r = cli.post(
            '/delete-subaccount/email-test/',
            json={'company_code': 'delsub-test'},
            headers={'Authorization': 'testing-key'},
        )
        assert r.status_code == 200, r.text
        assert sync_db.fetchval('select count(*) from messages') == 0
        assert sync_db.fetchval('select count(*) from message_groups') == 0
        assert sync_db.fetchval("select count(*) from companies where code = 'delsub-test'") == 0
    finally:
        try:
            db.rollback()
        except Exception:
            pass
        _set_company_fk_action('CASCADE')


# --- Finding 3: send_sms must not silently drop the SMS on a MessageBird error ---
def test_send_sms_records_failure_on_messagebird_error(cli, monkeypatch, sync_db: SyncDb):
    def boom(self, *args, **kwargs):
        raise ApiError('POST', 'http://dummy/messagebird/messages', 400, 'bad request')

    monkeypatch.setattr(tasks.MessageBird, 'post', boom)

    data = dict(
        uid=str(uuid.uuid4()),
        main_template='hi {{ x }}',
        company_code='sms-fail',
        from_name='FooBar',
        method='sms-messagebird',
        country_code='GB',
        context={'x': 'y'},
        recipients=[{'number': '07896541236'}],
    )
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    msg = sync_db.fetchrow('select * from messages')
    assert msg is not None, 'no Message row recorded for the failed SMS'
    assert msg['status'] == 'send_request_failed'


# --- Finding 4: a non-retryable Mandrill ApiError must record a failure row ---
def test_mandrill_non_retryable_error_records_failure_row(
    call_send_emails, worker_send_email, worker_ctx, sync_db: SyncDb
):
    group_id, c_id, m = call_send_emails(method='email-mandrill', subject_template='__500__')
    worker_ctx['job_try'] = 1
    # NB: not an @example.com address — that short-circuits the real Mandrill call in tests.
    worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='hard@testing.com'), m)

    msg = sync_db.fetchrow('select * from messages')
    assert msg is not None, 'no Message row recorded for the hard Mandrill error'
    assert msg['status'] == 'send_request_failed'


# --- Finding 5: mustache_partials=None must not crash rendering ---
def test_email_renders_partial_template_without_mustache_partials(send_email, tmpdir):
    # main_template references a partial but no mustache_partials map is supplied.
    message_id = send_email(
        main_template='hello {{> missing_p }} world',
        context={},
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert 'hello' in msg_file


# --- Finding 6: REDISCLOUD_URL must take precedence over REDIS_URL ---
def test_redis_url_prefers_rediscloud(monkeypatch):
    monkeypatch.setenv('REDISCLOUD_URL', 'redis://cloud:6379/1')
    monkeypatch.setenv('REDIS_URL', 'redis://plain:6379/2')
    assert Settings().redis_url == 'redis://cloud:6379/1'


def test_redis_url_falls_back_to_redis_url(monkeypatch):
    monkeypatch.delenv('REDISCLOUD_URL', raising=False)
    monkeypatch.setenv('REDIS_URL', 'redis://plain:6379/2')
    assert Settings().redis_url == 'redis://plain:6379/2'


# --- Finding 7: numeric str fields coerced on the send models (pydantic v1 parity) ---
def test_email_send_model_coerces_numeric_company_code():
    m = EmailSendModel(
        uid=uuid.uuid4(),
        subject_template='s',
        company_code=12345,
        from_address='Test <a@example.com>',
        method='email-test',
        recipients=[],
    )
    assert m.company_code == '12345'


def test_sms_send_model_coerces_numeric_company_code():
    m = SmsSendModel(
        uid='a' * 20,
        main_template='t',
        company_code=12345,
        method='sms-test',
        recipients=[],
    )
    assert m.company_code == '12345'


# --- Finding 8: sms billing GET with no body must 400, not 500 ---
def test_sms_billing_missing_body_returns_400(cli, db):
    db.add(Company(code='billing-empty'))
    db.commit()
    r = cli.get('/billing/sms-test/billing-empty/', headers={'Authorization': 'testing-key'})
    assert r.status_code == 400, r.text
