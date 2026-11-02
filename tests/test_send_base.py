"""Bulk sends store the shared message (m_base) once in Redis instead of in every queued task.

Each queued send_email / send_sms task used to carry its own full copy of m_base. A few large email
sends (about 26,000 recipients in total) filled Redis to maxmemory, and every endpoint that enqueues
a task returned 500. Measured locally, 26,000 queued tasks with a 4-6 KB m_base take 200-300 MiB.
"""

import json
import uuid

import pytest
from celery.exceptions import MaxRetriesExceededError

from app.messages.tasks import (
    SEND_BASE_TTL,
    SendBaseMissing,
    _SendEmailTask,
    _SendSMSTask,
    get_redis,
    send_email,
    send_sms,
)
from tests.conftest import SyncDb

AUTH = {'Authorization': 'testing-key'}
BIG_TEMPLATE = '<body>\n{{{ message }}}\n' + 'x' * 50_000 + '\n</body>'


def _email_data(**extra) -> dict:
    data = dict(
        uid=str(uuid.uuid4()),
        main_template=BIG_TEMPLATE,
        company_code='foobar',
        from_address='Sender Name <sender@example.org>',
        method='email-test',
        subject_template='test message',
        context={'message': 'this is a test'},
        recipients=[{'address': f'{i}@example.org'} for i in range(3)],
    )
    data.update(extra)
    return data


def _sms_data(**extra) -> dict:
    data = dict(
        uid=str(uuid.uuid4()),
        # Long, but under the 9-part (1,377 character) SMS limit so it still renders.
        main_template='this is a test {{ variable }}' + 'x' * 1_000,
        company_code='foobar',
        from_name='FooBar',
        method='sms-test',
        context={'variable': 'apples'},
        recipients=[{'number': f'0789112385{i}'} for i in range(3)],
    )
    data.update(extra)
    return data


@pytest.fixture
def captured_email_tasks(monkeypatch):
    calls = []
    monkeypatch.setattr(send_email, 'delay', lambda *args: calls.append(args))
    return calls


@pytest.fixture
def captured_sms_tasks(monkeypatch):
    calls = []
    monkeypatch.setattr(send_sms, 'delay', lambda *args: calls.append(args))
    return calls


def _assert_stored_once(group_id: int, main_template: str) -> None:
    redis = get_redis()
    assert redis.keys('send-base:*') == [f'send-base:{group_id}']
    stored = json.loads(redis.get(f'send-base:{group_id}'))
    assert stored['main_template'] == main_template
    assert stored['recipients'] == []
    assert 0 < redis.ttl(f'send-base:{group_id}') <= SEND_BASE_TTL


def test_bulk_email_stores_shared_message_once(cli, captured_email_tasks):
    r = cli.post('/send/email/', json=_email_data(), headers=AUTH)
    assert r.status_code == 201, r.text
    assert r.json() == {'message': '201 job enqueued'}

    assert [len(args) for args in captured_email_tasks] == [3, 3, 3]
    assert [args[2]['address'] for args in captured_email_tasks] == ['0@example.org', '1@example.org', '2@example.org']
    # Each task carries only ids and its recipient, not the 50 KB template.
    assert max(len(json.dumps(args)) for args in captured_email_tasks) < 500
    _assert_stored_once(captured_email_tasks[0][0], BIG_TEMPLATE)


def test_single_recipient_email_keeps_message_inline(cli, captured_email_tasks):
    r = cli.post('/send/email/', json=_email_data(recipients=[{'address': 'solo@example.org'}]), headers=AUTH)
    assert r.status_code == 201, r.text

    assert [len(args) for args in captured_email_tasks] == [4]
    assert captured_email_tasks[0][3]['main_template'] == BIG_TEMPLATE
    assert get_redis().keys('send-base:*') == []


def test_bulk_email_sends_every_recipient(cli, sync_db: SyncDb):
    r = cli.post('/send/email/', json=_email_data(), headers=AUTH)
    assert r.status_code == 201, r.text

    rows = sync_db.fetch('select to_address, status from messages order by to_address')
    assert [(row['to_address'], row['status']) for row in rows] == [
        ('0@example.org', 'send'),
        ('1@example.org', 'send'),
        ('2@example.org', 'send'),
    ]


def test_send_email_without_stored_message_raises(cli, captured_email_tasks, sync_db: SyncDb):
    r = cli.post('/send/email/', json=_email_data(), headers=AUTH)
    assert r.status_code == 201, r.text
    group_id, company_id, recipient = captured_email_tasks[0]
    get_redis().delete(f'send-base:{group_id}')

    with pytest.raises(SendBaseMissing, match=f'group {group_id}'):
        send_email(group_id, company_id, recipient)
    assert sync_db.fetchval('select count(*) from messages') == 0


def test_send_email_on_failure_reads_stored_message(cli, captured_email_tasks, sync_db: SyncDb):
    r = cli.post('/send/email/', json=_email_data(), headers=AUTH)
    assert r.status_code == 201, r.text

    _SendEmailTask().on_failure(MaxRetriesExceededError(), 'task-id', captured_email_tasks[0], {}, None)

    msg = sync_db.fetchrow('select to_address, status, body from messages')
    assert (msg['to_address'], msg['status'], msg['body']) == ('0@example.org', 'send_request_failed', 'upstream error')


def test_bulk_sms_stores_shared_message_once(cli, captured_sms_tasks):
    data = _sms_data()
    r = cli.post('/send/sms/', json=data, headers=AUTH)
    assert r.status_code == 201, r.text
    assert r.json() == {'status': 'enqueued', 'spend': None}

    assert [len(args) for args in captured_sms_tasks] == [3, 3, 3]
    assert max(len(json.dumps(args)) for args in captured_sms_tasks) < 500
    _assert_stored_once(captured_sms_tasks[0][0], data['main_template'])


def test_single_recipient_sms_keeps_message_inline(cli, captured_sms_tasks):
    data = _sms_data(recipients=[{'number': '07891123850'}])
    r = cli.post('/send/sms/', json=data, headers=AUTH)
    assert r.status_code == 201, r.text

    assert [len(args) for args in captured_sms_tasks] == [4]
    assert captured_sms_tasks[0][3]['main_template'] == data['main_template']
    assert get_redis().keys('send-base:*') == []


def test_bulk_sms_sends_every_recipient(cli, sync_db: SyncDb):
    r = cli.post('/send/sms/', json=_sms_data(), headers=AUTH)
    assert r.status_code == 201, r.text

    rows = sync_db.fetch('select to_address, status from messages order by to_address')
    assert [(row['to_address'], row['status']) for row in rows] == [
        ('+44 7891 123850', 'send'),
        ('+44 7891 123851', 'send'),
        ('+44 7891 123852', 'send'),
    ]


def test_send_sms_without_stored_message_raises(cli, captured_sms_tasks, sync_db: SyncDb):
    r = cli.post('/send/sms/', json=_sms_data(), headers=AUTH)
    assert r.status_code == 201, r.text
    group_id, company_id, recipient = captured_sms_tasks[0]
    get_redis().delete(f'send-base:{group_id}')

    with pytest.raises(SendBaseMissing, match=f'group {group_id}'):
        send_sms(group_id, company_id, recipient)
    assert sync_db.fetchval('select count(*) from messages') == 0


def test_send_sms_on_failure_reads_stored_message(cli, captured_sms_tasks, sync_db: SyncDb):
    r = cli.post('/send/sms/', json=_sms_data(), headers=AUTH)
    assert r.status_code == 201, r.text

    _SendSMSTask().on_failure(MaxRetriesExceededError(), 'task-id', captured_sms_tasks[0], {}, None)

    msg = sync_db.fetchrow('select to_address, status, body from messages')
    assert (msg['to_address'], msg['status'], msg['body']) == ('07891123850', 'send_request_failed', 'upstream error')
