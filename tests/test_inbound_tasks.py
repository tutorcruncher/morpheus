import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from unittest.mock import Mock

import dns.exception
import pytest

from app.core.celery import celery_app
from app.messages import tasks
from app.messages.tasks import CHAT_REPLY_RETRYING, deliver_chat_reply, read_inbound_reply
from tests.test_inbound_reply import PUBLISHED_KEY, REPLY_ADDRESS, SENT_SUBJECT, email_headers, raw_email, signed

TC2_PATH = '/tc2/comms/chat/email-reply/inbound/'
RECEIVED_AT = '2026-11-26T11:20:03Z'


@pytest.fixture(autouse=True)
def resolve(monkeypatch):
    """The DNS lookup of a signing key, answered with the key the tests sign with."""
    mock_resolve = Mock(return_value=[Mock(strings=[PUBLISHED_KEY])])
    monkeypatch.setattr('app.inbound.sender_proof.dns.resolver.resolve', mock_resolve)
    return mock_resolve


@pytest.fixture
def retries(monkeypatch):
    """
    The countdown of each retry either task asks for, in order. With `task_eager_propagates` off, Celery runs a
    retried task again at once, as a worker would after the countdown, in place of raising the Retry.
    """
    monkeypatch.setattr(celery_app.conf, 'task_eager_propagates', False)
    countdowns = []
    for task in (read_inbound_reply, deliver_chat_reply):

        def retry(*args, _retry=task.retry, **kwargs):
            countdowns.append(kwargs['countdown'])
            return _retry(*args, **kwargs)

        monkeypatch.setattr(task, 'retry', retry)
    return countdowns


def received_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def read(raw: str) -> None:
    read_inbound_reply.delay(raw, REPLY_ADDRESS, received_now())


def worker_messages(caplog) -> list[str]:
    """What the tasks themselves logged, without what Celery and httpx log around them."""
    return [record.getMessage() for record in caplog.records if record.name == 'worker']


def posts_to_tc2(dummy_state) -> list[dict]:
    return [json.loads(request.content) for request in dummy_state.log if request.url.path == TC2_PATH]


def payload(text: str = 'Yes, Tuesday works.') -> dict:
    return {
        'reply_address': REPLY_ADDRESS,
        'received_at': RECEIVED_AT,
        'idempotency_key': '9f2c0e5d6c0c3a0b7f4d1e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e',
        'from_address': 'client@example.org',
        'proof': 'recipient_signed',
        'proof_failure': None,
        'subject': f'Re: {SENT_SUBJECT}',
        'date': '2026-11-26T11:19:40+00:00',
        'text': text,
        'attachment_names': ['homework.pdf'],
    }


# read_inbound_reply ---------------------------------------------------------------------------------------------


def test_reply_is_read_and_posted_to_tutorcruncher_signed(settings, dummy_server, dummy_state):
    read_inbound_reply.delay(signed(raw_email()), REPLY_ADDRESS, received_now())
    assert dummy_server.log == [f'POST {TC2_PATH} > 200']
    request = dummy_state.log[0]
    assert request.headers['Content-Type'] == 'application/json'
    expected = hmac.new(settings.tc2_chat_reply_key.encode(), request.content, hashlib.sha256).hexdigest()
    assert request.headers['Webhook-Signature'] == expected
    posted = json.loads(request.content)
    assert (posted['reply_address'], posted['proof'], posted['text']) == (
        REPLY_ADDRESS,
        'recipient_signed',
        'Tuesday works, thanks.',
    )
    assert sorted(posted) == sorted(payload())


def test_automatic_reply_never_reaches_tutorcruncher(settings, dummy_server, caplog):
    caplog.set_level(logging.INFO, logger='worker')
    read(signed(raw_email(headers=email_headers(Auto_Submitted='auto-replied'))))
    assert dummy_server.log == []
    assert worker_messages(caplog) == ['inbound reply dropped: auto_reply']


def test_key_lookup_with_no_answer_is_tried_four_times_then_sent_unproven(
    settings, dummy_server, dummy_state, resolve, retries
):
    resolve.side_effect = dns.exception.Timeout
    read(signed(raw_email()))
    assert resolve.call_count == 4
    assert retries == [120, 120, 120]
    assert dummy_server.log == [f'POST {TC2_PATH} > 200']
    (posted,) = posts_to_tc2(dummy_state)
    assert (posted['proof'], posted['proof_failure'], posted['text']) == (None, 'dns_error', '')


def test_key_lookup_answered_on_a_later_try_sends_a_proof(settings, dummy_state, resolve, retries):
    resolve.side_effect = [dns.exception.Timeout, [Mock(strings=[PUBLISHED_KEY])]]
    read(signed(raw_email()))
    assert resolve.call_count == 2
    assert retries == [120]
    (posted,) = posts_to_tc2(dummy_state)
    assert (posted['proof'], posted['proof_failure']) == ('recipient_signed', None)


def test_error_while_reading_goes_to_sentry_and_is_not_raised(settings, dummy_server, monkeypatch, caplog):
    """An exception that leaves the task has its message logged by Celery and recorded by Logfire."""
    error = ValueError('could not read "Tuesday works" from client@example.org')
    monkeypatch.setattr(tasks, 'read_reply', Mock(side_effect=error))
    capture_exception = Mock()
    monkeypatch.setattr(tasks.sentry_sdk, 'capture_exception', capture_exception)
    read(signed(raw_email()))
    capture_exception.assert_called_once_with(error)
    assert dummy_server.log == []
    assert worker_messages(caplog) == ['inbound reply failed: ValueError']


def test_error_while_posting_goes_to_sentry_and_is_not_raised(settings, monkeypatch, caplog):
    error = RuntimeError('could not send "Tuesday works"')
    monkeypatch.setattr(tasks, 'post_chat_reply', Mock(side_effect=error))
    capture_exception = Mock()
    monkeypatch.setattr(tasks.sentry_sdk, 'capture_exception', capture_exception)
    deliver_chat_reply.delay(payload())
    capture_exception.assert_called_once_with(error)
    assert worker_messages(caplog) == ['inbound reply 9f2c0e5d6c0c: not delivered, RuntimeError']


@pytest.mark.parametrize('how', ['delay', 'signature'])
def test_arguments_are_queued_without_the_text_celery_logs(how, monkeypatch):
    """Celery logs `argsrepr`, the start of a task's arguments, when a worker receives or finishes the task."""
    monkeypatch.setattr(celery_app.conf, 'task_always_eager', False)
    send_task = Mock()
    monkeypatch.setattr(celery_app, 'send_task', send_task)
    if how == 'delay':
        read_inbound_reply.delay(signed(raw_email()), REPLY_ADDRESS, RECEIVED_AT)
        deliver_chat_reply.delay(payload())
    else:
        # how a retry queues the task again
        read_inbound_reply.signature((signed(raw_email()), REPLY_ADDRESS, RECEIVED_AT), countdown=120).apply_async()
        deliver_chat_reply.signature((payload(),), argsrepr='the arguments').apply_async()
    assert send_task.call_count == 2
    for call in send_task.call_args_list:
        assert (call.kwargs['argsrepr'], call.kwargs['kwargsrepr']) == ('[Filtered]', '[Filtered]')


# deliver_chat_reply ---------------------------------------------------------------------------------------------


def test_outcome_tutorcruncher_answers_is_logged(settings, dummy_server, caplog):
    caplog.set_level(logging.INFO, logger='worker')
    deliver_chat_reply.delay(payload())
    assert dummy_server.log == [f'POST {TC2_PATH} > 200']
    assert worker_messages(caplog) == ['inbound reply 9f2c0e5d6c0c: outcome=posted reason=']


def test_answer_that_is_not_json_is_logged_and_not_retried(settings, dummy_server, retries, caplog):
    caplog.set_level(logging.INFO, logger='worker')
    deliver_chat_reply.delay(payload('__tc2_not_json__'))
    assert dummy_server.log == [f'POST {TC2_PATH} > 200']
    assert retries == []
    assert worker_messages(caplog) == ['inbound reply 9f2c0e5d6c0c: outcome=None reason=']


def test_server_error_is_retried_five_times_then_given_up(settings, dummy_server, retries, caplog):
    deliver_chat_reply.delay(payload('__tc2_500__'))
    assert dummy_server.log == [f'POST {TC2_PATH} > 500'] * 6
    assert retries == [10, 60, 300, 900, 3600] == CHAT_REPLY_RETRYING
    assert worker_messages(caplog) == [
        'inbound reply 9f2c0e5d6c0c: not delivered after 6 tries, last failure status 500'
    ]


def test_timeout_is_retried_five_times_then_given_up(settings, dummy_state, retries, caplog):
    deliver_chat_reply.delay(payload('__tc2_timeout__'))
    assert len(posts_to_tc2(dummy_state)) == 6
    assert retries == [10, 60, 300, 900, 3600]
    assert worker_messages(caplog) == [
        'inbound reply 9f2c0e5d6c0c: not delivered after 6 tries, last failure ReadTimeout'
    ]


@pytest.mark.parametrize('status', [400, 403])
def test_refused_post_is_not_retried(status, settings, dummy_server, retries, caplog):
    deliver_chat_reply.delay(payload(f'__tc2_{status}__'))
    assert dummy_server.log == [f'POST {TC2_PATH} > {status}']
    assert retries == []
    assert worker_messages(caplog) == [f'inbound reply 9f2c0e5d6c0c: not delivered, TutorCruncher answered {status}']


def test_any_other_status_is_not_retried(settings, dummy_server, retries, monkeypatch, caplog):
    monkeypatch.setattr(settings, 'tc2_chat_reply_url', 'http://dummy/tc2/not-the-endpoint/')
    deliver_chat_reply.delay(payload())
    assert dummy_server.log == ['POST /tc2/not-the-endpoint/ > 404']
    assert retries == []
    assert worker_messages(caplog) == ['inbound reply 9f2c0e5d6c0c: not delivered, TutorCruncher answered 404']


@pytest.mark.parametrize('setting', ['mandrill_inbound_webhook_key', 'tc2_chat_reply_url', 'tc2_chat_reply_key'])
def test_nothing_is_posted_once_a_setting_is_taken_away(setting, settings, dummy_server, monkeypatch, caplog):
    monkeypatch.setattr(settings, setting, '')
    deliver_chat_reply.delay(payload())
    assert dummy_server.log == []
    assert worker_messages(caplog) == ['inbound reply 9f2c0e5d6c0c: not delivered, inbound replies are not set up']


# What is logged -------------------------------------------------------------------------------------------------


def test_log_lines_hold_no_address_subject_or_text(settings, resolve, retries, caplog):
    caplog.set_level(logging.DEBUG)
    read(signed(raw_email()))
    read(signed(raw_email(body='__tc2_500__ Tuesday works')))
    read(signed(raw_email(body='__tc2_403__ Tuesday works')))
    read(raw_email().decode())
    resolve.side_effect = dns.exception.Timeout
    read(signed(raw_email()))
    logged = ''
    for record in caplog.records:
        extra = dict(record.__dict__)
        # Run in the test's own process, a task has no `argsrepr`, so Celery logs its arguments here. A worker
        # logs the `argsrepr` the task was queued with: test_arguments_are_queued_without_the_text_celery_logs.
        if isinstance(extra.get('data'), dict):
            extra['data'] = {name: value for name, value in extra['data'].items() if name != 'args'}
            extra.pop('args', None)
        logged += f'{record.getMessage()} {extra}\n'
    assert 'inbound reply' in logged
    for private in ('reply+', 'reply.example.com', 'client@', 'example.org', 'Tuesday', 'unread messages'):
        assert private not in logged, private
