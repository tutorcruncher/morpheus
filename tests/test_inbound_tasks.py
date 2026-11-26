import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from unittest.mock import Mock

import dns.exception
import pytest

from app.core.celery import celery_app
from app.messages import tasks
from app.messages.tasks import CHAT_REPLY_RETRYING, deliver_chat_reply, read_inbound_reply
from app.sentry.setup import before_send, init_sentry
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


# Sentry ---------------------------------------------------------------------------------------------------------


def sentry_event(frames: list[tuple[str, str]], url: str | None = None) -> dict:
    """An event as Sentry builds one for an exception raised through `frames`, each a module and a function."""
    event = {
        'exception': {
            'values': [
                {
                    'type': 'HeaderParseError',
                    'value': 'could not read "Tuesday works"',
                    'stacktrace': {
                        'frames': [
                            {'module': module, 'function': function, 'vars': {'raw': 'the email'}}
                            for module, function in frames
                        ]
                    },
                }
            ]
        },
    }
    if url is not None:
        event['request'] = {'url': url, 'data': {'mandrill_events': 'the email'}}
    return event


def without_the_email(frames: list[tuple[str, str]], value: str) -> dict:
    return {
        'exception': {
            'values': [
                {
                    'type': 'HeaderParseError',
                    'value': value,
                    'stacktrace': {'frames': [{'module': module, 'function': function} for module, function in frames]},
                }
            ]
        },
    }


@pytest.mark.parametrize(
    'ours',
    [
        ('app.inbound.reply', 'read_reply'),
        ('app.inbound', 'anything'),
        ('app.messages.tasks', 'read_inbound_reply'),
        ('app.messages.tasks', 'deliver_chat_reply'),
        ('app.messages.api.webhooks', 'mandrill_inbound_view'),
        ('app.messages.api.webhooks', '_queue_inbound_replies'),
    ],
)
def test_event_from_reading_a_reply_is_sent_without_the_email(ours):
    frames = [('celery.app.trace', 'trace_task'), ours, ('email.parser', 'parsestr')]
    assert before_send(sentry_event(frames), {}) == without_the_email(frames, '[Filtered]')


@pytest.mark.parametrize('raised_in', ['encodings.utf_8', 'httpx._client', 'tests.test_inbound_tasks', ''])
def test_event_from_reading_a_reply_loses_the_message_of_any_code_but_ours(raised_in):
    frames = [('app.messages.tasks', 'read_inbound_reply'), (raised_in, 'decode')]
    assert before_send(sentry_event(frames), {}) == without_the_email(frames, '[Filtered]')


def test_event_from_reading_a_reply_keeps_a_message_of_our_own():
    frames = [('celery.app.trace', 'trace_task'), ('app.inbound.sender_proof', 'prove_sender')]
    event = before_send(sentry_event(frames), {})
    assert event == without_the_email(frames, 'could not read "Tuesday works"')


def test_event_from_the_inbound_webhook_is_sent_without_the_request():
    """Raised before any of our own frames, as by the form parser: only the path says what the event holds."""
    frames = [('starlette.routing', 'app'), ('json.decoder', 'decode')]
    event = before_send(sentry_event(frames, url='https://morpheus.example.com/webhook/mandrill/inbound/?a=b'), {})
    assert event == without_the_email(frames, '[Filtered]')


@pytest.mark.parametrize(
    'frames,url',
    [
        ([('app.messages.tasks', 'send_email'), ('email.parser', 'parsestr')], None),
        ([('app.messages.api.webhooks', 'mandrill_webhook_view')], 'https://morpheus.example.com/webhook/mandrill/'),
        ([('app.inbounds', 'other')], 'https://morpheus.example.com/webhook/mandrill/inbound/more/'),
    ],
)
def test_other_events_keep_their_request_and_variables(frames, url):
    event = sentry_event(frames, url=url)
    assert before_send(sentry_event(frames, url=url), {}) == event


@pytest.mark.parametrize('event', [{}, {'message': 'inbound reply failed: ValueError'}, {'exception': None}])
def test_event_without_an_exception_is_left_alone(event):
    assert before_send(dict(event), {}) == event


def test_sentry_is_set_up_to_filter_events_and_never_send_task_arguments(settings, monkeypatch):
    monkeypatch.setattr(settings, 'sentry_dsn', 'https://example@sentry.io/1')
    init = Mock()
    monkeypatch.setattr('sentry_sdk.init', init)
    init_sentry()
    assert init.call_args.kwargs['send_default_pii'] is False
    assert init.call_args.kwargs['before_send'] is before_send


def test_failed_task_reaches_sentry_without_the_email(settings, monkeypatch):
    """The real SDK, with a transport that keeps the events: arguments, variables and the message are all gone."""
    import sentry_sdk
    from sentry_sdk.transport import Transport

    sent = []

    class KeepEvents(Transport):
        def capture_envelope(self, envelope):
            sent.extend(item.payload.json for item in envelope.items if item.type == 'event')

    def read_reply(raw, *args, **kwargs):
        # the email package raises a ValueError that quotes the start of the email
        return parsedate_to_datetime(raw[:40])

    monkeypatch.setattr(tasks, 'read_reply', read_reply)
    monkeypatch.setattr(settings, 'sentry_dsn', 'https://example@sentry.io/1')
    with sentry_sdk.isolation_scope(), sentry_sdk.new_scope():
        client = sentry_sdk.Client(
            dsn=settings.sentry_dsn, transport=KeepEvents, send_default_pii=False, before_send=before_send
        )
        sentry_sdk.get_current_scope().set_client(client)
        read(signed(raw_email()))
        client.flush()
    assert len(sent) >= 1
    for event in sent:
        text = json.dumps(event)
        for private in ('reply+', 'client@example.org', 'Tuesday', 'DKIM-Signature', 'unread messages'):
            assert private not in text, private
    (exception,) = [e for event in sent for e in (event.get('exception') or {}).get('values', [])]
    assert (exception['type'], exception['value']) == ('ValueError', '[Filtered]')
    assert exception['stacktrace']['frames'][-1]['module'] == 'email.utils'
    assert all('vars' not in frame for frame in exception['stacktrace']['frames'])
