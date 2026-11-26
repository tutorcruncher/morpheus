import base64
import hashlib
import hmac
import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from urllib.parse import parse_qsl, urlencode

import pytest
from fastapi.testclient import TestClient

from app.messages.api import webhooks
from app.messages.api.webhooks import MAX_INBOUND_POST_BYTES, _mandrill_signature
from tests.test_inbound_reply import PUBLISHED_KEY, REPLY_ADDRESS, raw_email, signed

URL = '/webhook/mandrill/inbound/'
SIGNED_URL = 'https://localhost/webhook/mandrill/inbound/'
TC2_PATH = '/tc2/comms/chat/email-reply/inbound/'
FORM = {'Content-Type': 'application/x-www-form-urlencoded'}


@pytest.fixture(autouse=True)
def resolve(monkeypatch):
    """The DNS lookup of a signing key, answered with the key the tests sign with."""
    mock_resolve = Mock(return_value=[Mock(strings=[PUBLISHED_KEY])])
    monkeypatch.setattr('app.inbound.sender_proof.dns.resolver.resolve', mock_resolve)
    return mock_resolve


@pytest.fixture
def queued(monkeypatch):
    """The arguments of each task the webhook queues, in place of the task running."""
    delay = Mock()
    monkeypatch.setattr(webhooks.read_inbound_reply, 'delay', delay)
    return delay


def view_messages(caplog) -> list[str]:
    """What the webhook itself logged, without what httpx logs of the test client's request."""
    return [record.getMessage() for record in caplog.records if record.name == 'views.webhooks']


def mandrill_signature(fields: dict[str, str], key: str = 'testing-mandrill-inbound-key') -> str:
    signed_text = SIGNED_URL + ''.join(f'{name}{fields[name]}' for name in sorted(fields))
    return base64.b64encode(hmac.new(key.encode(), signed_text.encode(), hashlib.sha1).digest()).decode()


def post_fields(cli: TestClient, fields: dict[str, str], signature: str | None = None):
    if signature is None:
        signature = mandrill_signature(fields)
    return cli.post(URL, data=fields, headers={'X-Mandrill-Signature': signature})


def post_events(cli: TestClient, events) -> object:
    return post_fields(cli, {'mandrill_events': json.dumps(events)})


def inbound(raw: str, address: str = REPLY_ADDRESS) -> dict:
    return {'event': 'inbound', 'ts': 1, 'msg': {'email': address, 'raw_msg': raw, 'text': 'what Mandrill read'}}


def test_head_is_answered_so_mandrill_accepts_the_url(cli: TestClient):
    r = cli.head(URL)
    assert r.status_code == 200
    assert r.content == b''


def test_signed_reply_is_read_and_posted_to_tutorcruncher(cli: TestClient, settings, dummy_server, dummy_state):
    before = datetime.now(timezone.utc).replace(microsecond=0)
    r = post_events(cli, [inbound(signed(raw_email()), REPLY_ADDRESS.upper())])
    assert r.status_code == 200
    assert r.json() == 'ok\n'
    assert dummy_server.log == [f'POST {TC2_PATH} > 200']
    request = dummy_state.log[0]
    expected = hmac.new(settings.tc2_chat_reply_key.encode(), request.content, hashlib.sha256).hexdigest()
    assert request.headers['Webhook-Signature'] == expected
    assert request.headers['Content-Type'] == 'application/json'
    posted = json.loads(request.content)
    received_at = datetime.strptime(posted.pop('received_at'), '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    assert before <= received_at <= before + timedelta(seconds=10)
    date = datetime.fromisoformat(posted.pop('date'))
    assert timedelta(minutes=4) < received_at - date < timedelta(minutes=6)
    assert len(posted.pop('idempotency_key')) == 64
    assert posted == {
        'reply_address': REPLY_ADDRESS,
        'from_address': 'client@example.org',
        'proof': 'recipient_signed',
        'proof_failure': None,
        'subject': 'Re: You have unread messages in Maths with Sam',
        'text': 'Tuesday works, thanks.',
        'attachment_names': [],
    }


def test_each_email_in_a_post_is_queued_with_only_its_address_and_itself(cli: TestClient, settings, queued):
    first, second = signed(raw_email()), signed(raw_email(body='Wednesday is better.'))
    r = post_events(cli, [inbound(first, REPLY_ADDRESS.upper()), inbound(second)])
    assert (r.status_code, r.json()) == (200, 'ok\n')
    assert queued.call_count == 2
    (raw, address, received_at), kwargs = queued.call_args_list[0]
    assert (raw, address, kwargs) == (first, REPLY_ADDRESS, {})
    assert datetime.strptime(received_at, '%Y-%m-%dT%H:%M:%SZ')
    assert queued.call_args_list[1].args == (second, REPLY_ADDRESS, received_at)


@pytest.mark.parametrize('signature', ['bm90IHRoZSBzaWduYXR1cmU=', '', 'sïgnature'])
def test_wrong_signature_is_refused_and_nothing_is_queued(signature, cli: TestClient, settings, queued, dummy_server):
    fields = {'mandrill_events': json.dumps([inbound(signed(raw_email()))])}
    r = cli.post(URL, data=fields, headers={'X-Mandrill-Signature': signature.encode()})
    assert r.status_code == 403
    assert r.json() == {'message': 'invalid signature'}
    queued.assert_not_called()
    assert dummy_server.log == []


def test_post_without_a_signature_is_refused(cli: TestClient, settings, queued):
    r = cli.post(URL, data={'mandrill_events': json.dumps([inbound(signed(raw_email()))])})
    assert r.status_code == 403
    assert r.json() == {'message': 'invalid signature'}
    queued.assert_not_called()


def test_signature_made_with_the_delivery_webhooks_key_is_refused(cli: TestClient, settings, queued):
    fields = {'mandrill_events': json.dumps([inbound(signed(raw_email()))])}
    r = post_fields(cli, fields, mandrill_signature(fields, key=settings.mandrill_webhook_key))
    assert r.status_code == 403
    assert r.json() == {'message': 'invalid signature'}
    queued.assert_not_called()


def test_signature_covers_every_field_of_the_post(cli: TestClient, settings, queued):
    fields = {'mandrill_events': json.dumps([inbound(signed(raw_email()))])}
    signature = mandrill_signature(fields)
    r = post_fields(cli, {**fields, 'another': 'field'}, signature)
    assert (r.status_code, r.json()) == (403, {'message': 'invalid signature'})
    r = post_fields(cli, {**fields, 'another': 'field'})
    assert (r.status_code, r.json()) == (200, 'ok\n')
    assert queued.call_count == 1


@pytest.mark.parametrize('setting', ['mandrill_inbound_webhook_key', 'tc2_chat_reply_url', 'tc2_chat_reply_key'])
def test_post_is_answered_and_dropped_until_every_setting_is_set(
    setting, cli: TestClient, settings, queued, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger='views.webhooks')
    monkeypatch.setattr(settings, setting, '')
    # as Mandrill posts when the route is saved, before the key it then shows can have been set
    r = cli.post(URL, data={'mandrill_events': '[]'}, headers={'X-Mandrill-Signature': 'anything'})
    assert (r.status_code, r.json()) == (200, 'ok\n')
    r = post_events(cli, [inbound(signed(raw_email()))])
    assert (r.status_code, r.json()) == (200, 'ok\n')
    queued.assert_not_called()
    assert view_messages(caplog) == ['inbound not set up', 'inbound not set up']


def test_events_that_are_not_emails_for_a_reply_address_are_left_out(cli: TestClient, settings, queued, caplog):
    caplog.set_level(logging.INFO, logger='views.webhooks')
    raw = signed(raw_email())
    events = [
        {'event': 'send', 'msg': {'email': REPLY_ADDRESS, 'raw_msg': raw}},
        {'msg': {'email': REPLY_ADDRESS, 'raw_msg': raw}},
        inbound(raw, 'reply@reply.example.com'),
        inbound(raw, 'reply+tooshort@reply.example.com'),
        inbound(raw, 'reply+abcdefghijklmnopqrstuvwxy1@reply.example.com'),
        inbound(raw, f'someone@example.org, {REPLY_ADDRESS}'),
        {'event': 'inbound', 'msg': {'email': REPLY_ADDRESS}},
        {'event': 'inbound', 'msg': {'email': REPLY_ADDRESS, 'raw_msg': ['not', 'text']}},
        {'event': 'inbound', 'msg': {'email': 17, 'raw_msg': raw}},
        {'event': 'inbound', 'msg': 'not an object'},
        {'event': 'inbound'},
        'not an object',
        None,
        inbound(raw),
    ]
    r = post_events(cli, events)
    assert (r.status_code, r.json()) == (200, 'ok\n')
    assert queued.call_count == 1
    assert queued.call_args.args[:2] == (raw, REPLY_ADDRESS)
    assert view_messages(caplog) == ['inbound post: 1 of 14 events queued']


def test_post_of_more_fields_than_mandrill_sends_is_refused_unread(cli: TestClient, settings, queued):
    """Read into a form, a body of empty fields costs far more than its size; it is refused at the eleventh."""
    fields = {'mandrill_events': json.dumps([inbound(signed(raw_email()))]), **{f'f{n}': '' for n in range(10)}}
    r = post_fields(cli, fields)
    assert (r.status_code, r.json()) == (403, {'message': 'invalid signature'})
    r = cli.post(URL, content=b'&' * 1_000_000, headers={**FORM, 'X-Mandrill-Signature': 'anything'})
    assert (r.status_code, r.json()) == (403, {'message': 'invalid signature'})
    queued.assert_not_called()


@pytest.mark.parametrize(
    'body',
    [
        b'mandrill_events=%5B%7B%22a%22%3A+%22b+c%22%7D%5D',
        b'a=1&b=&c&&a=2',
        b'a=%2B+%20&%61=b%3Dc=d',
        b'a=' + b'%41' * 50 + b'x%4a%c3%a9',
        b'',
    ],
)
def test_form_fields_are_read_as_python_reads_them_at_any_slice_size(body, monkeypatch):
    expected = dict(parse_qsl(body.decode(), keep_blank_values=True))
    for slice_bytes in (3, 4, 7, 1024):
        monkeypatch.setattr(webhooks, 'FORM_DECODE_SLICE_BYTES', slice_bytes)
        assert webhooks._form_fields(body) == expected


@pytest.mark.parametrize('body', [b'a=100%', b'a=%zz', b'%4=b', b'a=%ff', b'a=\xc3'])
def test_form_fields_with_an_escape_or_text_mandrill_never_sends_are_refused(body):
    with pytest.raises(ValueError):
        webhooks._form_fields(body)


def test_body_that_is_not_text_is_refused(cli: TestClient, settings, queued):
    r = cli.post(URL, content=b'mandrill_events=\xff\xfe', headers={**FORM, 'X-Mandrill-Signature': 'anything'})
    assert (r.status_code, r.json()) == (403, {'message': 'invalid signature'})
    queued.assert_not_called()


@pytest.mark.parametrize(
    'fields',
    [
        {'mandrill_events': 'not json'},
        {'mandrill_events': '{"event": "inbound"}'},
        {'mandrill_events': '"inbound"'},
        {'mandrill_events': '[' * 100_000},
        {'something': 'else'},
        {},
    ],
)
def test_signed_post_whose_events_cannot_be_read_is_still_answered(fields, cli: TestClient, settings, queued, caplog):
    r = post_fields(cli, fields)
    assert (r.status_code, r.json()) == (200, 'ok\n')
    queued.assert_not_called()
    assert view_messages(caplog) == ['inbound post not read: its events are not a list']


def test_three_megabyte_email_is_queued(cli: TestClient, settings, queued):
    raw = signed(raw_email(body='word ' * 600_000))
    assert len(raw) > 3_000_000
    r = post_events(cli, [inbound(raw)])
    assert (r.status_code, r.json()) == (200, 'ok\n')
    assert queued.call_args.args[:2] == (raw, REPLY_ADDRESS)


def test_post_over_the_size_limit_is_answered_and_dropped(cli: TestClient, settings, queued, caplog):
    fields = {'mandrill_events': json.dumps([inbound('x' * MAX_INBOUND_POST_BYTES)])}
    r = post_fields(cli, fields)
    assert (r.status_code, r.json()) == (200, 'ok\n')
    queued.assert_not_called()
    assert view_messages(caplog) == ['inbound post not read: it has no length or is over the size limit']


def test_post_without_a_length_is_answered_and_dropped(cli: TestClient, settings, queued, caplog):
    fields = {'mandrill_events': json.dumps([inbound(signed(raw_email()))])}
    body = urlencode(fields).encode()
    r = cli.post(URL, content=iter([body]), headers={**FORM, 'X-Mandrill-Signature': mandrill_signature(fields)})
    assert 'content-length' not in r.request.headers
    assert (r.status_code, r.json()) == (200, 'ok\n')
    queued.assert_not_called()
    assert view_messages(caplog) == ['inbound post not read: it has no length or is over the size limit']


def test_error_while_queueing_is_raised_so_mandrill_sends_the_emails_again(cli: TestClient, settings, queued):
    queued.side_effect = RuntimeError('could not queue')
    with pytest.raises(RuntimeError, match='could not queue'):
        post_events(cli, [inbound(signed(raw_email()))])


def test_webhook_logs_no_address_or_text(cli: TestClient, settings, caplog):
    caplog.set_level(logging.DEBUG, logger='views.webhooks')
    r = post_events(cli, [inbound(signed(raw_email()))])
    assert (r.status_code, r.json()) == (200, 'ok\n')
    logged = '\n'.join(
        f'{record.getMessage()} {record.__dict__}' for record in caplog.records if 'views' in record.name
    )
    assert 'inbound post: 1 of 1 events queued' in logged
    for private in ('reply+', 'reply.example.com', 'client@', 'Tuesday'):
        assert private not in logged, private


def test_signature_is_the_one_the_delivery_webhook_has_always_checked():
    """The delivery-event webhook signs its one field; the helper it now shares must give the same bytes."""
    events = '[{"event": "open"}]'
    old = base64.b64encode(
        hmac.new(
            b'a-key', f'https://localhost/webhook/mandrill/mandrill_events{events}'.encode(), hashlib.sha1
        ).digest()
    )
    assert _mandrill_signature('https://localhost/webhook/mandrill/', 'a-key', {'mandrill_events': events}) == old
