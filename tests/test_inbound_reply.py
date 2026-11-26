import base64
import hashlib
import logging
import string
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime
from unittest.mock import Mock

import dkim
import dns.exception
import dns.resolver
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

from app.inbound.reply import read_reply
from app.inbound.reply_text import body_text, latest_reply
from app.inbound.sender_proof import (
    DKIM_KEY_RECORD_LIMIT_BYTES,
    HEADER_VALUE_LIMIT_BYTES,
    PART_LINES_LIMIT,
    RAW_LINES_LIMIT,
    SIGNED_HEADER_NAMES_LIMIT,
    DkimDnsError,
    readable_message,
)

REPLY_ADDRESS = 'reply+abcdefghijklmnopqrstuvwxyz@reply.example.com'
SENT_SUBJECT = 'You have unread messages in Maths with Sam'

# made for this test run and never saved: no signing key is committed
SIGNING_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SIGNING_KEY_PEM = SIGNING_KEY.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption())
PUBLISHED_KEY = b'v=DKIM1; k=rsa; p=' + base64.b64encode(
    SIGNING_KEY.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
)
# what a signature that names the recipient covers, and what one that leaves the recipient out covers
RECIPIENT_SIGNED = ('from', 'to', 'cc', 'subject', 'date')
RECIPIENT_UNSIGNED = ('from', 'subject', 'date')


def now() -> datetime:
    return datetime.now(timezone.utc)


def signed(raw: bytes, headers: tuple[str, ...] = RECIPIENT_SIGNED, domain: str = 'example.org', **kwargs) -> str:
    """The email with a DKIM signature by `domain` over `headers` and the body put in front of it."""
    signature = dkim.sign(
        raw, b'sel', domain.encode(), SIGNING_KEY_PEM, include_headers=[h.encode() for h in headers], **kwargs
    )
    return (signature + raw).decode()


def email_headers(**changes: str) -> list[tuple[str, str]]:
    headers = {
        'From': 'Carla Client <client@example.org>',
        'To': f'Acme Tutors <{REPLY_ADDRESS}>',
        'Subject': f'Re: {SENT_SUBJECT}',
        'Date': format_datetime(now() - timedelta(minutes=5)),
        'Message-ID': '<reply-1@mail.example.org>',
    }
    headers.update({name.replace('_', '-'): value for name, value in changes.items()})
    return list(headers.items())


def raw_email(body: str = 'Tuesday works, thanks.', headers: list[tuple[str, str]] | None = None) -> bytes:
    lines = [f'{name}: {value}' for name, value in headers or email_headers()]
    lines += ['MIME-Version: 1.0', 'Content-Type: text/plain; charset="utf-8"', '', body, '']
    return '\r\n'.join(lines).encode()


def built_email(**changes: str) -> EmailMessage:
    email = EmailMessage()
    for name, value in email_headers(**changes):
        email[name] = value
    return email


def read(raw: str, **kwargs) -> dict | None:
    return read_reply(raw, REPLY_ADDRESS, now(), **kwargs)


def assert_not_proven(payload: dict | None, failure: str, from_address: str | None = 'client@example.org') -> None:
    """The payload says why the sender is not proven, and holds nothing the email said."""
    assert payload is not None
    assert payload['proof'] is None
    assert payload['proof_failure'] == failure
    assert payload['from_address'] == from_address
    assert (payload['text'], payload['subject'], payload['date'], payload['attachment_names']) == ('', None, None, [])


@pytest.fixture
def resolve(monkeypatch):
    """The DNS lookup of a signing key, answered with the key the tests sign with."""
    mock_resolve = Mock(return_value=[Mock(strings=[PUBLISHED_KEY])])
    monkeypatch.setattr('app.inbound.sender_proof.dns.resolver.resolve', mock_resolve)
    return mock_resolve


@pytest.fixture(autouse=True)
def _resolve(resolve):
    """No test in this file looks anything up for real."""


# The whole payload ----------------------------------------------------------------------------------------------


def test_recipient_signed_reply_is_proven():
    received_at = datetime(2026, 11, 26, 11, 20, 3, 500, tzinfo=timezone(timedelta(hours=1)))
    date = received_at - timedelta(minutes=5)
    raw = signed(raw_email(headers=email_headers(Date=format_datetime(date))))
    payload = read_reply(raw, REPLY_ADDRESS.upper(), received_at)
    assert payload is not None
    key = payload.pop('idempotency_key')
    assert len(key) == 64 and set(key) <= set(string.hexdigits.lower())
    assert key != hashlib.sha256(raw.encode()).hexdigest()
    assert payload == {
        'reply_address': REPLY_ADDRESS,
        'received_at': '2026-11-26T10:20:03Z',
        'from_address': 'client@example.org',
        'proof': 'recipient_signed',
        'proof_failure': None,
        'subject': f'Re: {SENT_SUBJECT}',
        'date': '2026-11-26T11:15:03+01:00',
        'text': 'Tuesday works, thanks.',
        'attachment_names': [],
    }


def test_unsigned_recipient_is_proven_by_the_signed_subject_and_date():
    payload = read(signed(raw_email(), RECIPIENT_UNSIGNED))
    assert (payload['proof'], payload['proof_failure']) == ('subject_and_date_signed', None)
    assert payload['subject'] == f'Re: {SENT_SUBJECT}'
    assert datetime.fromisoformat(payload['date']).tzinfo is not None
    assert payload['text'] == 'Tuesday works, thanks.'


def test_unsigned_recipient_is_proven_whatever_the_subject_and_however_old_our_email():
    """TutorCruncher compares the subject and date with the email it sent; Morpheus has neither."""
    headers = email_headers(Subject='Something else', Date=format_datetime(now() - timedelta(hours=23)))
    payload = read(signed(raw_email(headers=headers), RECIPIENT_UNSIGNED))
    assert (payload['proof'], payload['subject']) == ('subject_and_date_signed', 'Something else')


def test_unproven_email_is_told_apart_by_its_content(resolve):
    raw = raw_email().decode()
    payload = read(raw)
    assert payload == {
        'reply_address': REPLY_ADDRESS,
        'received_at': payload['received_at'],
        'idempotency_key': hashlib.sha256(raw.encode()).hexdigest(),
        'from_address': 'client@example.org',
        'proof': None,
        'proof_failure': 'no_aligned_signature',
        'subject': None,
        'date': None,
        'text': '',
        'attachment_names': [],
    }
    resolve.assert_not_called()


def test_email_that_is_not_text_is_unreadable():
    """Half a surrogate pair, which JSON can carry, cannot be encoded."""
    payload = read('\ud800' + raw_email().decode())
    assert_not_proven(payload, 'unreadable', from_address=None)
    assert len(payload['idempotency_key']) == 64


# Automatic replies ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    'changes',
    [
        {'Auto_Submitted': 'auto-replied'},
        {'Precedence': 'bulk'},
        {'Subject': 'Automatic reply: You have unread messages'},
        {'Subject': 'Out of Office'},
        {'From': 'MAILER-DAEMON@example.org'},
        {'X_Autoreply': 'yes'},
        {'X_Autorespond': ''},
    ],
)
def test_automatic_answer_is_dropped(changes, resolve):
    assert read(signed(raw_email(headers=email_headers(**changes)))) is None
    resolve.assert_not_called()


def test_from_that_is_not_an_address_is_not_an_automatic_answer():
    payload = read(signed(raw_email(headers=email_headers(From='not an address'))))
    assert_not_proven(payload, 'from_not_one_address', from_address=None)


def test_reply_marked_as_written_by_a_person_is_read():
    payload = read(signed(raw_email(headers=email_headers(Auto_Submitted='no'))))
    assert payload['proof'] == 'recipient_signed'


# Refusals -------------------------------------------------------------------------------------------------------


def test_unsigned_email_is_not_proven():
    assert_not_proven(read(raw_email().decode()), 'no_aligned_signature')


def test_signature_by_another_domain_is_not_proven(resolve):
    assert_not_proven(read(signed(raw_email(), domain='evil.example')), 'no_aligned_signature')
    resolve.assert_not_called()


def test_signature_by_a_subdomain_is_not_proven():
    assert_not_proven(read(signed(raw_email(), domain='mail.example.org')), 'no_aligned_signature')


def test_address_as_a_display_name_is_not_the_from_address():
    headers = email_headers(From='"client@example.org" <attacker@evil.example>')
    payload = read(signed(raw_email(headers=headers), domain='evil.example'))
    assert (payload['proof'], payload['from_address']) == ('recipient_signed', 'attacker@evil.example')


def test_from_header_that_reads_two_ways_is_not_proven():
    headers = email_headers(From='client@example.org <attacker@evil.example>')
    payload = read(signed(raw_email(headers=headers), domain='evil.example'))
    assert_not_proven(payload, 'from_not_one_address', from_address=None)


@pytest.mark.parametrize(
    'from_header',
    [
        'Dr. C. Client <client@example.org>',
        '"Client, Carla" <client@example.org>',
        '=?utf-8?q?Client=2C_Carla?= <client@example.org>',
        '=?utf-8?b?Q2FybGEgQ2xpw6ludA==?=\r\n <client@example.org>',
        'client@example.org (Carla)',
        '<CLIENT@example.org>',
    ],
)
def test_from_headers_mail_apps_write_read_one_way(from_header):
    payload = read(signed(raw_email(headers=email_headers(From=from_header))))
    assert (payload['proof'], payload['from_address']) == ('recipient_signed', 'client@example.org')


def test_two_from_addresses_are_not_proven():
    headers = email_headers(From='client@example.org, attacker@evil.example')
    assert_not_proven(read(signed(raw_email(headers=headers))), 'from_not_one_address', from_address=None)


def test_sender_that_differs_from_from_is_not_proven():
    payload = read(signed(raw_email(headers=email_headers(Sender='attacker@example.org'))))
    assert_not_proven(payload, 'sender_differs')


def test_sender_that_matches_from_is_proven():
    payload = read(signed(raw_email(headers=email_headers(Sender='client@example.org'))))
    assert payload['proof'] == 'recipient_signed'


def test_each_header_given_twice_is_not_proven():
    values = dict(email_headers(Cc='someone@example.org', Reply_To='client@example.org', Sender='client@example.org'))
    for name, value in values.items():
        payload = read(signed(raw_email(headers=[*values.items(), (name, value)])))
        # with two From headers, which one a reader is shown is not known
        from_address = None if name == 'From' else 'client@example.org'
        assert_not_proven(payload, 'duplicate_header', from_address=from_address)


def test_second_header_that_says_how_to_read_the_text_is_not_proven(resolve):
    raw = signed(raw_email(headers=[*email_headers(), ('Content-Transfer-Encoding', '7bit')]))
    for header in ('Content-Type: text/plain', 'Content-Transfer-Encoding: base64', 'MIME-Version: 1.0'):
        assert_not_proven(read(f'{header}\r\n{raw}'), 'duplicate_header')
    resolve.assert_not_called()


def _hidden_below_a_carriage_return(signed_headers: tuple[str, ...], shown: list[tuple[str, str]]) -> str:
    """
    An email the client sent somebody else, signed over `signed_headers`, with the `shown` headers and other
    words put above a line ending in a lone carriage return and its own headers left below it.
    """
    original = raw_email(body='See you Thursday.', headers=email_headers(To='somebody@example.net'))
    signature = dkim.sign(
        original, b'sel', b'example.org', SIGNING_KEY_PEM, include_headers=[h.encode() for h in signed_headers]
    )
    headers, __, body = original.partition(b'\r\n\r\n')
    from_line, __, other_lines = headers.partition(b'\r\n')
    shown_lines = ''.join(f'{name}: {value}\r\n' for name, value in shown).encode()
    return (
        signature
        + from_line
        + b'\r\n'
        + shown_lines
        + b'X-Note: x\r\r\n Please pay the new invoice to account 12345678.\r\n'
        + other_lines
        + b'\r\n\r\n'
        + body
    ).decode()


def test_signed_recipient_hidden_below_a_lone_carriage_return_is_unreadable(resolve):
    raw = _hidden_below_a_carriage_return(RECIPIENT_SIGNED, [('To', REPLY_ADDRESS)])
    assert_not_proven(read(raw), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_signed_subject_and_date_hidden_below_a_lone_carriage_return_are_unreadable():
    shown = [
        ('To', REPLY_ADDRESS),
        ('Subject', f'Re: {SENT_SUBJECT}'),
        ('Date', format_datetime(now() - timedelta(minutes=1))),
    ]
    raw = _hidden_below_a_carriage_return(RECIPIENT_UNSIGNED, shown)
    assert_not_proven(read(raw), 'unreadable', from_address=None)


def test_header_line_the_signature_check_cannot_read_is_unreadable():
    headers, __, body = signed(raw_email()).partition('\r\n\r\n')
    assert_not_proven(read(f'{headers}\r\nnot a header line\r\n\r\n{body}'), 'unreadable', from_address=None)


def test_text_added_on_a_line_only_one_parser_reads_as_a_header_is_unreadable(resolve):
    """The mbox `From ` line: dkimpy skips it, and Python can start the body at it."""
    headers, __, body = signed(raw_email()).partition('\r\n\r\n')
    raw = f'{headers}\r\nFrom now on pay invoices to account 12345678.\r\n\r\n{body}'
    assert_not_proven(read(raw), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_line_only_one_parser_reads_inside_a_folded_header_is_unreadable(resolve):
    signature, __, rest = signed(raw_email()).partition('\r\n ')
    assert_not_proven(read(f'{signature}\r\nFrom x\r\n {rest}'), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_email_that_starts_with_a_continued_line_is_unreadable():
    assert_not_proven(read((b' continued\r\n' + raw_email()).decode()), 'unreadable', from_address=None)


def test_message_id_too_deeply_nested_to_read_is_unreadable():
    nested = raw_email(headers=email_headers(Message_ID=f'{"(" * 5000}<nested@mail.example.org>'))
    assert_not_proven(read(signed(nested)), 'unreadable', from_address=None)


def test_email_that_is_only_headers_is_not_proven():
    assert_not_proven(read(signed(raw_email()).partition('\r\n\r\n')[0]), 'signature_invalid')


def test_only_the_first_three_signatures_are_checked(resolve):
    raw = raw_email()
    wrong_key = b'v=DKIM1; k=rsa; p=' + base64.b64encode(
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .public_key()
        .public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    )
    signature = dkim.sign(
        raw, b'sel', b'example.org', SIGNING_KEY_PEM, include_headers=[h.encode() for h in RECIPIENT_SIGNED]
    )
    resolve.return_value = [Mock(strings=[wrong_key])]
    assert_not_proven(read((signature * 5 + raw).decode()), 'signature_invalid')
    assert resolve.call_count == 3


def test_reply_address_in_signed_cc_is_proven():
    headers = email_headers(To='someone@example.org', Cc=REPLY_ADDRESS.upper())
    assert read(signed(raw_email(headers=headers)))['proof'] == 'recipient_signed'


def test_reply_address_in_cc_when_only_cc_is_signed_is_proven():
    headers = email_headers(To='someone@example.org', Cc=REPLY_ADDRESS)
    payload = read(signed(raw_email(headers=headers), headers=('from', 'cc', 'subject', 'date')))
    assert payload['proof'] == 'recipient_signed'


def test_another_reply_address_in_the_signed_recipients_is_not_proven():
    headers = email_headers(To='reply+zyxwvutsrqponmlkjihgfedcba@reply.example.com')
    assert_not_proven(read(signed(raw_email(headers=headers))), 'not_bound_to_address')


def test_reply_address_in_an_unsigned_header_beside_a_signed_one_is_not_proven():
    headers = email_headers(To='someone@example.org', Cc=REPLY_ADDRESS)
    raw = signed(raw_email(headers=headers), headers=('from', 'to', 'subject', 'date'))
    assert_not_proven(read(raw), 'not_bound_to_address')


def test_reply_address_only_as_a_hidden_copy_is_not_proven(resolve):
    payload = read(signed(raw_email(headers=email_headers(To='someone@example.org'))))
    assert_not_proven(payload, 'not_bound_to_address')
    resolve.assert_not_called()


def test_unsigned_recipient_dated_a_little_after_it_arrived_is_proven():
    headers = email_headers(Date=format_datetime(now() + timedelta(minutes=10)))
    assert read(signed(raw_email(headers=headers), RECIPIENT_UNSIGNED))['proof'] == 'subject_and_date_signed'


def test_unsigned_recipient_dated_over_a_day_before_it_arrived_is_not_proven():
    headers = email_headers(Date=format_datetime(now() - timedelta(hours=25)))
    assert_not_proven(read(signed(raw_email(headers=headers), RECIPIENT_UNSIGNED)), 'not_bound_to_address')


@pytest.mark.parametrize('date', ['never', 'Mon, 16 Nov 2026 10:00:00 -0000'])
def test_unsigned_recipient_with_a_date_that_cannot_be_read_is_not_proven(date):
    payload = read(signed(raw_email(headers=email_headers(Date=date)), RECIPIENT_UNSIGNED))
    assert_not_proven(payload, 'not_bound_to_address')


def test_signed_recipient_with_a_date_that_cannot_be_read_is_proven_without_a_date():
    payload = read(signed(raw_email(headers=email_headers(Date='never'))))
    assert (payload['proof'], payload['date']) == ('recipient_signed', None)


@pytest.mark.parametrize('changes', [{'To': f'{REPLY_ADDRESS}, someone@example.org'}, {'Cc': 'someone@example.org'}])
def test_unsigned_recipient_sent_to_somebody_else_as_well_is_not_proven(changes):
    payload = read(signed(raw_email(headers=email_headers(**changes)), RECIPIENT_UNSIGNED))
    assert_not_proven(payload, 'not_bound_to_address')


def test_signature_covering_neither_recipient_nor_subject_and_date_is_not_proven():
    assert_not_proven(read(signed(raw_email(), headers=('from', 'subject'))), 'not_bound_to_address')


def test_body_changed_after_signing_is_not_proven():
    raw = signed(raw_email()).replace('Tuesday works', 'Tuesday worked')
    assert_not_proven(read(raw), 'signature_invalid')


def test_signature_over_part_of_the_body_is_not_proven():
    assert_not_proven(read(signed(raw_email(), length=True)), 'body_length_limited')


def test_signature_that_leaves_from_out_is_not_proven(resolve):
    signature = 'DKIM-Signature: v=1; a=rsa-sha256; d=example.org; s=sel; h=to:subject:date; bh=aaaa; b=bbbb\r\n'
    assert_not_proven(read(signature + raw_email().decode()), 'from_not_signed')
    resolve.assert_not_called()


def test_expired_signature_is_not_proven(resolve):
    signature = 'DKIM-Signature: v=1; a=rsa-sha256; d=example.org; s=sel; h=from:to; x=1000; bh=aaaa; b=bbbb\r\n'
    assert_not_proven(read(signature + raw_email().decode()), 'signature_expired')
    resolve.assert_not_called()


def test_signature_that_cannot_be_read_is_passed_over():
    raw = 'DKIM-Signature: not a signature\r\n' + raw_email().decode()
    assert_not_proven(read(raw), 'no_aligned_signature')


def test_second_signature_is_used_when_the_first_is_another_domains():
    raw = raw_email()
    mailing_list_signature = dkim.sign(raw, b'sel', b'list.example', SIGNING_KEY_PEM).decode()
    assert read(mailing_list_signature + signed(raw))['proof'] == 'recipient_signed'


def test_long_signature_from_another_domain_is_passed_over():
    other = f'DKIM-Signature: v=1; d=other.example; s=sel; h=from; bh=x; b={"=?utf-8?q?a?= " * 2000}\r\n'
    assert read(other + signed(raw_email()))['proof'] == 'recipient_signed'


# The signing key ------------------------------------------------------------------------------------------------


def test_domain_that_publishes_no_key_is_not_proven(resolve):
    resolve.side_effect = dns.resolver.NXDOMAIN
    assert_not_proven(read(signed(raw_email())), 'signature_invalid')
    assert resolve.call_args.args == ('sel._domainkey.example.org.', 'TXT')
    assert resolve.call_args.kwargs == {'lifetime': 5}


def test_published_key_over_the_size_limit_is_not_used(resolve):
    """The key itself is the right one: only the notes after it take the record over the limit."""
    notes = b'; n=' + b'a' * DKIM_KEY_RECORD_LIMIT_BYTES
    resolve.return_value = [Mock(strings=[PUBLISHED_KEY, notes])]
    assert_not_proven(read(signed(raw_email())), 'signature_invalid')


def test_key_lookup_with_no_answer_is_raised_to_be_tried_again(resolve):
    resolve.side_effect = dns.exception.Timeout
    with pytest.raises(DkimDnsError) as exc_info:
        read(signed(raw_email()))
    # the name of the error only: the lookup's own message names the domain
    assert str(exc_info.value) == 'Timeout'


def test_key_lookup_with_no_answer_on_the_final_try_is_not_proven(resolve):
    resolve.side_effect = dns.exception.Timeout
    assert_not_proven(read(signed(raw_email()), final_try=True), 'dns_error')


# Size limits ----------------------------------------------------------------------------------------------------


def test_header_python_reads_over_the_size_limit_is_unreadable(resolve):
    headers = email_headers(Subject=f'Re: {SENT_SUBJECT} {"(a)" * (HEADER_VALUE_LIMIT_BYTES // 3)}')
    assert_not_proven(read(signed(raw_email(headers=headers))), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_long_header_python_does_not_read_is_allowed():
    headers = [*email_headers(), ('X-Long', '(a)' * HEADER_VALUE_LIMIT_BYTES)]
    assert read(signed(raw_email(headers=headers)))['proof'] == 'recipient_signed'


def _with_a_file() -> bytes:
    email = built_email()
    email.set_content('Homework attached.')
    email.add_attachment(b'%PDF-1.4', maintype='application', subtype='pdf', filename='work.pdf')
    return email.as_bytes(policy=SMTP)


def test_long_header_on_a_part_of_the_email_is_unreadable(resolve):
    raw = _with_a_file().replace(
        b'Content-Type: application/pdf', b'Content-Type: application/pdf ' + b'(a)' * HEADER_VALUE_LIMIT_BYTES
    )
    assert b'(a)(a)' in raw
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_header_on_a_part_that_python_cannot_read_is_unreadable():
    raw = _with_a_file().replace(
        b'Content-Disposition: attachment', b'Content-Disposition: attachment ' + b'(' * 500 + b')' * 500
    )
    assert b'((' in raw
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)


def test_long_header_on_a_part_only_one_of_pythons_parsers_finds_is_unreadable(resolve):
    """A comment in the boundary is part of it for the parser that reads headers as written, and not for ours."""
    part = f'--abc\r\nContent-Type: text/plain {"(c)" * 10_000}\r\n\r\nTuesday works.\r\n--abc--'
    raw = raw_email(body=part).replace(
        b'Content-Type: text/plain; charset="utf-8"', b'Content-Type: multipart/mixed; boundary=(x)abc'
    )
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_email_in_more_parts_than_the_limit_is_unreadable(resolve):
    body = 'Tuesday works.\r\n' + '--part\r\n' * (PART_LINES_LIMIT + 1)
    assert_not_proven(read(signed(raw_email(body=body))), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_lines_that_could_start_a_part_up_to_the_limit_are_read():
    body = 'Tuesday works.\r\n' + '--\r\n' * PART_LINES_LIMIT
    assert read(signed(raw_email(body=body)))['proof'] == 'recipient_signed'


def test_lines_that_could_say_what_a_part_is_up_to_the_limit_are_read():
    body = 'Tuesday works.\r\n' + 'Content-Type: text/plain\r\n' * (PART_LINES_LIMIT - 1)
    assert read(signed(raw_email(body=body)))['proof'] == 'recipient_signed'


def test_email_of_more_lines_than_the_limit_is_unreadable(resolve):
    body = 'Tuesday works.' + '\r\n' * (RAW_LINES_LIMIT + 1)
    assert_not_proven(read(signed(raw_email(body=body))), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_emails_nested_too_deeply_to_read_are_unreadable():
    raw = (
        raw_email()
        .decode()
        .replace('Content-Type: text/plain; charset="utf-8"', 'Content-Type: message/rfc822\r\n\r\n' * 1500)
    )
    assert_not_proven(read(signed(raw.encode())), 'unreadable', from_address=None)


def test_more_emails_inside_each_other_than_the_limit_are_unreadable(resolve):
    nested = b'Content-Type: message/rfc822\r\n\r\n' * (PART_LINES_LIMIT + 1)
    raw = raw_email().replace(b'Content-Type: text/plain; charset="utf-8"', nested)
    assert raw.count(b'message/rfc822') == PART_LINES_LIMIT + 1
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_delivery_report_in_more_parts_than_the_limit_is_unreadable(resolve):
    """Each block of a delivery report is a part, and none starts at a "--" line."""
    body = 'Content-Type: text/plain\r\n\r\n' * PART_LINES_LIMIT
    raw = raw_email(body=body).replace(
        b'Content-Type: text/plain; charset="utf-8"', b'Content-Type: message/delivery-status'
    )
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_delivery_report_in_more_parts_that_do_not_say_what_they_are_is_unreadable(resolve):
    body = 'Status: 5.0.0\r\n\r\n' * (PART_LINES_LIMIT + 1)
    raw = raw_email(body=body).replace(
        b'Content-Type: text/plain; charset="utf-8"', b'Content-Type: message/delivery-status'
    )
    assert_not_proven(read(signed(raw)), 'unreadable', from_address=None)
    resolve.assert_not_called()


def test_signature_over_more_header_names_than_the_limit_is_not_proven(resolve):
    names = RECIPIENT_SIGNED + tuple(f'x-{index}' for index in range(SIGNED_HEADER_NAMES_LIMIT))
    assert_not_proven(read(signed(raw_email(), headers=names)), 'signature_invalid')
    resolve.assert_not_called()


def test_signature_over_as_many_header_names_as_the_limit_is_checked():
    extra = SIGNED_HEADER_NAMES_LIMIT - len(RECIPIENT_SIGNED)
    names = RECIPIENT_SIGNED + tuple(f'x-{index}' for index in range(extra))
    assert read(signed(raw_email(), headers=names))['proof'] == 'recipient_signed'


# The idempotency key --------------------------------------------------------------------------------------------


def test_signed_email_with_its_signature_spelt_differently_has_the_same_key():
    """The last character of a signature has four bits nothing reads, so it can be changed and still verify."""
    raw = signed(raw_email())
    signature, ending, rest = raw.partition('==\r\n')
    alphabet = string.ascii_uppercase + string.ascii_lowercase + string.digits + '+/'
    respelt = alphabet[alphabet.index(signature[-1]) + 1]
    sent_again = f'{signature[:-1]}{respelt}{ending}{rest}'.replace('reply-1@', 'reply-2@')
    assert sent_again != raw
    first, second = read(raw), read(sent_again)
    assert (first['proof'], second['proof']) == ('recipient_signed', 'recipient_signed')
    assert first['idempotency_key'] == second['idempotency_key']


def test_same_words_in_a_later_email_have_another_key():
    first = read(signed(raw_email()))
    headers = email_headers(Date=format_datetime(now() - timedelta(minutes=1)), Message_ID='<reply-2@mail.example.org>')
    second = read(signed(raw_email(headers=headers)))
    assert (first['text'], first['proof']) == (second['text'], second['proof'])
    assert first['idempotency_key'] != second['idempotency_key']


# The text and the files -----------------------------------------------------------------------------------------


def test_quoted_email_is_cut_and_the_signature_stays():
    body = (
        'Tuesday works.\r\n\r\nSent from my iPhone\r\n\r\n'
        'On Mon, 16 Nov 2026 at 10:00, Acme Tutors <x@example.com> wrote:\r\n> Can you do Tuesday?\r\n'
    )
    assert read(signed(raw_email(body=body)))['text'] == 'Tuesday works.\n\nSent from my iPhone'


def test_quoted_email_from_a_mail_app_in_another_language_is_cut():
    body = (
        'Dienstag passt.\r\n\r\n'
        'Am Mo., 16. Nov. 2026 um 10:00 Uhr schrieb Acme Tutors <reply@reply.example.com>:\r\n'
        '> Can you do Tuesday?\r\n'
    )
    assert read(signed(raw_email(body=body)))['text'] == 'Dienstag passt.'


def test_html_only_email_gives_what_it_shows():
    email = built_email()
    email.set_content(
        '<html><head><title>Reply</title><style>p {color: red}</style></head><body>'
        '<p style="color: red">Tuesday <b>works</b>.</p><div style="display: none">tracking text</div>'
        '<span hidden>hidden text</span><script>alert(1)</script><div>See you<br>then.</div>'
        '</body></html>',
        subtype='html',
    )
    assert read(signed(email.as_bytes(policy=SMTP)))['text'] == 'Tuesday works.\nSee you\nthen.'


def test_html_hidden_inside_hidden_html_is_left_out():
    html = '<div style="display:none"><span style="display:none">preview</span></div><p>Tuesday works.</p>'
    raw = raw_email(body=html).replace(b'text/plain', b'text/html')
    assert read(signed(raw))['text'] == 'Tuesday works.'


@pytest.mark.parametrize('html', ['https://example.com/notes', 'notes.txt'])
def test_html_that_is_only_a_link_or_a_file_name_is_read(html):
    raw = raw_email(body=html).replace(b'text/plain', b'text/html')
    assert read(signed(raw))['text'] == html


def test_checking_a_signature_logs_nothing(caplog):
    caplog.set_level(logging.DEBUG)
    assert read(signed(raw_email()))['proof'] == 'recipient_signed'
    assert [record.name for record in caplog.records if 'dkim' in record.name] == []
    assert 'example.org' not in caplog.text


def test_text_in_a_character_set_python_does_not_know_is_read():
    raw = raw_email().replace(b'charset="utf-8"', b'charset="unknown-8bit"')
    assert read(signed(raw))['text'] == 'Tuesday works, thanks.'


def test_nul_is_taken_out_of_the_text():
    assert read(signed(raw_email(body='Tuesday\x00 works.')))['text'] == 'Tuesday works.'


def test_many_blank_lines_are_read_as_one():
    raw = raw_email(body='Tuesday works.' + '\r\n' * 5000 + 'Thanks.')
    assert read(signed(raw))['text'] == 'Tuesday works.\n\nThanks.'


def test_text_past_the_parse_limit_is_not_read():
    raw = raw_email(body='Tuesday works.\r\n' + 'x' * 8_000 + '\r\nlast words')
    text = read(signed(raw))['text']
    assert (len(text), 'last words' in text) == (7_999, False)
    assert text == latest_reply(body_text(readable_message(raw)))


def test_email_with_neither_text_nor_html_has_no_text():
    email = built_email()
    email.set_content(b'%PDF-1.4', maintype='application', subtype='pdf')
    payload = read(signed(email.as_bytes(policy=SMTP)))
    assert (payload['proof'], payload['text']) == ('recipient_signed', '')


def test_attached_files_are_named_and_a_picture_shown_in_the_email_is_not():
    email = built_email()
    email.set_content('Homework attached.')
    email.add_attachment(b'%PDF-1.4', maintype='application', subtype='pdf', filename='Home \t work.pdf')
    email.add_attachment(b'GIF89a', maintype='image', subtype='gif', disposition='inline', cid='<logo>')
    email.add_attachment(b'%PDF-1.4', maintype='application', subtype='pdf', disposition='inline', filename='b.pdf')
    email.add_attachment(b'data', maintype='application', subtype='octet-stream')
    payload = read(signed(email.as_bytes(policy=SMTP)))
    assert payload['text'] == 'Homework attached.'
    assert payload['attachment_names'] == ['Home \t work.pdf', 'b.pdf', 'file']


def test_named_picture_shown_in_the_email_by_its_content_id_is_not_a_file():
    email = built_email()
    email.set_content('With my signature.')
    email.add_attachment(
        b'GIF89a', maintype='image', subtype='gif', disposition='inline', filename='image001.gif', cid='<logo>'
    )
    payload = read(signed(email.as_bytes(policy=SMTP)))
    assert (payload['text'], payload['attachment_names']) == ('With my signature.', [])
