"""
Reading an email received at a chat reply address: what TutorCruncher is told about it. Morpheus says what it
could prove of the email; whether the reply is posted to the chat is TutorCruncher's decision.
"""

import hashlib
import re
from datetime import datetime, timezone
from email.message import EmailMessage

from app.inbound.reply_text import attached_file_names, body_text, latest_reply
from app.inbound.sender_proof import (
    DkimDnsError,
    ProofFailure,
    header_addresses,
    header_failure,
    prove_sender,
    readable_message,
)

AUTO_REPLY_PRECEDENCES = ('bulk', 'list', 'junk', 'auto_reply')
AUTO_REPLY_SENDERS = ('mailer-daemon', 'postmaster', 'no-reply', 'noreply')
AUTO_REPLY_SUBJECT = re.compile(r'^\s*(out of office|automatic reply|auto[- ]?reply)', re.IGNORECASE)
# set by mail systems that answer for somebody who is away, whatever its value
AUTO_REPLY_HEADERS = ('X-Autoreply', 'X-Autorespond')


def is_auto_reply(message: EmailMessage) -> bool:
    """Whether a mail system sent the email with nobody writing it: an out-of-office answer, a bounce, a list."""
    auto_submitted = str(message.get('Auto-Submitted', 'no')).strip().lower()
    precedence = str(message.get('Precedence', '')).strip().lower()
    try:
        from_addresses = header_addresses(message, 'From')
    except ValueError:
        from_addresses = []
    return (
        not auto_submitted.startswith('no')
        or precedence in AUTO_REPLY_PRECEDENCES
        or any(message.get(name) is not None for name in AUTO_REPLY_HEADERS)
        or any(address.rpartition('@')[0] in AUTO_REPLY_SENDERS for address in from_addresses)
        or bool(AUTO_REPLY_SUBJECT.match(str(message.get('Subject', ''))))
    )


def _one_from_address(message: EmailMessage) -> str | None:
    """The email's From address, or None when it has two From headers or its From is not one address."""
    if [name.lower() for name, __ in message.raw_items()].count('from') != 1:
        return None
    try:
        addresses = header_addresses(message, 'From')
    except ValueError:
        return None
    if len(addresses) == 1:
        return addresses[0]
    else:
        return None


def _signed_date(message: EmailMessage) -> str | None:
    """The Date header with its offset, or None when it is absent, unreadable or says no offset."""
    date_header = message['Date']
    if date_header is None or date_header.datetime is None or date_header.datetime.tzinfo is None:
        return None
    else:
        return date_header.datetime.isoformat()


def read_reply(raw: str, reply_address: str, received_at: datetime, *, final_try: bool = False) -> dict | None:
    """
    What TutorCruncher is sent about the email `raw`, which arrived for `reply_address` at `received_at`, or None
    when it is an automatic reply and nothing is sent. The text, subject, date and file names are only in it
    when a signature proved who sent the email, so nothing an unproven sender wrote leaves Morpheus.

    Raises `DkimDnsError` when the sender's signing key could not be looked up, unless `final_try`, when that
    becomes the reason the sender is not proven.
    """
    try:
        raw_message = raw.encode()
        is_text = True
    except UnicodeEncodeError:
        # a string from JSON can hold half a surrogate pair, which no email is made of
        raw_message = raw.encode(errors='replace')
        is_text = False
    reply_address = reply_address.lower()
    payload = {
        'reply_address': reply_address,
        'received_at': received_at.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'idempotency_key': hashlib.sha256(raw_message).hexdigest(),
        'from_address': None,
        'proof': None,
        'proof_failure': None,
        'subject': None,
        'date': None,
        'text': '',
        'attachment_names': [],
    }

    def not_proven(failure: ProofFailure) -> dict:
        return {**payload, 'proof_failure': failure.value}

    message = readable_message(raw_message) if is_text else None
    if message is None:
        return not_proven(ProofFailure.UNREADABLE)
    elif is_auto_reply(message):
        return None

    payload['from_address'] = _one_from_address(message)
    if failure := header_failure(message):
        return not_proven(failure)
    elif reply_address not in header_addresses(message, 'To') + header_addresses(message, 'Cc'):
        # sent to the reply address as a hidden copy, so nothing a signature covers names it
        return not_proven(ProofFailure.NOT_BOUND_TO_ADDRESS)

    try:
        proof = prove_sender(raw_message, message, reply_address=reply_address, received_dt=received_at)
    except DkimDnsError:
        if final_try:
            return not_proven(ProofFailure.DNS_ERROR)
        else:
            raise
    if proof.rule is None:
        return not_proven(proof.failure or ProofFailure.NO_ALIGNED_SIGNATURE)
    else:
        return {
            **payload,
            # the same signed email, under any Message-ID or spelling of its signature, has the same key
            'idempotency_key': proof.signature_hash,
            'proof': proof.rule.value,
            'subject': str(message['Subject'] or ''),
            'date': _signed_date(message),
            'text': latest_reply(body_text(message)),
            'attachment_names': attached_file_names(message),
        }
