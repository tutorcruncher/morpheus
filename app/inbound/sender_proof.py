"""
Whether an email received at a reply address was sent by the person the address belongs to. Everything is read
from the email as it arrived, and the signature is verified here against the key its domain publishes.
"""

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from email import message_from_bytes
from email.message import EmailMessage
from email.policy import EmailPolicy
from email.utils import getaddresses
from enum import Enum

import dkim
import dns.exception
import dns.resolver
from dkim.util import InvalidTagValueList, parse_tag_value

# an email carrying two of any of these can show one value to the person reading it and another to us, or have a
# signature that covers one copy while its text is read with the other
SINGLE_HEADERS = (
    'From',
    'To',
    'Cc',
    'Subject',
    'Date',
    'Message-ID',
    'Reply-To',
    'Sender',
    'Content-Type',
    'Content-Transfer-Encoding',
    'MIME-Version',
)
DNS_TIMEOUT_SECONDS = 5
# each signature checked costs a key lookup and a pass over the whole email
SIGNATURES_VERIFIED = 3
LONE_CARRIAGE_RETURN = re.compile(rb'\r(?!\n)')
MBOX_FROM_LINE = b'From '
LINE_END = re.compile(rb'\r?\n')
HEADERS_END = re.compile(rb'\r?\n\r?\n')
# the headers Python's parser works out the meaning of, at the top of an email, beyond `SINGLE_HEADERS`. The time
# and memory that takes grow with the square of a header's length, so each is held to `HEADER_VALUE_LIMIT_BYTES`.
# Any other header at the top is only ever read as written
PARSED_HEADERS = frozenset(
    name.lower()
    for name in (
        *SINGLE_HEADERS,
        'Auto-Submitted',
        'Precedence',
        'X-Autoreply',
        'X-Autorespond',
        'Content-Disposition',
        'Content-ID',
    )
)
# the headers read on a part of an email, to find the text and the names of the files
PART_PARSED_HEADERS = ('content-type', 'content-disposition', 'content-transfer-encoding', 'content-id')
# From, To and Subject run to a few hundred bytes in real mail
HEADER_VALUE_LIMIT_BYTES = 2048
# The headers of a part are read for every part, by the parser and again by whatever looks for the text and the
# files, so they are held to less. The longest real one is a file's name in another alphabet, encoded: the limit
# is reached by a name of about 90 Chinese or 130 Cyrillic characters, and that email is then unreadable
PART_HEADER_VALUE_LIMIT_BYTES = 1024
# What a header meant is kept while the email is read, and Python holds up to a thousand times a header's size
# for it (measured: 1.2 MB for a 1 KB Content-Type of empty parameters, 335 MB for 98 parts of them). The
# headers read of real mail, a dozen files attached, come to a few kilobytes
PARSED_HEADERS_LIMIT_BYTES = 32 * 1024
# every part of an email has its Content-Type parsed. A part starts at a line beginning "--", or, in a delivery
# report or an email inside an email, wherever a Content-Type header can be read. A reply has a handful of either
PART_LINES_LIMIT = 100
PART_LINE = b'\n--'
CONTENT_TYPE_LINE = re.compile(rb'^content-type:', re.IGNORECASE | re.MULTILINE)
# both parsers hold every line of an email. A file at the size limit of a post from Mandrill is under 200,000
RAW_LINES_LIMIT = 300_000
# dkimpy joins the lines of a folded header one at a time, in time that grows with the square of their number
# (measured on Python 3.12: 21 seconds for one header folded over 290,000 lines, and it reads the email twice).
# The headers of real mail, signatures and delivery trail included, come to a few tens of kilobytes
HEADERS_LIMIT_BYTES = 256 * 1024
# dkimpy rewrites every run of spaces and tabs in the body before it hashes it, and holds about 180 bytes for
# each while it does (measured: 1.4 GB for a 15 MB body of single words). Every space and tab is counted, which
# is never fewer than the runs. A file attached in base64, as mail apps attach them, has none; half a million
# is megabytes of written text, so a very large text file attached as it is makes the email unreadable
WHITESPACE_RUNS_LIMIT = 500_000
# the only signatures RFC 8301 lets a verifier accept: rsa-sha1 is not one
SIGNATURE_ALGORITHMS = (b'rsa-sha256', b'ed25519-sha256')
# dkimpy looks through every header once for each name a signature lists. Real signatures list under 40
SIGNED_HEADER_NAMES_LIMIT = 100
# dkimpy checks a signature in time that grows steeply with the size of the published key, and nothing can stop it
# part way. A 4096-bit key's record is about 750 bytes
DKIM_KEY_RECORD_LIMIT_BYTES = 1500
# how far an email's signed Date may be from when it reached us, under the rule that reads the Date
SIGNED_DATE_WINDOW = timedelta(hours=24)


# dkimpy logs the signature it is checking, the headers it hashed and the domain a key lookup failed for. This
# logger is handed to it in place of its own and its level is above any dkimpy logs at, whatever level the
# application logs at.
DKIM_LOGGER = logging.getLogger('app.inbound.dkim')
DKIM_LOGGER.setLevel(logging.CRITICAL + 1)


class ProofRule(str, Enum):
    """How a signature was tied to the reply address it was sent to."""

    # the signature covers the To or Cc header that names the reply address
    RECIPIENT_SIGNED = 'recipient_signed'
    # the signature covers the Date and Subject, the Date is close to when the email arrived, and nobody else was
    # sent it. Whether they match the email the reply address was sent in is for TutorCruncher to check
    SUBJECT_AND_DATE_SIGNED = 'subject_and_date_signed'


class ProofFailure(str, Enum):
    DUPLICATE_HEADER = 'duplicate_header'
    FROM_NOT_ONE_ADDRESS = 'from_not_one_address'
    SENDER_DIFFERS = 'sender_differs'
    NO_ALIGNED_SIGNATURE = 'no_aligned_signature'
    BODY_LENGTH_LIMITED = 'body_length_limited'
    FROM_NOT_SIGNED = 'from_not_signed'
    SIGNATURE_EXPIRED = 'signature_expired'
    NOT_BOUND_TO_ADDRESS = 'not_bound_to_address'
    SIGNATURE_INVALID = 'signature_invalid'
    UNREADABLE = 'unreadable'
    DNS_ERROR = 'dns_error'


class DkimDnsError(Exception):
    """The lookup of a signing key got no answer, so the signature could be neither verified nor refused."""


@dataclass(frozen=True)
class SenderProof:
    """The outcome of `prove_sender`: the `rule` a signature passed with its `signature_hash`, or the `failure`."""

    rule: ProofRule | None = None
    signature_hash: str | None = None
    failure: ProofFailure | None = None


def headers_of(raw_message: bytes) -> bytes:
    """The email up to the first empty line, which is where its headers end."""
    if headers_end := HEADERS_END.search(raw_message):
        return raw_message[: headers_end.start()]
    else:
        return raw_message


class _BoundedHeaderPolicy(EmailPolicy):
    """
    Python's email policy, refusing to work out the meaning of a header over `HEADER_VALUE_LIMIT_BYTES`, or of one
    of `PART_PARSED_HEADERS` over `PART_HEADER_VALUE_LIMIT_BYTES`. The parser itself reads each part's
    Content-Type to find the parts, so the limit has to hold while it parses.

    Python works a header's meaning out again each time it is read, and a part's headers are read a dozen times
    over. One policy is made for each email (`parsed={}`) and keeps what each header meant, for at most
    `PARSED_HEADERS_LIMIT_BYTES` of headers: past that the email is refused.
    """

    # a policy takes only the attributes its class has
    parsed: dict = {}

    def header_fetch_parse(self, name, value):
        is_part_header = name.lower() in PART_PARSED_HEADERS
        if len(value) > (PART_HEADER_VALUE_LIMIT_BYTES if is_part_header else HEADER_VALUE_LIMIT_BYTES):
            raise ValueError(f'{name} header is over the size limit')
        key = (name.lower(), value)
        if key not in self.parsed:
            if len(value) + sum(len(kept) for __, kept in self.parsed) > PARSED_HEADERS_LIMIT_BYTES:
                raise ValueError('the headers read are over the size limit')
            self.parsed[key] = super().header_fetch_parse(name, value)
        return self.parsed[key]


def _is_cheap_to_parse(raw_message: bytes) -> bool:
    """
    Whether both parsers can read the email in a bounded time and memory: its headers, the number of parts it
    could have and its lines are within their limits.
    """
    return (
        len(headers_of(raw_message)) <= HEADERS_LIMIT_BYTES
        and raw_message.count(b' ') + raw_message.count(b'\t') <= WHITESPACE_RUNS_LIMIT
        and raw_message.count(b'\n') <= RAW_LINES_LIMIT
        and raw_message.count(PART_LINE) <= PART_LINES_LIMIT
        and len(CONTENT_TYPE_LINE.findall(raw_message)) <= PART_LINES_LIMIT
    )


def readable_message(raw_message: bytes) -> EmailMessage | None:
    """
    The email parsed, or None when the two parsers that read it could read different headers. Who the email is from
    and to is read with Python's parser and the signature is checked with dkimpy's, so a signature could otherwise
    verify over headers, or a body, that nothing else read. A lone carriage return ends a line for one parser and
    not the other, and a line starting "From " is skipped by dkimpy and can start the body for Python, so neither is
    allowed, nor is a header line dkimpy cannot read. An email `_is_cheap_to_parse` turns down is
    refused before either parser runs, and `_BoundedHeaderPolicy` refuses a long header when Python reads it with
    `get` or `[...]`. Serialising the message (`as_string`, `as_bytes`) or calling `get_content()` on a part that is
    not text refolds its headers without that policy, so never do either.
    """
    if LONE_CARRIAGE_RETURN.search(raw_message):
        return None
    try:
        if not _is_cheap_to_parse(raw_message):
            return None
        dkim.rfc822_parse(raw_message)
        policy = _BoundedHeaderPolicy(parsed={})  # ty:ignore[no-matching-overload]
        message = message_from_bytes(raw_message, policy=policy)
        # a delivery report's blocks are parts that no line `_is_cheap_to_parse` counts has to start
        if len(list(message.walk())) > PART_LINES_LIMIT:
            return None
        # reading these here means one Python cannot parse is refused now, not raised from a later check
        for name in PARSED_HEADERS:
            message.get(name)
        for part in message.walk():
            for name in PART_PARSED_HEADERS:
                part.get(name)
    except (dkim.DKIMException, IndexError, RecursionError, ValueError, LookupError, AttributeError):
        # AttributeError is Python 3.12's parser on a group inside a group in an address header ("a:b:;;")
        return None
    if any(line.startswith(MBOX_FROM_LINE) for line in LINE_END.split(headers_of(raw_message))):
        return None
    else:
        return message  # ty:ignore[invalid-return-type]


def header_addresses(message: EmailMessage, name: str) -> list[str]:
    """
    The lowercased addresses in an address header, or an empty list when the header is absent. Two parsers read the
    header, and when they disagree about it `ValueError` is raised: a header written to be read two ways is how a
    display name gets passed off as the address.

    The second parser reads the header both as the first wrote it back out and as it arrived. Python 3.12's first
    parser leaves out the part of a header it cannot read ("a@example.org <b@example.org>" is one address to it),
    so what it writes back out agrees with it, and only the header as it arrived shows the difference.
    """
    if (header := message[name]) is None:
        return []
    else:
        addresses = [address.addr_spec.lower() for address in header.addresses]
        as_written = next(value for raw_name, value in message.raw_items() if raw_name.lower() == name.lower())
        for text in (str(header), ''.join(as_written.splitlines())):
            try:
                checked = [address.lower() for __, address in getaddresses([text], strict=True)]
            except RecursionError:
                # groups inside groups ("a:b:c:...") nest the second parser as deep as they go
                raise ValueError(f'{name} header is nested too deeply') from None
            if addresses != checked:
                raise ValueError(f'{name} header reads two ways')
        if not all(_is_address(address) for address in addresses):
            raise ValueError(f'{name} header reads two ways')
        return addresses


def _is_address(address: str) -> bool:
    name, at, domain = address.rpartition('@')
    return bool(name and at and domain)


def header_failure(message: EmailMessage) -> ProofFailure | None:
    """What is wrong with the headers that say who an email is from and to, or None when nothing is."""
    names = [name.lower() for name, __ in message.raw_items()]
    if any(names.count(name.lower()) > 1 for name in SINGLE_HEADERS):
        return ProofFailure.DUPLICATE_HEADER
    try:
        from_addresses = header_addresses(message, 'From')
        sender_addresses = header_addresses(message, 'Sender')
        header_addresses(message, 'To')
        header_addresses(message, 'Cc')
    except ValueError:
        return ProofFailure.FROM_NOT_ONE_ADDRESS
    if len(from_addresses) != 1:
        return ProofFailure.FROM_NOT_ONE_ADDRESS
    elif sender_addresses and sender_addresses != from_addresses:
        return ProofFailure.SENDER_DIFFERS
    else:
        return None


def _dkim_txt(name: bytes, timeout: int) -> bytes | None:
    """
    The TXT record a signing key is published in. None when the domain publishes none under that name, or one over
    `DKIM_KEY_RECORD_LIMIT_BYTES`, which dkimpy then treats as no key. So is a name that cannot be looked up at
    all (an empty or overlong label in the signature's selector): trying again would get the same.
    """
    try:
        answer = dns.resolver.resolve(name.decode(), 'TXT', lifetime=timeout)
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.exception.SyntaxError, dns.exception.FormError):
        return None
    except dns.exception.DNSException as e:
        raise DkimDnsError(type(e).__name__) from None
    record = b''.join(answer[0].strings)
    if len(record) > DKIM_KEY_RECORD_LIMIT_BYTES:
        return None
    else:
        return record


def _binding_rule(
    message: EmailMessage,
    signed_headers: list[str],
    *,
    to_addresses: list[str],
    cc_addresses: list[str],
    reply_address: str,
    received_dt: datetime,
) -> ProofRule | None:
    """
    The rule by which a signature covering `signed_headers` ties the email to `reply_address`, or None. Under
    `SUBJECT_AND_DATE_SIGNED` the Subject and Date are only known to be signed: TutorCruncher compares them with
    the email the reply address was sent in.
    """
    if 'to' in signed_headers or 'cc' in signed_headers:
        signed_recipients = []
        if 'to' in signed_headers:
            signed_recipients += to_addresses
        if 'cc' in signed_headers:
            signed_recipients += cc_addresses
        if reply_address in signed_recipients:
            return ProofRule.RECIPIENT_SIGNED
        else:
            return None
    elif 'date' in signed_headers and 'subject' in signed_headers:
        date_header = message['Date']
        if date_header is None or date_header.datetime is None or date_header.datetime.tzinfo is None:
            return None
        date = date_header.datetime
        if abs(received_dt - date) <= SIGNED_DATE_WINDOW and to_addresses == [reply_address] and not cc_addresses:
            return ProofRule.SUBJECT_AND_DATE_SIGNED
        else:
            return None
    else:
        return None


def _signed_content_hash(message: EmailMessage, signed_headers: list[str], tags: dict[bytes, bytes]) -> str:
    """
    A hash of what a verified signature covers: the body's hash and each header it signs. The signature's own
    text can't identify the email, as the same signature can be written in more than one way and still verify, and
    a domain can sign one email twice.
    """
    values_by_name = defaultdict(list)
    for name, value in message.raw_items():
        values_by_name[name.lower()].append(''.join(value.split()))
    covered = [''.join(tags[b'bh'].decode('ascii', 'surrogateescape').split())]
    for name in signed_headers:
        # a signature covers the last header of a name first, and nothing once it has covered them all
        values = values_by_name[name]
        if values:
            covered.append(f'{name}:{values.pop()}')
        else:
            covered.append(f'{name}:')
    return hashlib.sha256('\n'.join(covered).encode(errors='surrogateescape')).hexdigest()


def prove_sender(
    raw_message: bytes,
    message: EmailMessage,
    *,
    reply_address: str,
    received_dt: datetime,
) -> SenderProof:
    """
    Whether a signature on the email proves its From address sent it to `reply_address`. `message` is `raw_message`
    as `readable_message` parsed it, and has passed `header_failure`.

    Only a signature made by the From address's own domain is looked at, so the only DNS lookups are of that
    domain's keys. It must cover the From header and the whole body, not have expired, be tied to the reply
    address by one of the `ProofRule`s, and verify. At most `SIGNATURES_VERIFIED` of them are verified. Raises
    `DkimDnsError` when a key lookup gets no answer.
    """
    from_domain = header_addresses(message, 'From')[0].rpartition('@')[2]
    reply_address = reply_address.lower()
    to_addresses = header_addresses(message, 'To')
    cc_addresses = header_addresses(message, 'Cc')
    # made when a signature first has to be verified: making it reads the whole email again
    verifier = None
    failure = ProofFailure.NO_ALIGNED_SIGNATURE
    verified = 0
    # read as written, in the order dkimpy numbers them: Python's parser is not needed to read a signature
    signatures = [value for name, value in message.raw_items() if name.lower() == 'dkim-signature']
    for index, signature in enumerate(signatures):
        try:
            tags = parse_tag_value(signature.encode('ascii', 'surrogateescape'))
        except InvalidTagValueList:
            continue
        if tags.get(b'd', b'').decode(errors='replace').strip().lower() != from_domain:
            continue
        signed_headers = [name.strip().lower() for name in tags.get(b'h', b'').decode(errors='replace').split(':')]
        expires = tags.get(b'x', b'').strip()
        if tags.get(b'a', b'').strip().lower() not in SIGNATURE_ALGORITHMS:
            failure = ProofFailure.SIGNATURE_INVALID
        elif b'l' in tags:
            failure = ProofFailure.BODY_LENGTH_LIMITED
        elif 'from' not in signed_headers:
            failure = ProofFailure.FROM_NOT_SIGNED
        elif len(signed_headers) > SIGNED_HEADER_NAMES_LIMIT:
            failure = ProofFailure.SIGNATURE_INVALID
        elif expires.isdigit() and int(expires) < received_dt.timestamp():
            failure = ProofFailure.SIGNATURE_EXPIRED
        elif rule := _binding_rule(
            message,
            signed_headers,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            reply_address=reply_address,
            received_dt=received_dt,
        ):
            if verified == SIGNATURES_VERIFIED:
                break
            verified += 1
            if verifier is None:
                verifier = dkim.DKIM(raw_message, logger=DKIM_LOGGER, timeout=DNS_TIMEOUT_SECONDS)
            try:
                is_verified = verifier.verify(idx=index, dnsfunc=_dkim_txt)
            except dkim.DKIMException:
                is_verified = False
            if is_verified:
                return SenderProof(rule=rule, signature_hash=_signed_content_hash(message, signed_headers, tags))
            else:
                failure = ProofFailure.SIGNATURE_INVALID
        else:
            failure = ProofFailure.NOT_BOUND_TO_ADDRESS
    return SenderProof(failure=failure)
