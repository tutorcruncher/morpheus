import base64
import hashlib
import hmac
import json
import logging
import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl

from fastapi import APIRouter, Form, Header, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from app.common.api.errors import HTTP400, HTTP403, HTTP422
from app.core.config import settings
from app.messages.api.common import index
from app.messages.models import SendMethod
from app.messages.schemas import MandrillSingleWebhook, MessageBirdWebHook
from app.messages.tasks import read_inbound_reply, update_mandrill_webhooks, update_message_status

router = APIRouter()
logger = logging.getLogger('views.webhooks')

MANDRILL_EVENTS_FIELD = 'mandrill_events'
INBOUND_EVENT = 'inbound'
# Mandrill takes emails of up to 25 MB, and every email it posts here is held in Redis until a worker reads it.
# A reply is a few kilobytes; one over this is not read.
MAX_INBOUND_POST_BYTES = 15 * 1024 * 1024
# Mandrill posts one field. A body of millions of empty fields costs seconds and hundreds of megabytes to read
# into a form, and anybody can post one: the signature can only be checked once the fields are known
MAX_INBOUND_POST_FIELDS = 10
# an address a chat email gave out to reply to: a mailbox name, then a plus tag of 26 base32 characters. Only
# TutorCruncher knows whose it is; an email for any other address is not worth a worker's time
CHAT_REPLY_ADDRESS = re.compile(r'^[^+@\s]+\+[a-z2-7]{26}@', re.IGNORECASE)


def _mandrill_signature(url: str, key: str, fields: dict[str, str]) -> bytes:
    """Mandrill's signature of a POST: the webhook's URL, then each field's name and value in name order."""
    signature = hmac.new(key.encode(), url.encode(), hashlib.sha1)
    for name in sorted(fields):
        signature.update(name.encode())
        signature.update(fields[name].encode())
    return base64.b64encode(signature.digest())


@router.post('/test/')
def test_webhook_view(m: MandrillSingleWebhook):
    """Update messages faux-sent with email-test."""
    update_message_status.delay(SendMethod.email_test.value, m.model_dump(mode='json', by_alias=True))
    return 'message status updated\n'


@router.head('/mandrill/')
async def mandrill_head_view(request: Request):
    return await index(request)


@router.post('/mandrill/')
def mandrill_webhook_view(
    mandrill_events=Form(None),
    X_Mandrill_Signature: bytes = Header(None),
):
    try:
        events = json.loads(mandrill_events)
    except (ValueError, TypeError):
        raise HTTP400('Invalid data')

    sig_generated = _mandrill_signature(
        settings.mandrill_webhook_url, settings.mandrill_webhook_key, {MANDRILL_EVENTS_FIELD: mandrill_events}
    )
    if not hmac.compare_digest(sig_generated, X_Mandrill_Signature or b''):
        raise HTTP403('invalid signature')

    update_mandrill_webhooks.delay(events)
    return 'message status updated\n'


@router.head('/mandrill/inbound/')
def mandrill_inbound_head_view():
    """Mandrill checks a webhook's URL with a HEAD request when the inbound route is saved."""
    return Response(status_code=200)


@router.post('/mandrill/inbound/')
async def mandrill_inbound_view(request: Request):
    """
    Emails Mandrill received at chat reply addresses. Each is queued to be read by `read_inbound_reply`; nothing
    of an email is read or logged here.

    The body is read from the request, not declared as `Form(...)` parameters: Logfire records the declared
    arguments of an endpoint, and this body is people's emails. Mandrill sends an email again when the answer
    is not 200, so only a POST that could succeed on another try is answered with anything else.
    """
    if not settings.inbound_replies_set_up:
        # Mandrill has to be able to save the route before the key it then shows exists
        logger.info('inbound not set up')
        return 'ok\n'
    content_length = request.headers.get('content-length', '')
    if not content_length.isdigit() or int(content_length) > MAX_INBOUND_POST_BYTES:
        logger.warning('inbound post not read: it has no length or is over the size limit')
        return 'ok\n'
    body = await request.body()
    signature = request.headers.get('x-mandrill-signature', '')
    # reading the fields, checking the signature and reading the events take time that grows with the body, and
    # queueing talks to Redis: none of it belongs on the event loop
    return await run_in_threadpool(_queue_inbound_replies, body, signature)


def _queue_inbound_replies(body: bytes, signature: str) -> str:
    try:
        fields = dict(parse_qsl(body.decode(), keep_blank_values=True, max_num_fields=MAX_INBOUND_POST_FIELDS))
    except ValueError:
        # not text, or more fields than Mandrill posts: nothing Mandrill signed
        raise HTTP403('invalid signature')
    sig_generated = _mandrill_signature(
        settings.mandrill_inbound_webhook_url, settings.mandrill_inbound_webhook_key, fields
    )
    if not hmac.compare_digest(sig_generated, signature.encode()):
        raise HTTP403('invalid signature')

    try:
        events = json.loads(fields.get(MANDRILL_EVENTS_FIELD, ''))
    except (ValueError, RecursionError):
        events = None
    if not isinstance(events, list):
        # Mandrill would send the same again
        logger.warning('inbound post not read: its events are not a list')
        return 'ok\n'

    received_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    queued = 0
    for event in events:
        # only the address Mandrill matched and the email as it arrived are used, never the fields Mandrill
        # read out of the email itself
        message = event.get('msg') if isinstance(event, dict) and event.get('event') == INBOUND_EVENT else None
        if not isinstance(message, dict):
            continue
        address, raw_message = message.get('email'), message.get('raw_msg')
        if isinstance(address, str) and isinstance(raw_message, str) and CHAT_REPLY_ADDRESS.match(address):
            # an exception here is raised, so Mandrill is answered 500 and sends the emails again
            read_inbound_reply.delay(raw_message, address.lower(), received_at)
            queued += 1
    logger.info('inbound post: %d of %d events queued', queued, len(events))
    return 'ok\n'


@router.get('/messagebird/')
def messagebird_webhook_view(request: Request):
    """Update messages sent with messagebird."""
    try:
        event = MessageBirdWebHook(**dict(request.query_params))
    except ValidationError as e:
        raise HTTP422(str(e))
    if event.error_code is not None:
        if event.error_code == '104':
            logger.error(
                '[webhooks][mesagebird] carrier rejected error',
                extra={'id': event.message_id, 'datetime': event.ts.isoformat(), 'status': event.status},
            )
        else:
            logger.error('[webhooks][mesagebird] delivery failed with status: %s', event.status)

    method = SendMethod.sms_messagebird
    test_param = request.query_params.get('test')
    if test_param and test_param.lower() == 'true':
        method = SendMethod.sms_test
    update_message_status.delay(method.value, event.model_dump(mode='json', by_alias=True))
    return 'message status updated\n'
