import base64
import hashlib
import hmac
import json
import logging

from fastapi import APIRouter, Form, Header, Request
from pydantic import ValidationError

from app.common.api.errors import HTTP400, HTTP403, HTTP422
from app.core.config import settings
from app.messages.api.common import index
from app.messages.models import SendMethod
from app.messages.schemas import MandrillSingleWebhook, MessageBirdWebHook
from app.messages.tasks import update_mandrill_webhooks, update_message_status

router = APIRouter()
logger = logging.getLogger('views.webhooks')


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

    msg = f'{settings.mandrill_webhook_url}mandrill_events{mandrill_events}'
    sig_generated = base64.b64encode(
        hmac.new(settings.mandrill_webhook_key.encode(), msg=msg.encode(), digestmod=hashlib.sha1).digest()
    )
    if not hmac.compare_digest(sig_generated, X_Mandrill_Signature or b''):
        raise HTTP403('invalid signature')

    update_mandrill_webhooks.delay(events)
    return 'message status updated\n'


@router.get('/messagebird/')
def messagebird_webhook_view(request: Request):
    """Update messages sent with messagebird."""
    try:
        event = MessageBirdWebHook(**dict(request.query_params))  # ty:ignore[invalid-argument-type]
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
