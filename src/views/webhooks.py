import base64
import hashlib
import hmac
import ujson
from fastapi import APIRouter, Header
from foxglove import glove
from foxglove.exceptions import HttpBadRequest, HttpForbidden
from foxglove.route_class import KeepBodyAPIRoute

from src.schema import MandrillSingleWebhook, MessageBirdWebHook, SendMethod
from src.views.common import index

app = APIRouter(route_class=KeepBodyAPIRoute)


@app.post('/test/')
async def test_webhook_view(m: MandrillSingleWebhook):
    """
    Simple view to update messages faux-sent with email-test
    """
    await glove.redis.enqueue_job('update_message_status', SendMethod.email_test, m)
    return 'message status updated\n'


@app.head('/mandrill/')
async def mandrill_head_view():
    return await index()


@app.post('/mandrill/')
async def mandrill_webhook_view(event_data: dict, X_Mandrill_Signature: str = Header(None)):
    sig_generated = base64.b64encode(
        hmac.new(
            glove.settings.webhook_auth_key,
            msg=(glove.settings.mandrill_webhook_url + 'mandrill_events' + event_data).encode(),
            digestmod=hashlib.sha1,
        ).digest()
    )
    if not hmac.compare_digest(sig_generated, X_Mandrill_Signature):
        raise HttpForbidden('invalid signature')
    try:
        events = ujson.loads(event_data)
    except ValueError as e:
        raise HttpBadRequest(f'invalid json data: {e}')

    await glove.redis.enqueue_job('update_mandrill_webhooks', events)
    return 'message status updated\n'


@app.get('/messagebird/')
async def messagebird_webhook_view(m: MessageBirdWebHook):
    """
    Update messages sent with message bird
    """
    await glove.redis.enqueue_job('update_message_status', SendMethod.sms_messagebird, m)
    return 'message status updated\n'
