import base64
import hashlib
import hmac
import json
from fastapi import APIRouter, Form, Header
from foxglove import glove
from foxglove.exceptions import HttpBadRequest, HttpForbidden, HttpUnprocessableEntity
from foxglove.route_class import KeepBodyAPIRoute
from pydantic import ValidationError
from starlette.requests import Request

from src.schemas.messages import SendMethod
from src.schemas.webhooks import MandrillSingleWebhook, MessageBirdWebHook
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
async def mandrill_head_view(request: Request):
    return await index(request)


@app.post('/mandrill/')
async def mandrill_webhook_view(mandrill_events=Form(None), X_Mandrill_Signature: bytes = Header(None)):
    try:
        events = json.loads(mandrill_events)
    except ValueError:
        raise HttpBadRequest('Invalid data')
    msg = f'{glove.settings.mandrill_webhook_url}mandrill_events{mandrill_events}'
    sig_generated = base64.b64encode(
        hmac.new(glove.settings.mandrill_webhook_key.encode(), msg=msg.encode(), digestmod=hashlib.sha1).digest()
    )
    if not hmac.compare_digest(sig_generated, X_Mandrill_Signature):
        raise HttpForbidden('invalid signature')

    await glove.redis.enqueue_job('update_mandrill_webhooks', events)
    return 'message status updated\n'


@app.get('/messagebird/')
async def messagebird_webhook_view(request: Request):
    """
    Update messages sent with message bird
    """
    try:
        event = MessageBirdWebHook(**request.query_params)
    except ValidationError as e:
        raise HttpUnprocessableEntity(e.args[0])
    method = SendMethod.sms_messagebird
    if (test:=request.query_params.get('test')) and bool(test) == True:
        method = SendMethod.sms_test
    await glove.redis.enqueue_job('update_message_status', method, event)
    return 'message status updated\n'
