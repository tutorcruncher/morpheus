from enum import Enum
from typing import Dict, List

import msgpack
from aiohttp.web import HTTPConflict, Response
from pydantic import BaseModel, constr

from .utils import ServiceView, WebModel


class SendMethod(str, Enum):
    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


class RecipientModel(BaseModel):
    first_name: str = None
    last_name: str = None
    user_id: int = None
    address: str = ...
    tags: dict = None
    context: dict = {}
    pdf_html: List[Dict[str, str]] = []


class SendModel(WebModel):
    id: constr(min_length=20, max_length=40) = ...
    outer_template: str = None
    main_template: str = ...
    subject_template: str = None
    company_code: str = ...
    from_address: str = ...
    reply_to: str = None
    method: SendMethod = ...
    subaccount: str = None
    analytics_tags: List[str] = []
    context: dict = {}
    recipients: List[RecipientModel] = ...


class SendView(ServiceView):
    async def call(self, request):
        m: SendModel = await self.request_data(SendModel)
        async with await self.sender.get_redis_conn() as redis:
            group_key = f'group:{m.id}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.id}" already exists\n')
            recipients_key = f'recipients:{m.id}'
            data = m.values
            recipients = data.pop('recipients')
            await redis.lpush(recipients_key, *map(self.encode_recipients, recipients))
            await redis.expire(group_key, 86400)
            await redis.expire(recipients_key, 86400)
            await self.sender.send(recipients_key, **data)
        return Response(text='201 job enqueued\n', status=201)

    def encode_recipients(self, recipient):
        return msgpack.packb(recipient.values, use_bin_type=True)
