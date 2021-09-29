import json
import re
from datetime import datetime, timezone
from enum import Enum
from pydantic import BaseModel, validator
from pydantic.validators import str_validator
from typing import List

from src.schemas.messages import MessageStatus


class MandrillMessageStatus(str, Enum):
    """
    compatible with mandrill webhook event field
    https://mandrill.zendesk.com/hc/en-us/articles/205583307-Message-Event-Webhook-format
    """

    send = 'send'
    deferral = 'deferral'
    hard_bounce = 'hard_bounce'
    soft_bounce = 'soft_bounce'
    open = 'open'
    click = 'click'
    spam = 'spam'
    unsub = 'unsub'
    reject = 'reject'


class MessageBirdMessageStatus(str, Enum):
    """
    https://developers.messagebird.com/docs/messaging#messaging-dlr
    """

    scheduled = 'scheduled'
    send = 'send'
    buffered = 'buffered'
    delivered = 'delivered'
    expired = 'expired'
    delivery_failed = 'delivery_failed'


class BaseWebhook(BaseModel):
    ts: datetime
    status: MessageStatus
    message_id: str

    def extra_json(self, sort_keys=False):
        raise NotImplementedError()

    @validator('ts')
    def add_tz(cls, v):
        if v and not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


class IDStr(str):
    @classmethod
    def get_validators(cls):
        yield str_validator
        yield cls.validate

    @classmethod
    def validate(cls, value: str) -> str:
        return ID_REGEX.sub('', value)


class MandrillSingleWebhook(BaseWebhook):
    ts: datetime
    status: MandrillMessageStatus
    message_id: IDStr
    user_agent: str = None
    location: dict = None
    msg: dict = {}

    def extra_json(self, sort_keys=False):
        return json.dumps(
            {
                'user_agent': self.user_agent,
                'location': self.location,
                **{f: self.msg.get(f) for f in self.__config__.msg_fields},
            },
            sort_keys=sort_keys,
        )

    class Config:
        ignore_extra = True
        fields = {'message_id': '_id', 'status': 'event'}
        msg_fields = ('bounce_description', 'clicks', 'diag', 'reject', 'opens', 'resends', 'smtp_events', 'state')


class MandrillWebhook(BaseModel):
    events: List[MandrillSingleWebhook]


class MessageBirdWebHook(BaseWebhook):
    ts: datetime
    status: MessageBirdMessageStatus
    message_id: IDStr
    error_code: str = None

    def extra_json(self, sort_keys=False):
        return json.dumps({'error_code': self.error_code} if self.error_code else {}, sort_keys=sort_keys)

    class Config:
        ignore_extra = True
        fields = {'message_id': 'id', 'ts': 'statusDatetime', 'error_code': 'statusErrorCode'}


ID_REGEX = re.compile(r'[/<>= ]')
