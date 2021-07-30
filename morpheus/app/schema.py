import json
import re
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from pydantic import BaseModel as _BaseModel, NameEmail, constr, validator, BaseModel
from pydantic.validators import str_validator
from typing import Dict, List
from uuid import UUID

THIS_DIR = Path(__file__).parent.resolve()


class BaseModel(_BaseModel):
    def __setstate__(self, state):
        if '__values__' in state:
            object.__setattr__(self, '__dict__', state['__values__'])
        else:
            object.__setattr__(self, '__dict__', state['__dict__'])
        object.__setattr__(self, '__fields_set__', state['__fields_set__'])


class SendMethod(str, Enum):
    """
    Should match SEND_METHODS sql enum
    """

    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


class EmailSendMethod(str, Enum):
    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'


class SmsSendMethod(str, Enum):
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


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


class MessageStatus(str, Enum):
    """
    Combined MandrillMessageStatus and MessageBirdMessageStatus

    Should match MESSAGE_STATUSES sql enum
    """

    render_failed = 'render_failed'
    send_request_failed = 'send_request_failed'

    send = 'send'
    deferral = 'deferral'
    hard_bounce = 'hard_bounce'
    soft_bounce = 'soft_bounce'
    open = 'open'
    click = 'click'
    spam = 'spam'
    unsub = 'unsub'
    reject = 'reject'

    # used for sms
    scheduled = 'scheduled'
    # send = 'send'  # above
    buffered = 'buffered'
    delivered = 'delivered'
    expired = 'expired'
    delivery_failed = 'delivery_failed'


class PDFAttachmentModel(BaseModel):
    name: str
    html: str
    id: int = None

    class Config:
        max_anystr_length = int(1e7)


class AttachmentModel(BaseModel):
    name: str
    mime_type: str
    content: bytes


class EmailRecipientModel(BaseModel):
    first_name: str = None
    last_name: str = None
    user_link: str = None
    address: str = ...
    tags: List[str] = []
    context: dict = {}
    headers: dict = {}
    pdf_attachments: List[PDFAttachmentModel] = []
    attachments: List[AttachmentModel] = []

    class Config:
        max_anystr_length = int(1e7)


class EmailSendModel(BaseModel):
    uid: UUID
    main_template: str = (THIS_DIR / 'extra' / 'default-email-template.mustache').read_text()
    mustache_partials: Dict[str, str] = None
    macros: Dict[str, str] = None
    subject_template: str = ...
    company_code: str = ...
    from_address: NameEmail = ...
    method: EmailSendMethod = ...
    subaccount: str = None
    tags: List[str] = []
    context: dict = {}
    headers: dict = {}
    important = False
    recipients: List[EmailRecipientModel] = ...


class SubaccountModel(BaseModel):
    company_code: str = ...
    company_name: str = None


class SmsRecipientModel(BaseModel):
    first_name: str = None
    last_name: str = None
    user_link: str = None
    number: str = ...
    tags: List[str] = []
    context: dict = {}


class SmsSendModel(BaseModel):
    uid: constr(min_length=20, max_length=40)
    main_template: str
    company_code: str
    cost_limit: float = None
    country_code: constr(min_length=2, max_length=2) = 'GB'
    from_name: constr(min_length=1, max_length=11) = 'Morpheus'
    method: SmsSendMethod = ...
    tags: List[str] = []
    context: dict = {}
    recipients: List[SmsRecipientModel] = ...


class SmsNumbersModel(BaseModel):
    numbers: Dict[int, str]
    country_code: constr(min_length=2, max_length=2) = 'GB'


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


ID_REGEX = re.compile(r'[/<>= ]')


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


class Session(BaseModel):
    company: str
    expires: datetime
