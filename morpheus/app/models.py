from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List

from aiohttp.web_exceptions import HTTPBadRequest
from pydantic import BaseModel, NameEmail, ValidationError, constr

THIS_DIR = Path(__file__).parent.resolve()


class WebModel(BaseModel):
    def _process_values(self, values):
        try:
            return super()._process_values(values)
        except ValidationError as e:
            raise HTTPBadRequest(text=e.display_errors)


class SendMethod(str, Enum):
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


class AttachmentModel(BaseModel):
    name: str = ...
    html: str = ...
    id: int = None

    class Config:
        max_anystr_length = int(1e7)


class EmailRecipientModel(BaseModel):
    first_name: str = None
    last_name: str = None
    user_link: str = None
    address: str = ...
    tags: List[str] = []
    context: dict = {}
    headers: dict = {}
    pdf_attachments: List[AttachmentModel] = []

    class Config:
        max_anystr_length = int(1e7)


class EmailSendModel(WebModel):
    uid: constr(min_length=20, max_length=40) = ...
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


class SubaccountModel(WebModel):
    company_code: str = ...
    company_name: str = None


class SmsRecipientModel(BaseModel):
    first_name: str = None
    last_name: str = None
    user_link: str = None
    number: str = ...
    tags: List[str] = []
    context: dict = {}


class SmsSendModel(WebModel):
    uid: constr(min_length=20, max_length=40) = ...
    main_template: str = ...
    company_code: str = ...
    cost_limit: float = None
    country_code: constr(min_length=2, max_length=2) = 'GB'
    from_name: constr(min_length=1, max_length=11) = 'Morpheus'
    method: SmsSendMethod = ...
    tags: List[str] = []
    context: dict = {}
    recipients: List[SmsRecipientModel] = ...


class SmsNumbersModel(WebModel):
    numbers: Dict[int, str] = ...
    country_code: constr(min_length=2, max_length=2) = 'GB'


class BaseWebhook(WebModel):
    ts: datetime
    status: MessageStatus
    message_id: str

    def extra(self):
        raise NotImplementedError()


class MandrillSingleWebhook(BaseWebhook):
    ts: datetime
    status: MandrillMessageStatus
    message_id: str
    user_agent: str = None
    location: dict = None
    msg: dict = {}

    def extra(self):
        return {
            'user_agent': self.user_agent,
            'location': self.location,
            **{f: self.msg.get(f) for f in self.config.msg_fields},
        }

    class Config:
        ignore_extra = True
        fields = {
            'message_id': '_id',
            'status': 'event'
        }
        msg_fields = (
            'bounce_description',
            'clicks',
            'diag',
            'reject',
            'opens',
            'resends',
            'smtp_events',
            'state',
        )


class MandrillWebhook(WebModel):
    events: List[MandrillSingleWebhook] = ...


class MessageBirdWebHook(BaseWebhook):
    ts: datetime
    status: MessageBirdMessageStatus
    message_id: str
    error_code: str = None

    def extra(self):
        return {'error_code': self.error_code} if self.error_code else {}

    class Config:
        ignore_extra = True
        fields = {
            'message_id': 'id',
            'ts': 'statusDatetime',
            'error_code': 'statusErrorCode',
        }


class ClickInfo(BaseWebhook):
    ts: datetime
    status: MessageStatus
    message_id: str
    extra_: dict

    def extra(self):
        return self.extra_
