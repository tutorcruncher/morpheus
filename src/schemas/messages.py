from enum import Enum
from pathlib import Path
from pydantic import BaseModel, NameEmail, constr
from typing import Dict, List
from uuid import UUID

THIS_DIR = Path(__file__).parent.parent.resolve()


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


class MessageStatus(str, Enum):
    """
    Combined MandrillMessageStatus and MessageBirdMessageStatus

    Should match MESSAGE_STATUSES sql enum
    """

    render_failed = 'render_failed'
    send_request_failed = 'send_request_failed'
    spam_detected = 'spam_detected'  # this status is used when morpheus spam service check detects spam

    send = 'send'
    deferral = 'deferral'
    hard_bounce = 'hard_bounce'
    soft_bounce = 'soft_bounce'
    open = 'open'
    click = 'click'
    spam = 'spam'  # this status is used when recipient marks the email as spam
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
