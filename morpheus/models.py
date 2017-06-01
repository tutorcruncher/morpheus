from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, NameEmail, constr

from .utils import WebModel

THIS_DIR = Path(__file__).parent.resolve()


class SendMethod(str, Enum):
    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


class MessageStatus(str, Enum):
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


class AttachmentModel(BaseModel):
    name: str = ...
    html: str = ...


class RecipientModel(BaseModel):
    # TODO prepend to_ to first_name, last_name, address
    first_name: str = None
    last_name: str = None
    user_id: int = None
    address: str = ...
    search_tags: List[str] = []
    context: dict = {}
    pdf_attachments: List[AttachmentModel] = []


class SendModel(WebModel):
    uid: constr(min_length=20, max_length=40) = ...
    main_template: str = (THIS_DIR / 'extra' / 'default-email-template.mustache').read_text()
    markdown_template: str = ...
    mustache_partials: Dict[str, str] = None
    subject_template: str = ...
    company_code: str = ...
    from_address: NameEmail = ...
    reply_to: str = None
    method: SendMethod = ...
    subaccount: str = None
    analytics_tags: List[str] = []
    context: dict = {}
    recipients: List[RecipientModel] = ...


class MandrillWebhook(WebModel):
    ts: datetime = ...
    event: MessageStatus = ...
    message_id: str = ...

    class Config:
        allow_extra = True
        fields = {
            'message_id': '_id',
        }
