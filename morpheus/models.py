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
    sent = 'sent'
    deferred = 'deferred'
    opened = 'opened'
    soft_bounced = 'soft_bounced'
    hard_bounced = 'hard_bounced'
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
    search_tags: dict = None
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