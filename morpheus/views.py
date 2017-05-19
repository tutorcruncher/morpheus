from enum import Enum
from typing import Dict, List, Any

from pydantic import BaseModel


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
    tags: Dict[str, Any] = None
    context: dict = ...
    pdf_html: List[Dict[str, str]] = ...


class SendModel(BaseModel):
    id: str = ...
    outer_template: str = None
    main_template: str = ...
    subject_template: str = None
    company_code: str = ...
    from_address: str = ...
    reply_to: str = None
    method: SendMethod = ...
    subaccount: str = None
    analytics_tags: List[str] = ...
    recipients: List[RecipientModel] = ...
