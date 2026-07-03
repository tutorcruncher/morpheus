import json
import re
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, NameEmail, StringConstraints, field_validator
from typing_extensions import Annotated

from app.messages.models import EmailSendMethod, MessageStatus, SendMethod, SmsSendMethod  # noqa: F401

THIS_DIR = Path(__file__).parent.parent.resolve()


class PDFAttachmentModel(BaseModel):
    model_config = ConfigDict(str_max_length=int(1e7))

    name: str
    html: str
    id: Optional[int] = None


class AttachmentModel(BaseModel):
    name: str
    mime_type: str
    content: bytes


class EmailRecipientModel(BaseModel):
    model_config = ConfigDict(str_max_length=int(1e7), coerce_numbers_to_str=True)

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    user_link: Optional[str] = None
    address: str
    tags: list[str] = []
    context: dict = {}
    headers: dict = {}
    pdf_attachments: list[PDFAttachmentModel] = []
    attachments: list[AttachmentModel] = []


def _default_email_template() -> str:
    return (THIS_DIR / 'extra' / 'default-email-template.mustache').read_text()


class EmailSendModel(BaseModel):
    # pydantic v1 coerced numeric JSON values to str for str fields; preserve that for callers
    # that send e.g. a numeric company_code (v2 rejects int→str by default).
    model_config = ConfigDict(coerce_numbers_to_str=True)

    uid: UUID
    main_template: str = Field(default_factory=_default_email_template)
    mustache_partials: Optional[dict[str, str]] = None
    macros: Optional[dict[str, str]] = None
    subject_template: str
    company_code: str
    from_address: NameEmail
    method: EmailSendMethod
    subaccount: Optional[str] = None
    tags: list[str] = []
    context: dict = {}
    headers: dict = {}
    important: bool = False
    recipients: list[EmailRecipientModel]


class SubaccountModel(BaseModel):
    company_code: str
    company_name: Optional[str] = None


class SmsRecipientModel(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    user_link: Optional[str] = None
    number: str
    tags: list[str] = []
    context: dict = {}


class SmsSendModel(BaseModel):
    # See EmailSendModel: preserve pydantic v1 numeric→str coercion for str fields.
    model_config = ConfigDict(coerce_numbers_to_str=True)

    uid: Annotated[str, StringConstraints(min_length=20, max_length=40)]
    main_template: str
    company_code: str
    cost_limit: Optional[float] = None
    country_code: Annotated[str, StringConstraints(min_length=2, max_length=2)] = 'GB'
    from_name: Annotated[str, StringConstraints(min_length=1, max_length=11)] = 'Morpheus'
    method: SmsSendMethod
    tags: list[str] = []
    context: dict = {}
    recipients: list[SmsRecipientModel]


class SmsNumbersModel(BaseModel):
    numbers: dict[int, str]
    country_code: Annotated[str, StringConstraints(min_length=2, max_length=2)] = 'GB'

    @field_validator('numbers', mode='before')
    @classmethod
    def coerce_pairs_to_dict(cls, v: Any) -> Any:
        # TC2 sends `numbers` as a list of (id, number) pairs (dict.items()). Pydantic v1
        # coerced this to a dict via dict(v); v2 does not, so restore that behaviour here.
        if isinstance(v, (list, tuple)):
            return dict(v)
        return v


# --- Webhook schemas ---


class MandrillMessageStatus(str, Enum):
    send = 'send'
    deferral = 'deferral'
    delivered = 'delivered'
    hard_bounce = 'hard_bounce'
    soft_bounce = 'soft_bounce'
    open = 'open'
    click = 'click'
    spam = 'spam'
    unsub = 'unsub'
    reject = 'reject'


class MessageBirdMessageStatus(str, Enum):
    scheduled = 'scheduled'
    send = 'send'
    buffered = 'buffered'
    delivered = 'delivered'
    expired = 'expired'
    delivery_failed = 'delivery_failed'


ID_REGEX = re.compile(r'[/<>= ]')


def _clean_id(v: Any) -> str:
    return ID_REGEX.sub('', str(v))


class BaseWebhook(BaseModel):
    ts: datetime
    status: MessageStatus
    message_id: str

    @field_validator('ts')
    @classmethod
    def add_tz(cls, v: datetime) -> datetime:
        if v and not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v

    def extra_json(self, sort_keys: bool = False) -> str:
        raise NotImplementedError


class MandrillSingleWebhook(BaseWebhook):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    status: MandrillMessageStatus = Field(alias='event')
    message_id: str = Field(alias='_id')
    user_agent: Optional[str] = None
    location: Optional[dict] = None
    msg: dict = {}

    _MSG_FIELDS = (
        'bounce_description',
        'clicks',
        'diag',
        'reject',
        'opens',
        'resends',
        'smtp_events',
        'state',
    )

    @field_validator('message_id', mode='before')
    @classmethod
    def clean_id(cls, v):
        return _clean_id(v)

    def extra_json(self, sort_keys: bool = False) -> str:
        return json.dumps(
            {
                'user_agent': self.user_agent,
                'location': self.location,
                **{f: self.msg.get(f) for f in self._MSG_FIELDS},
            },
            sort_keys=sort_keys,
        )


class MessageBirdWebHook(BaseWebhook):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    status: MessageBirdMessageStatus
    message_id: str = Field(alias='id')
    ts: datetime = Field(alias='statusDatetime')
    error_code: Optional[str] = Field(default=None, alias='statusErrorCode')
    price_amount: Optional[float] = Field(default=None, alias='price[amount]')

    @field_validator('message_id', mode='before')
    @classmethod
    def clean_id(cls, v):
        return _clean_id(v)

    def extra_json(self, sort_keys: bool = False) -> str:
        return json.dumps({'error_code': self.error_code} if self.error_code else {}, sort_keys=sort_keys)
