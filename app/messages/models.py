import enum
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from markupsafe import Markup
from sqlalchemy import ARRAY, Column, ForeignKey, Index, String, Text, text as sa_text
from sqlalchemy.dialects.postgresql import ENUM, JSONB, TIMESTAMP, TSVECTOR
from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    pass


class SendMethod(str, enum.Enum):
    """Matches SEND_METHODS sql enum."""

    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


class EmailSendMethod(str, enum.Enum):
    email_mandrill = 'email-mandrill'
    email_ses = 'email-ses'
    email_test = 'email-test'


class SmsSendMethod(str, enum.Enum):
    sms_messagebird = 'sms-messagebird'
    sms_test = 'sms-test'


class MessageStatus(str, enum.Enum):
    """Matches MESSAGE_STATUSES sql enum."""

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
    scheduled = 'scheduled'
    buffered = 'buffered'
    delivered = 'delivered'
    expired = 'expired'
    delivery_failed = 'delivery_failed'


# Use existing pg ENUM types created by bootstrap.sql; do not let SQLAlchemy create or drop them.
SEND_METHODS_PG = ENUM(
    'email-mandrill',
    'email-ses',
    'email-test',
    'sms-messagebird',
    'sms-test',
    name='send_methods',
    create_type=False,
)
MESSAGE_STATUSES_PG = ENUM(
    'render_failed',
    'send_request_failed',
    'send',
    'deferral',
    'hard_bounce',
    'soft_bounce',
    'open',
    'click',
    'spam',
    'unsub',
    'reject',
    'scheduled',
    'buffered',
    'delivered',
    'expired',
    'delivery_failed',
    name='message_statuses',
    create_type=False,
)


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class Company(SQLModel, table=True):
    __tablename__ = 'companies'

    id: Optional[int] = Field(default=None, primary_key=True)
    code: str = Field(sa_column=Column(String(63), nullable=False, unique=True))


class MessageGroup(SQLModel, table=True):
    __tablename__ = 'message_groups'
    __table_args__ = (
        Index('message_group_uuid', 'uuid', unique=True),
        Index('message_group_company_method', 'company_id', 'message_method'),
        Index('message_group_method', 'message_method'),
        Index('message_group_created_ts', 'created_ts'),
        Index('message_group_company_id', 'company_id'),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: UUID = Field(nullable=False)
    company_id: int = Field(sa_column=Column(ForeignKey('companies.id', ondelete='CASCADE'), nullable=False))
    message_method: str = Field(sa_column=Column(SEND_METHODS_PG, nullable=False))
    created_ts: datetime = Field(
        default_factory=utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=sa_text('CURRENT_TIMESTAMP')),
    )
    from_email: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    from_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))


class Message(SQLModel, table=True):
    __tablename__ = 'messages'
    __table_args__ = (
        Index('message_company_id', 'company_id'),
        Index('message_group_id_send_ts', 'group_id', 'send_ts'),
        Index('message_group_id', 'group_id'),
        Index('message_external_id', 'external_id'),
        Index('message_send_ts', sa_text('send_ts DESC'), 'method', 'company_id'),
        Index('message_update_ts', sa_text('update_ts DESC')),
        Index('message_tags', 'tags', 'method', 'company_id', postgresql_using='gin'),
        Index('message_vector', 'vector', 'method', 'company_id', postgresql_using='gin'),
        Index('message_company_method', 'method', 'company_id', 'id'),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    external_id: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    group_id: int = Field(sa_column=Column(ForeignKey('message_groups.id', ondelete='CASCADE'), nullable=False))
    company_id: int = Field(sa_column=Column(ForeignKey('companies.id', ondelete='CASCADE'), nullable=False))
    method: str = Field(sa_column=Column(SEND_METHODS_PG, nullable=False))
    send_ts: datetime = Field(
        default_factory=utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=sa_text('CURRENT_TIMESTAMP')),
    )
    update_ts: datetime = Field(
        default_factory=utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=sa_text('CURRENT_TIMESTAMP')),
    )
    status: str = Field(sa_column=Column(MESSAGE_STATUSES_PG, nullable=False, server_default='send'))
    to_first_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    to_last_name: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    to_user_link: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    to_address: Optional[str] = Field(default=None, sa_column=Column(String(255)))
    tags: Optional[list[str]] = Field(default=None, sa_column=Column(ARRAY(String(255))))
    subject: Optional[str] = Field(default=None, sa_column=Column(Text))
    body: Optional[str] = Field(default=None, sa_column=Column(Text))
    attachments: Optional[list[str]] = Field(default=None, sa_column=Column(ARRAY(String(255))))
    cost: Optional[float] = Field(default=None)
    extra: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    # Populated by the set_message_vector BEFORE INSERT trigger; the empty-tsvector server_default
    # is a defensive backup so an INSERT still succeeds if the trigger ever goes missing.
    vector: Optional[str] = Field(
        default=None,
        sa_column=Column(TSVECTOR, nullable=False, server_default=sa_text("''::tsvector")),
    )

    @staticmethod
    def status_display(v: str) -> str:
        return {
            'send': 'Sent',
            'open': 'Opened',
            'click': 'Opened & clicked on',
            'soft_bounce': 'Bounced (retried)',
            'hard_bounce': 'Bounced',
            'delivered': 'Delivered',
            'delivery_failed': 'Delivery failed',
            'sent': 'Sent',
            'expired': 'Expired',
        }.get(v, v)

    def get_status_display(self) -> str:
        return self.status_display(self.status)

    @property
    def parsed_details(self) -> dict:
        method = self.method or ''
        return {
            'id': self.id,
            'external_id': self.external_id,
            'to_ext_link': self.to_user_link,
            'to_address': self.to_address,
            'to_dst': f'{self.to_first_name or ""} {self.to_last_name or ""} <{self.to_address}>'.strip(' '),
            'to_name': f'{self.to_first_name or ""} {self.to_last_name or ""}',
            'send_ts': self.send_ts,
            'subject': self.subject if method.startswith('email') else self.body,
            'update_ts': self.update_ts,
            'status': self.get_status_display(),
            'method': self.method,
            'cost': self.cost or 0,
        }

    def get_attachments(self):
        if self.attachments:
            for a in self.attachments:
                name = None
                try:
                    doc_id_str, name = a.split('::')
                    doc_id = int(doc_id_str)
                except ValueError:
                    yield '#', name or a
                else:
                    yield f'/attachment-doc/{doc_id}/', name


class Event(SQLModel, table=True):
    __tablename__ = 'events'
    __table_args__ = (Index('event_message_id', 'message_id'),)

    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(sa_column=Column(ForeignKey('messages.id', ondelete='CASCADE'), nullable=False))
    status: str = Field(sa_column=Column(MESSAGE_STATUSES_PG, nullable=False))
    ts: datetime = Field(
        default_factory=utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=sa_text('CURRENT_TIMESTAMP')),
    )
    extra: Optional[dict] = Field(default=None, sa_column=Column(JSONB))

    @staticmethod
    def status_display(v: str) -> str:
        return {
            'send': 'Sent',
            'open': 'Opened',
            'click': 'Opened & clicked on',
            'soft_bounce': 'Bounced (retried)',
            'hard_bounce': 'Bounced',
        }.get(v, v)

    def get_status_display(self) -> str:
        return self.status_display(self.status)

    @property
    def parsed_details(self) -> dict:
        event_data: dict = dict(status=self.get_status_display(), datetime=self.ts)
        if self.extra:
            event_data['details'] = Markup(json.dumps(self.extra, indent=2))
        return event_data


class Link(SQLModel, table=True):
    __tablename__ = 'links'
    __table_args__ = (
        Index('link_token', 'token'),
        Index('link_message_id', 'message_id'),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(sa_column=Column(ForeignKey('messages.id', ondelete='CASCADE'), nullable=False))
    token: Optional[str] = Field(default=None, sa_column=Column(String(31)))
    url: Optional[str] = Field(default=None, sa_column=Column(Text))
