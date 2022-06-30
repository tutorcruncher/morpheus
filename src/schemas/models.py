import json
from datetime import datetime
from markupsafe import Markup
from pydantic import UUID4, BaseModel, Json, PositiveInt
from typing import List, Optional

from src.schemas.messages import MessageStatus, SendMethod


class Company(BaseModel):
    id: PositiveInt
    code: str


class MessageGroup(BaseModel):
    id: PositiveInt
    uuid: UUID4
    company_id: PositiveInt
    message_method: SendMethod
    created_ts: datetime
    from_email: str
    from_name: str


class Message(BaseModel):
    id: PositiveInt
    external_id: Optional[str] = ''
    group_id: Optional[PositiveInt]
    company_id: Optional[PositiveInt]

    method: SendMethod
    send_ts: datetime = datetime.now()
    update_ts: datetime = datetime.now()
    status: MessageStatus = MessageStatus.send
    to_first_name: Optional[str] = ''
    to_last_name: Optional[str] = ''
    to_user_link: Optional[str] = ''
    to_address: str = ''
    tags: List[str] = []
    subject: Optional[str] = ''
    body: Optional[str] = ''
    attachments: Optional[List[str]] = []
    cost: Optional[float] = 0
    extra: Json
    vector: Optional[str] = ''

    @staticmethod
    def status_display(v):
        return {
            'send': 'Sent',
            'open': 'Opened',
            'click': 'Opened & clicked on',
            'soft_bounce': 'Bounced (retried)',
            'hard_bounce': 'Bounced',
        }.get(v, v)

    def get_status_display(self):
        return self.status_display(self.status)

    @property
    def parsed_details(self):
        return {
            'id': self.id,
            'external_id': self.external_id,
            'to_ext_link': self.to_user_link,
            'to_address': self.to_address,
            'to_dst': f'{self.to_first_name or ""} {self.to_last_name or ""} <{self.to_address}>'.strip(' '),
            'to_name': f'{self.to_first_name or ""} {self.to_last_name or ""}',
            'send_ts': self.send_ts,
            'subject': self.subject if self.method.startswith('email') else self.body,
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
                    doc_id, name = a.split('::')
                    doc_id = int(doc_id)
                except ValueError:
                    yield '#', name or a
                else:
                    yield f'/attachment-doc/{doc_id}/', name


class Event(BaseModel):
    id: Optional[PositiveInt]
    message_id: Optional[PositiveInt]
    status: Optional[MessageStatus]
    ts: Optional[datetime]
    extra: Optional[Json]

    @staticmethod
    def status_display(v):
        return {
            'send': 'Sent',
            'open': 'Opened',
            'click': 'Opened & clicked on',
            'soft_bounce': 'Bounced (retried)',
            'hard_bounce': 'Bounced',
        }.get(v, v)

    def get_status_display(self):
        return self.status_display(self.status)

    @property
    def parsed_details(self):
        event_data = dict(status=self.get_status_display(), datetime=self.ts)
        if self.extra:
            event_data['details'] = Markup(json.dumps(self.extra, indent=2))
        return event_data


class Link(BaseModel):
    id: PositiveInt
    message_id: PositiveInt
    token: str
    url: str
