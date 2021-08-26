import re
from datetime import date
from sqlalchemy import TEXT, VARCHAR, Column, DateTime, Enum, Float, ForeignKey, Index, Integer, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR, UUID
from sqlalchemy.orm import declarative_base, relationship
from typing import List, Tuple

from src.crud import BaseManager
from src.schema import MessageStatus, SendMethod, SmsSendMethod

Base = declarative_base()

__all__ = ('Base', 'Company', 'MessageGroup', 'Message', 'Event', 'Link')


class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, index=True)
    code = Column(VARCHAR(63), unique=True, nullable=False)

    message_groups = relationship('MessageGroup', back_populates='company')
    messages = relationship('Message', back_populates='company')


Company.manager = BaseManager(Company)


class MessageGroup(Base):
    __tablename__ = 'message_groups'

    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(UUID(as_uuid=True), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), index=True)
    message_method = Column(Enum(SendMethod), nullable=False, index=True)
    created_ts = Column(DateTime(timezone=True), nullable=False, default=func.now(), index=True)
    from_email = Column(VARCHAR(255))
    from_name = Column(VARCHAR(255))

    company = relationship('Company', back_populates='message_groups')
    messages = relationship('Message', back_populates='message_group')

    Index('message_group_company_method', 'company_id', 'message_method')
    Index('message_group_uuid', 'uuid', unique=True)


MessageGroup.manager = BaseManager(MessageGroup)


class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(VARCHAR(255), index=True)
    group_id = Column(Integer, ForeignKey('message_groups.id', ondelete='CASCADE'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), index=True, nullable=False)

    method = Column(Enum(SendMethod), nullable=False)
    send_ts = Column(DateTime(timezone=True), nullable=False, default=func.now())
    update_ts = Column(DateTime(timezone=True), nullable=False, default=func.now())
    status = Column(Enum(MessageStatus), default=MessageStatus.send, nullable=False)
    to_first_name = Column(VARCHAR(255))
    to_last_name = Column(VARCHAR(255))
    to_user_link = Column(VARCHAR(255))
    to_address = Column(VARCHAR(255))
    tags = Column(ARRAY(VARCHAR(255)))
    subject = Column(TEXT)
    body = Column(TEXT)
    attachments = Column(ARRAY(VARCHAR(255)))
    cost = Column(Float)
    extra = Column(JSONB)
    vector = Column(TSVECTOR, nullable=False)

    company = relationship('Company', back_populates='messages')
    message_group = relationship('MessageGroup', back_populates='messages')
    events = relationship('Event', back_populates='message')
    links = relationship('Link', back_populates='message')

    Index('message_group_id_send_ts', 'group_id', 'send_ts')
    Index('message_send_ts', 'send_ts desc', 'method', 'company_id')
    Index('message_update_ts', 'update_ts desc')
    Index('message_tags', 'tags', 'method', 'company_id', postgresql_using='gin')
    Index('message_vector', 'vector', 'method', 'company_id', postgresql_using='gin')
    Index('message_company_method', 'method', 'company_id', 'id')

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
    def list_details(self):
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


normalise_re = re.compile(r'[^a-zA-Z0-9 ]')


class MessageManager(BaseManager):
    model = Message

    def filter(
        self, *args, tags: list = None, q: str = None, offset: int = None, limit=10_000, **kwargs
    ) -> List[Message]:
        query = super().filter(*args, **kwargs).join(Message.message_group).order_by(Message.id.desc())
        if tags:
            query = query.filter(Message.tags.contains(tags))
        if q:
            # TODO: Changing the string like this isn't the best, we should find a way to use PG's plain_tsvquery(q)
            query = query.filter(Message.vector.match(normalise_re.sub('', q).replace(' ', '&')))
        if offset:
            query = query.offset(offset)
        return query.limit(limit)

    def get_events(self, **kwargs) -> Tuple[int, List['Event']]:
        events = self.db.query(Event).filter_by(**kwargs).order_by(Event.message_id).limit(51).all()
        extra_count = 0
        if len(events) == 51:
            extra_count = self.db.query(Event).filter_by(**kwargs).count() - 50
        return extra_count, events

    def get_sms_spend(self, company_id: int, start: date, end: date, method: SmsSendMethod) -> float:
        return (
            self.db.query(func.sum(Message.cost))
            .filter(Message.method == method, Message.company_id == company_id, Message.send_ts.between(start, end))
            .scalar()
        )


Message.manager = MessageManager()


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey('messages.id', ondelete='CASCADE'), index=True)
    status = Column(Enum(MessageStatus), default=MessageStatus.send, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=func.now())
    extra = Column(JSONB)

    message = relationship(Message, back_populates='events')

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


class EventManager(BaseManager):
    model = Event

    def create(self, **kwargs) -> Event:
        event = super().create(**kwargs)
        m = event.message
        if event.ts > m.update_ts:
            m.status = event.status
            m.update_ts = event.ts
            self.update(m)
        return event


Event.manager = EventManager(Event)


class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey('messages.id', ondelete='CASCADE'))
    token = Column(VARCHAR(31))
    url = Column(TEXT)

    message = relationship(Message, back_populates='links')


Link.manager = BaseManager(Link)
