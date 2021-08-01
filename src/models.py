from sqlalchemy import TEXT, VARCHAR, Column, Enum, Float, ForeignKey, Index, Integer, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP, TSVECTOR, UUID
from sqlalchemy.orm import declarative_base, relationship

from src.schema import MessageStatus, SendMethod

Base = declarative_base()


class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, index=True)
    code = Column(VARCHAR(63), unique=True, nullable=False)


class MessageGroup(Base):
    __tablename__ = 'message_groups'

    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(UUID, nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), index=True)
    message_method = Column(Enum(SendMethod), nullable=False, index=True)
    created_ts = Column(TIMESTAMP, nullable=False, default=func.now(), index=True)
    from_email = Column(VARCHAR(255))
    from_name = Column(VARCHAR(255))

    company = relationship(Company, back_populates='message_groups')

    Index('message_group_company_method', 'company_id', 'message_method')
    Index('message_group_uuid', 'uuid', unique=True)


class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(VARCHAR(255), nullable=False, index=True)
    group_id = Column(Integer, ForeignKey('message_groups.id', ondelete='CASCADE'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id', ondelete='CASCADE'), index=True, nullable=False)

    method = Column(Enum(SendMethod), nullable=False)
    send_ts = Column(TIMESTAMP, nullable=False, default=func.now())
    update_ts = Column(TIMESTAMP, nullable=False, default=func.now())
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

    company = relationship(Company, back_populates='messages')
    group = relationship(MessageGroup, back_populates='messages')

    Index('message_group_id_send_ts', 'group_id', 'send_ts')
    Index('message_send_ts', 'send_ts desc', 'method', 'company_id')
    Index('message_update_ts', 'update_ts desc')
    Index('message_tags', 'tags', 'method', 'company_id', postgresql_using='gin')
    Index('message_vector', 'vector', 'method', 'company_id', postgresql_using='gin')
    Index('message_company_method', 'method', 'company_id', 'id')

    @property
    def details(self):
        yield 'ID', self.external_id
        yield 'Status', self.status.title()

        dst = f'{self.to_first_name or ""} {self.to_last_name or ""} <{self.to_address}>'.strip(' ')
        if self.to_user_link:
            yield 'To', dict(href=self.to_user_link, value=dst)
        else:
            yield 'To', dst

        yield 'Subject', self.subject
        # could do with using prettier timezones here
        yield 'Send Time', {'class': 'datetime', 'value': self.send_ts}
        yield 'Last Updated', {'class': 'datetime', 'value': self.update_ts}

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


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey('messages.id', ondelete='CASCADE'), index=True)
    status = Column(Enum(MessageStatus), default=MessageStatus.send, nullable=False)
    ts = Column(TIMESTAMP, nullable=False, default=func.now())
    extra = Column(JSONB)

    message = relationship(Message, back_populates='events')


class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey('messages.id', ondelete='CASCADE'))
    token = Column(VARCHAR(31))
    url = Column(TEXT)

    message = relationship(Message, back_populates='links')
