from datetime import date
from foxglove.exceptions import HttpNotFound
from sqlalchemy import func, insert, select
from sqlalchemy.orm import Session
from typing import List, Tuple

from src.models import Company, Event, Link, Message, MessageGroup
from src.schema import SendMethod, SmsSendMethod


def get_company_id(conn, company_code: str) -> int:
    company_id = conn.query(Company).filter(Company.code == company_code)
    if not company_id:
        raise HttpNotFound('company not found')
    return company_id


def get_messages(conn, company_id: int, tags: list = None, q: str = None, p_from: int = None) -> List[Message]:
    query = select(Message).join(Message.c.message_group).where(Message.company_id == company_id).limit(10000)
    if tags:
        query = query.where(Message.c.tags.contains(tags))
    if q:
        query = query.where(Message.c.vector.match(q))
    if p_from:
        query = query.offset(p_from)
    return conn.execute(query)


def create_message(conn: Session, message: Message):
    conn.add(message)
    conn.commit()
    conn.refresh(message)
    return message


def get_message(conn: Session, id: int, company_code: str = None, send_method: SendMethod = None) -> Message:
    if company_code:
        company_id = get_company_id(conn, company_code)
        return conn.query(Message).filter(Message.company_id == company_id, Message.id == id).first()
    else:
        assert send_method
        return conn.query(Message).filter(Message.method == send_method, Message.id == id).first()


def get_message_events(conn: Session, message_id: int) -> Tuple[int, List[Event]]:
    events = conn.query(Event).filter(Event.message_id == message_id).order_by(Event.message_id).limit(51)
    extra_count = 0
    if len(events) == 51:
        extra_count = 1 + conn.query(Event).filter(Event.message_id == message_id).count()
    return extra_count, events


def create_message_group(conn: Session, message_group: MessageGroup) -> MessageGroup:
    conn.add(message_group)
    conn.commit()
    conn.refresh(message_group)
    return message_group


def create_links(conn: Session, *links: Link):
    conn.execute(Link.c.insert(), links)


def get_link(conn: Session, token: str = None, id: int = None) -> Link:
    if token:
        return conn.query(Link).filter(Link.token == token)
    else:
        assert id
        return conn.query(Link).filter(Link.id == id)


def create_event(conn: Session, event: Event) -> Event:
    conn.add(event)
    conn.commit()
    conn.refresh(event)
    return event


def get_sms_spend(conn: Session, company_id: int, start: date, end: date, method: SmsSendMethod):
    return conn.query(func.sum(Message.cost)).filter(
        Message.method == method, Message.company_id == company_id, Message.send_ts.between(start, end)
    )


def get_create_company_id(conn, company_code: str) -> int:
    company_id = get_company_id(conn, company_code)
    if not company_id:
        query = (
            insert(Company)
            .values(company_code=company_code)
            .on_conflict_do_update(set_={'code': company_code})
            .returning(Company.c.id)
        )
        company_id = conn.execute(query)
    return company_id


def delete_message_groups(conn: Session, company_id: int):
    return conn.query(MessageGroup).filter(company_id=company_id).delete()


def delete_company(conn: Session, company_id):
    m_count = conn.query(Message).filter(company_id=company_id).delete()
    g_count = conn.query(MessageGroup).filter(company_id=company_id).delete()
    conn.query(Company).filter(company_id=company_id).delete()
    return m_count, g_count


agg_sql = """
select json_build_object(
  'histogram', histogram,
  'all_90_day', coalesce(agg.all_90, 0),
  'open_90_day', coalesce(agg.open_90, 0),
  'all_28_day', coalesce(agg.all_28, 0),
  'open_28_day', coalesce(agg.open_28, 0),
  'all_7_day', coalesce(agg.all_7, 0),
  'open_7_day', coalesce(agg.open_7, 0)
)
from (
  select coalesce(json_agg(t), '[]') AS histogram from (
    select coalesce(sum(count), 0) as count, date as day, status
    from message_aggregation
    where %(where)s and date > current_timestamp::date - '28 days'::interval
    group by date, status
  ) as t
) as histogram,
(
  select
    sum(count) as all_90,
    sum(count) filter (where status = 'open') as open_90,
    sum(count) filter (where date > current_timestamp::date - '28 days'::interval) as all_28,
    sum(count) filter (where date > current_timestamp::date - '28 days'::interval and status = 'open') as open_28,
    sum(count) filter (where date > current_timestamp::date - '7 days'::interval) as all_7,
    sum(count) filter (where date > current_timestamp::date - '7 days'::interval and status = 'open') as open_7
  from message_aggregation
  where %(where)s
) as agg
"""


def get_messages_aggregated(conn, company_id, method):
    where = f'company_id = {company_id} and method = {method}'
    return conn.execute(agg_sql % {'where': where})
