from datetime import date

from asyncpg import Connection
from foxglove.exceptions import HttpNotFound
from sqlalchemy.orm import Session

from morpheus.app.models import Message, Event, Company


def get_company_id(conn, company_code: str) -> int:
    company_id = conn.query(Company).filter(code=company_code)
    if not company_id:
        raise HttpNotFound('company not found')
    return company_id


def get_messages(conn, **query_params):
    pass


def get_message(conn: Session, company_code: str, id: int) -> Message:
    company_id = get_company_id(conn, company_code)
    return conn.query(Message).filter(company_id=company_id, id=id).first()


def get_message_events(conn: Session, message_id: int):
    events = conn.query(Event).filter(message_id=message_id).order_by(Event.message_id).limit(51)
    extra_count = 0
    if len(events) == 51:
        extra_count = 1 + conn.query(Event).filter(message_id=message_id).count()
    return extra_count, events


def get_sms_spend(conn: Connection, company_code: str, start: date, end: date, method: str):
    v = await conn.fetchval(
        """
        select sum(cost)
        from messages
        join companies c on messages.company_id = c.id
        where c.code=$1 and method = $4 and send_ts between $2 and $3
        """,
        company_code,
        start,
        end,
        method,
    )
    return v or 0


def get_create_company_id(conn, company_code: str) -> int:
    company_id = await conn.fetchval('select id from companies where code=$1', company_code)
    if not company_id:
        company_id = await conn.fetchval(
            """
            insert into companies (code) values ($1)
            on conflict (code) do update set code=excluded.code
            returning id
            """,
            company_code,
        )
    return company_id
