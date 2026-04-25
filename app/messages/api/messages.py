import json
import re
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from markupsafe import Markup
from sqlalchemy import func, text
from sqlmodel import select

from app.common.api.errors import HTTP404
from app.common.auth import UserSession
from app.core.database import DBSession, get_db
from app.messages.api.sms import _get_or_create_company, _get_sms_spend, month_interval
from app.messages.models import Event, Message, SendMethod

router = APIRouter(dependencies=[Depends(UserSession)])

LIST_PAGE_SIZE = 100

MESSAGE_COLUMNS = (
    Message.id,
    Message.external_id,
    Message.to_user_link,
    Message.to_address,
    Message.to_first_name,
    Message.to_last_name,
    Message.send_ts,
    Message.update_ts,
    Message.subject,
    Message.body,
    Message.status,
    Message.method,
    Message.attachments,
    Message.cost,
)
MESSAGE_COLUMN_NAMES = (
    'id',
    'external_id',
    'to_user_link',
    'to_address',
    'to_first_name',
    'to_last_name',
    'send_ts',
    'update_ts',
    'subject',
    'body',
    'status',
    'method',
    'attachments',
    'cost',
)

max_length = 100
re_null = re.compile('\x00')
pg_tsquery_split = ''.join((':', '&', '|', '%', '"', "'", '<', '>', '!', '*', '(', ')', r'\s'))
re_tsquery = re.compile(f'[^{pg_tsquery_split}]{{2,}}')


def prepare_search_query(raw_query: Optional[str]) -> Optional[str]:
    if raw_query is None:
        return None
    query = re_null.sub('', raw_query.lower())[:max_length]
    words = re_tsquery.findall(query)
    if not words:
        return None
    return ' & '.join(words) + ':*'


def _row_to_message(row) -> Message:
    return Message(**dict(zip(MESSAGE_COLUMN_NAMES, row)))


@router.get('/{method}/')
def messages_list(
    request: Request,
    method: SendMethod,
    tags: Optional[list[str]] = Query(None),
    q: Optional[str] = None,
    offset: Optional[int] = 0,
    db: DBSession = Depends(get_db),
    user_session=Depends(UserSession),
):
    company = _get_or_create_company(db, user_session.company)
    where_clauses = [Message.method == method.value, Message.company_id == company.id]
    if tags:
        where_clauses.append(Message.tags.op('@>')(tags))
    if q:
        where_clauses.append(Message.vector.op('@@')(func.plainto_tsquery(q.strip())))

    full_count = db.exec(
        select(func.count()).select_from(select(Message.id).where(*where_clauses).limit(10000).subquery())
    ).first()

    items_rows = db.exec(
        select(*MESSAGE_COLUMNS)
        .where(*where_clauses)
        .order_by(Message.id.desc())
        .limit(LIST_PAGE_SIZE)
        .offset(offset or 0)
    ).all()
    items = [_row_to_message(r) for r in items_rows]

    data = {'items': [m.parsed_details for m in items], 'count': full_count}
    this_url = str(request.url_for('messages_list', method=method.value))
    if (offset + len(items)) < full_count:
        data['next'] = f'{this_url}?offset={offset + len(items)}'
    if offset:
        data['previous'] = f'{this_url}?offset={max(offset - LIST_PAGE_SIZE, 0)}'
    if 'sms' in method.value:
        start, end = month_interval()
        data['spend'] = _get_sms_spend(db, company_id=company.id, start=start, end=end, method=method.value) or 0
    return data


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
    where method = :method and company_id = :company_id and date > current_timestamp::date - '28 days'::interval
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
  where method = :method and company_id = :company_id
) as agg
"""


@router.get('/{method}/aggregation/')
def message_aggregation(
    method: SendMethod,
    user_session=Depends(UserSession),
    db: DBSession = Depends(get_db),
):
    company = _get_or_create_company(db, user_session.company)
    raw = db.execute(text(agg_sql), {'method': method.value, 'company_id': company.id}).scalar_one()
    data = raw if isinstance(raw, dict) else json.loads(raw)
    for item in data['histogram']:
        item['status'] = Message.status_display(item['status'])
    return data


@router.get('/{method}/{id:int}/')
def message_details(
    method: SendMethod,
    id: int,
    user_session=Depends(UserSession),
    db: DBSession = Depends(get_db),
    safe: bool = True,
):
    company = _get_or_create_company(db, user_session.company)
    m = db.exec(
        select(*MESSAGE_COLUMNS).where(
            Message.company_id == company.id,
            Message.method == method.value,
            Message.id == id,
        )
    ).first()
    if not m:
        raise HTTP404('message not found')

    msg = _row_to_message(m)

    events = db.exec(
        select(Event.status, Event.message_id, Event.ts, Event.extra)
        .where(Event.message_id == id)
        .order_by(Event.id)
        .limit(51)
    ).all()
    events_data = [Event(**dict(zip(('status', 'message_id', 'ts', 'extra'), e))).parsed_details for e in events[:50]]
    if len(events) > 50:
        extra = db.exec(select(func.count()).select_from(Event).where(Event.message_id == id)).first() - 50
        events_data.append(
            dict(
                status=f'{extra} more',
                datetime=None,
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2)),
            )
        )
    body = msg.body or ''
    if safe:
        body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)
    return dict(**msg.parsed_details, events=events_data, body=body, attachments=list(msg.get_attachments()))
