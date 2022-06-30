import json
import re
from buildpg import V, logic
from buildpg.asyncpg import BuildPgConnection
from buildpg.clauses import Select
from fastapi import APIRouter, Depends, Query
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from markupsafe import Markup
from starlette.requests import Request
from typing import List, Optional

from src.schemas.messages import SendMethod
from src.schemas.models import Event, Message
from src.schemas.session import UserSession
from src.views.sms import month_interval
from src.views.utils import get_or_create_company, get_sms_spend

app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(UserSession)])


LIST_PAGE_SIZE = 100

MESSAGE_SELECT = Select(
    [
        V('id'),
        V('external_id'),
        V('to_user_link'),
        V('to_address'),
        V('to_first_name'),
        V('to_last_name'),
        V('send_ts'),
        V('update_ts'),
        V('subject'),
        V('body'),
        V('status'),
        V('method'),
        V('attachments'),
        V('cost'),
    ]
)

max_length = 100
re_null = re.compile('\x00')
# characters that cause syntax errors in to_tsquery and/or should be used to split
pg_tsquery_split = ''.join((':', '&', '|', '%', '"', "'", '<', '>', '!', '*', '(', ')', r'\s'))
re_tsquery = re.compile(f'[^{pg_tsquery_split}]{{2,}}')


def prepare_search_query(raw_query: Optional[str]) -> Optional[str]:
    if raw_query is None:
        return None

    query = re_null.sub('', raw_query.lower())[:max_length]

    words = re_tsquery.findall(query)
    if not words:
        return None

    # just using a "foo & bar:*"
    return ' & '.join(words) + ':*'


@app.get('/{method}/')
async def messages_list(
    request: Request,
    method: SendMethod,
    tags: Optional[List[str]] = Query(None),
    q: Optional[str] = None,
    offset: Optional[int] = 0,
    conn: BuildPgConnection = Depends(get_db),
    user_session=Depends(UserSession),
):
    company_id = await get_or_create_company(conn, user_session.company)
    # We get the total count, and the list limited by pagination.
    where = (V('method') == method) & (V('company_id') == company_id)
    if tags:
        where &= V('tags').contains(tags)
    if q:
        where &= V('vector').matches(logic.Func('plainto_tsquery', q.strip()))
    full_count = await conn.fetchval_b(
        'select count(*) from (select 1 from messages where :where limit 10000) as t', where=where
    )
    items = await conn.fetch_b(
        ':select from messages where :where order by id desc limit :limit offset :offset',
        where=where,
        select=MESSAGE_SELECT,
        limit=LIST_PAGE_SIZE,
        offset=offset or 0,
    )
    data = {'items': [Message(**m).parsed_details for m in items], 'count': full_count}
    this_url = request.url_for('messages_list', method=method.value)
    if (offset + len(items)) < full_count:
        data['next'] = f"{this_url}?offset={offset + len(items)}"
    if offset:
        data['previous'] = f"{this_url}?offset={max(offset - LIST_PAGE_SIZE, 0)}"
    if 'sms' in method:
        start, end = month_interval()
        data['spend'] = await get_sms_spend(conn, company_id=company_id, start=start, end=end, method=method)
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
    where :where and date > current_timestamp::date - '28 days'::interval
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
  where :where
) as agg
"""


@app.get('/{method}/aggregation/')
async def message_aggregation(
    method: SendMethod, user_session=Depends(UserSession), conn: BuildPgConnection = Depends(get_db)
):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    company_id = await get_or_create_company(conn, user_session.company)
    data = await conn.fetchval_b(agg_sql, where=(V('method') == method) & (V('company_id') == company_id))
    data = json.loads(data)
    for item in data['histogram']:
        item['status'] = Message.status_display(item['status'])
    return data


@app.get('/{method}/{id:int}/')
async def message_details(
    method: SendMethod,
    id: int,
    user_session=Depends(UserSession),
    conn: BuildPgConnection = Depends(get_db),
    safe: bool = True,
):
    company_id = await get_or_create_company(conn, user_session.company)
    m = await conn.fetchrow_b(
        ':select from messages where :where',
        select=MESSAGE_SELECT,
        where=(V('company_id') == company_id) & (V('method') == method) & (V('id') == id),
    )
    if not m:
        raise HttpNotFound('message not found')

    m = Message(**m)
    events = await conn.fetch_b(
        'select status, message_id, ts, extra from events where :where order by id limit 51',
        where=V('message_id') == id,
    )
    events_data = [Event(**e).parsed_details for e in events[:50]]
    if len(events) > 50:
        extra = await conn.fetchval_b('select count(*) - 50 from events where :where', where=V('message_id') == id)
        events_data.append(
            dict(
                status=f'{extra} more',
                datetime=None,
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2)),
            )
        )
    body = m.body
    if safe:
        body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)
    return dict(**m.parsed_details, events=events_data, body=body, attachments=list(m.get_attachments()))
