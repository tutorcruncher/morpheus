import asyncio
import json
import logging
import re
from typing import List, Optional

import Markup
import pytz as pytz
from buildpg import Var, Func
from buildpg.clauses import Select
from fastapi import APIRouter, Depends
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpBadRequest, HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from starlette.requests import Request

from morpheus.app import crud
from morpheus.app.crud import get_sms_spend, get_create_company_id, get_company_id
from morpheus.app.schema import SendMethod
from morpheus.app.utils import Session
from morpheus.app.views.sms import month_interval

logger = logging.getLogger('views.webhooks')
app = APIRouter(route_class=KeepBodyAPIRoute)


class Messages:
    offset = True

    def __init__(self, request, conn):
        self.request = request
        self.conn = conn

    def get_dt_tz(self):
        dt_tz = self.request.query.get('dttz') or 'utc'
        try:
            pytz.timezone(dt_tz)
        except KeyError:
            raise HttpBadRequest(f'unknown timezone: "{dt_tz}"')
        return dt_tz

    def get_date_func(self):
        pretty_ts = bool(self.request.query.get('pretty_ts'))
        return 'pretty_ts' if pretty_ts else 'iso_ts'

    def _select_fields(self):
        tz = self.get_dt_tz()
        date_func = self.get_date_func()
        return [
            Var('m.id').as_('id'),
            Func(date_func, Var('send_ts'), tz).as_('send_ts'),
            Func(date_func, Var('update_ts'), tz).as_('update_ts'),
            'external_id',
            'status',
            'to_first_name',
            'to_last_name',
            'to_user_link',
            'to_address',
            'm.company_id',
            'method',
            'subject',
            'body',
            'tags',
            'attachments',
            'from_name',
            'from_name',
            'cost',
            'extra',
        ]

    async def query(self, *, company_code, message_id=None, tags=None, query=None):
        where = Var('method') == self.request.match_info['method']
        company_id = await get_create_company_id(self.conn, company_code=company_code)
        where &= Var('company_id') == company_id

        if message_id:
            where &= Var('id') == message_id
        elif tags:
            where &= Var('tags').contains(tags)
        elif query:
            return await self.query_general(where, query)

        # count is limited to 10,000 as it speeds up the query massively
        count, items = await asyncio.gather(
            self.conn.fetchval_b(
                """
                select count(*)
                from (select 1 from messages where :where limit 10000) as t
                """,
                where=where,
            ),
            self.conn.fetch_b(
                """
                :select
                from messages m
                join message_groups j on m.group_id = j.id
                where m.id in (
                  select id from messages
                  where :where
                  order by id desc
                  limit 100
                  offset :offset
                )
                order by m.id desc
                """,
                select=Select(self._select_fields()),
                where=where,
                offset=self.get_arg_int('from', 0) if self.offset else 0,
            ),
        )
        return {'count': count, 'items': [dict(r) for r in items]}

    async def query_general(self, where, query):
        async with self.app['pg'].acquire() as conn:
            items = await conn.fetch_b(
                """
                :select
                from messages m
                join message_groups j on m.group_id = j.id
                where m.id in (
                  select id from messages
                  where :where and vector @@ plainto_tsquery(:query)
                  order by id desc
                  limit 100
                  offset :offset
                )
                order by m.id desc
                """,
                select=Select(self._select_fields()),
                tz=self.get_dt_tz(),
                query=query,
                where=where,
                offset=self.get_arg_int('from', 0) if self.offset else 0,
            )
        return {'count': len(items), 'items': [dict(r) for r in items]}


@app.get('/{method}/')
async def messages_list(
    request: Request,
    method: SendMethod,
    tags: Optional[List[str]] = None,
    q: Optional[str] = None,
    conn=Depends(get_db),
    session=Session,
):
    sms_method = 'sms' in method
    messages = Messages(request=request, conn=conn)
    company_code = session.company
    data = await messages.query(tags=tags, query=q, company_code=company_code)
    if sms_method and company_code != '__all__':
        start, end = month_interval()
        data['spend'] = await get_sms_spend(conn, company_code, start, end, method)
    return data


@app.get('/{method}/{id}/')
async def message_details(
    request: Request,
    method: SendMethod,
    id: int,
    session=Session,
    conn=Depends(get_db),
):
    m = crud.get_message(conn, session.company, id)
    if not m:
        raise HttpNotFound('message not found')
    preview_path = request.url_for('preview_message', method=method, id=id)

    extra_count, events = crud.get_message_events(conn, m.id)
    events_data = []
    for event in events[:50]:
        event_data = dict(status=event['status'].title(), datetime=event['ts'])
        if event['extra']:
            event_data['details'] = Markup(json.dumps(json.loads(event['extra']), indent=2))
        events_data.append(event_data)
    if extra_count:
        events_data.append(
            dict(
                status=f'{extra_count} more',
                datetime=None,
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2)),
            )
        )

    return dict(
        base_template='user/base-raw.jinja',
        title=f'{m.method} - {m.external_id}',
        id=m.external_id,
        method=m.method,
        details=m.details,
        events=events_data,
        preview_url=Markup(f'{request.url.scheme}://{request.client.host}/{preview_path}?{request.url.query}'),
        attachments=list(m.get_attachments()),
    )


@app.get('/{method}/{id}/preview/')
async def preview_message(method: SendMethod, id: int, session=Session, conn=Depends(get_db)):
    """
    preview a message
    """
    where = (Var('m.method') == method) & (Var('m.id') == id)
    if session.company != '__all__':
        where &= Var('c.code') == session.company

    data = await conn.fetchrow_b(
        """
        select from_name, to_last_name, to_address, status, body, extra
        from messages m
        join message_groups j on m.group_id = j.id
        join companies c on m.company_id = c.id
        where :where
        """,
        where=where,
    )

    if not data:
        raise HttpNotFound('message not found')

    data = dict(data)
    body = data['body']
    # Remove links from preview
    body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)

    extra = json.loads(data['extra']) if data.get('extra') else {}
    if method.startswith('sms'):
        # need to render the sms so it makes sense to users
        return {
            'from': data['from_name'],
            'to': data['to_last_name'] or data['to_address'],
            'status': data['status'],
            'message': body,
            'extra': extra,
        }
    else:
        return {'raw': body}


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


@app.get('/{method}/aggregation')
async def message_aggregation(method: SendMethod, session=Session, conn=Depends(get_db)):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    where = Var('method') == method
    if session.company != '__all__':
        where &= Var('company_id') == await get_company_id(conn, session.company)
    return await conn.fetchval_b(agg_sql, where=where)
