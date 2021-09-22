import json
import re
from fastapi import APIRouter, Depends, Query
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from markupsafe import Markup
from sqlalchemy import func
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from typing import List, Optional

from src import crud
from src.db import get_session
from src.models import Company, Message
from src.schema import SendMethod, UserSession
from src.views.sms import month_interval

app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(UserSession)])


LIST_PAGE_SIZE = 100


@app.get('/{method}/')
async def messages_list(
    request: Request,
    method: SendMethod,
    tags: Optional[List[str]] = Query(None),
    q: Optional[str] = None,
    offset: Optional[int] = 0,
    conn: AsyncSession = Depends(get_session),
    user_session=Depends(UserSession),
):
    company = await Company.manager(conn).get_or_create(code=user_session.company)
    # We get the total count, and the list limited by pagination.
    filter_args = [Message.company_id == company.id, Message.method == method]
    if tags:
        filter_args.append(Message.tags.contains(tags))
    if q:
        filter_args.append(Message.vector.op('@@')(func.plainto_tsquery(q.strip())))
    full_count = await Message.manager(conn).count(*filter_args)
    query = Message.manager(conn).filter(*filter_args).order_by(Message.id.desc())
    if offset:
        query = query.offset(offset)
    items = [m[0].list_details for m in (await conn.execute(query.limit(LIST_PAGE_SIZE))).all()]
    data = {'items': items, 'count': full_count}
    this_url = request.url_for('messages_list', method=method.value)
    if (offset + len(items)) < full_count:
        data['next'] = f"{this_url}?offset={offset + len(items)}"
    if offset:
        data['previous'] = f"{this_url}?offset={max(offset - LIST_PAGE_SIZE, 0)}"
    if 'sms' in method:
        start, end = month_interval()
        data['spend'] = await Message.manager(conn).get_sms_spend(company.id, start, end, method)
    return data


@app.get('/{method}/aggregation/')
async def message_aggregation(
    method: SendMethod, user_session=Depends(UserSession), conn: AsyncSession = Depends(get_session)
):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    company = await Company.manager(conn).get_or_create(code=user_session.company)
    data = await crud.get_messages_aggregated(conn, company.id, method)
    for item in data['histogram']:
        item['status'] = Message.status_display(item['status'])
    return data


@app.get('/{method}/{id:int}/')
async def message_details(
    method: SendMethod,
    id: int,
    user_session=Depends(UserSession),
    conn: AsyncSession = Depends(get_session),
    safe: bool = True,
):
    company = Company.manager(conn).get_or_create(code=user_session.company)
    try:
        m = await Message.manager(conn).get(company_id=company.id, id=id, method=method)
    except NoResultFound:
        raise HttpNotFound('message not found')

    extra_count, events = Message.manager(conn).get_events(message_id=m.id)
    events_data = []
    for event in events[:50]:
        event_data = dict(status=event.get_status_display(), datetime=event.ts)
        if event.extra:
            event_data['details'] = Markup(json.dumps(json.loads(event.extra), indent=2))
        events_data.append(event_data)
    if extra_count:
        events_data.append(
            dict(
                status=f'{extra_count} more',
                datetime=None,
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2)),
            )
        )
    body = m.body
    if safe:
        body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)
    return dict(**m.list_details, events=events_data, body=body, attachments=list(m.get_attachments()))
