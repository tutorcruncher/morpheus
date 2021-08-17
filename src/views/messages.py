import json
from fastapi import APIRouter, Depends, Query
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from markupsafe import Markup
from sqlalchemy.exc import NoResultFound
from starlette.requests import Request
from typing import List, Optional

from src import crud
from src.models import Company, Message
from src.schema import SendMethod, UserSession
from src.utils import get_db
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
    db=Depends(get_db),
    user_session=Depends(UserSession),
):
    company = Company.manager.get_or_create(db, code=user_session.company)
    # We get the total count, and the list limited by pagination.
    kwargs = dict(company_id=company.id, tags=tags, q=q, method=method)
    full_count = Message.manager.filter(db, **kwargs).count()
    items = [m.list_details for m in Message.manager.filter(db, offset=offset, limit=LIST_PAGE_SIZE, **kwargs)]
    data = {'items': items, 'count': full_count}
    this_url = request.url_for('messages_list', method=method.value)
    if (offset + len(items)) < full_count:
        data['next'] = f"{this_url}?offset={offset + len(items)}"
    if offset:
        data['previous'] = f"{this_url}?offset={max(offset - LIST_PAGE_SIZE, 0)}"
    if 'sms' in method:
        start, end = month_interval()
        data['spend'] = Message.manager.get_sms_spend(db, company.id, start, end, method)
    return data


@app.get('/{method}/aggregation/')
async def message_aggregation(method: SendMethod, user_session=Depends(UserSession), db=Depends(get_db)):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    company = Company.manager.get_or_create(db, code=user_session.company)
    data = crud.get_messages_aggregated(db, company.id, method)
    for item in data['histogram']:
        item['status'] = Message.status_display(item['status'])
    return data


@app.get('/{method}/{id}/')
async def message_details(method: SendMethod, id: int, user_session=Depends(UserSession), db=Depends(get_db)):
    company = Company.manager.get_or_create(db, code=user_session.company)
    try:
        m = Message.manager.get(db, company_id=company.id, id=id, method=method)
    except NoResultFound:
        raise HttpNotFound('message not found')

    extra_count, events = Message.manager.get_events(db, message_id=m.id)
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
    return dict(
        **m.list_details,
        events=events_data,
        body=m.body,
        attachments=list(m.get_attachments()),
    )
