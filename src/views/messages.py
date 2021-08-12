import json
from fastapi import APIRouter, Depends, Query
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from markupsafe import Markup
from sqlalchemy.exc import NoResultFound
from typing import List, Optional

from src import crud
from src.models import Company, Message
from src.schema import SendMethod, UserSession
from src.utils import get_db
from src.views.sms import month_interval

app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(UserSession)])


@app.get('/{method}/')
async def messages_list(
    method: SendMethod,
    tags: Optional[List[str]] = Query(None),
    q: Optional[str] = None,
    offset: Optional[int] = None,
    db=Depends(get_db),
    user_session=Depends(UserSession),
):
    try:
        company = Company.manager.get(db, code=user_session.company)
    except NoResultFound:
        raise HttpNotFound('company not found')
    # We get the total count, and the list limited by pagination.
    kwargs = dict(company_id=company.id, tags=tags, q=q, method=method)
    data = {
        'items': [m.list_details for m in Message.manager.filter(db, offset=offset, limit=100, **kwargs)],
        'count': Message.manager.filter(db, **kwargs).count(),
    }
    if 'sms' in method:
        start, end = month_interval()
        data['spend'] = Message.manager.get_sms_spend(db, company.id, start, end, method)
    return data


@app.get('/{method}/aggregation/')
async def message_aggregation(method: SendMethod, user_session=Depends(UserSession), db=Depends(get_db)):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    try:
        company = Company.manager.get(db, code=user_session.company)
    except NoResultFound:
        raise HttpNotFound('company not found')
    return crud.get_messages_aggregated(db, company.id, method)


@app.get('/{method}/{id}/')
async def message_details(method: SendMethod, id: int, user_session=Depends(UserSession), db=Depends(get_db)):
    try:
        company = Company.manager.get(db, code=user_session.company)
    except NoResultFound:
        raise HttpNotFound('company not found')
    try:
        m = Message.manager.get(db, company_id=company.id, id=id, method=method)
    except NoResultFound:
        raise HttpNotFound('message not found')

    extra_count, events = Message.manager.get_events(db, message_id=m.id)
    events_data = []
    for event in events[:50]:
        event_data = dict(status=event.status.title(), datetime=event.ts)
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
        attachments=list(m.get_attachments()),
    )
