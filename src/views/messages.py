import json
import re
from fastapi import APIRouter, Depends
from fastapi_pagination import LimitOffsetPage
from fastapi_pagination.ext.sqlalchemy import paginate
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from markupsafe import Markup
from sqlalchemy.exc import NoResultFound
from starlette.requests import Request
from typing import List, Optional

from src import crud
from src.models import Company, Message
from src.schema import MessageListOutModel, SendMethod, Session, UserSession
from src.utils import get_db
from src.views.sms import month_interval

app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(UserSession)])


@app.get('/{method}/')
async def messages_list(
    method: SendMethod,
    tags: Optional[List[str]] = None,
    q: Optional[str] = None,
    p_from: Optional[int] = None,
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
        'items': [m.list_details for m in Message.manager.filter(db, p_from=p_from, limit=100, **kwargs)],
        'count': Message.manager.filter(db, **kwargs).count(),
    }
    if 'sms' in method:
        start, end = month_interval()
        data['spend'] = Message.manager.get_sms_spend(db, company.id, start, end, method)
    return data


@app.get('/{method}/{id}/')
async def message_details(request: Request, method: SendMethod, id: int, session=Session, db=Depends(get_db)):
    try:
        m = Message.get(db, company__code=session.company, id=id)
    except NoResultFound:
        raise HttpNotFound('message not found')
    preview_path = request.url_for('preview_message', method=method, id=id)

    extra_count, events = Message.manager.get_events(db, message_id=m.id)
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
    return MessageOutModel(**m)

    return dict(
        id=m.external_id,
        method=m.method,
        details=m.details,
        status=m.status,
        events=events_data,
        preview_url=Markup(f'{request.url.scheme}://{request.client.host}/{preview_path}?{request.url.query}'),
        attachments=list(m.get_attachments()),
    )


@app.get('/{method}/{id}/preview/')
async def preview_message(method: SendMethod, id: int, session=Session, db=Depends(get_db)):
    """
    preview a message
    """
    m = Message.manager.get(db, company__code=session.company, id=id)
    if not m:
        raise HttpNotFound('message not found')

    body = m.body
    # Remove links from preview
    body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)

    extra = json.loads(m.extra) if m.extra else {}
    if method.startswith('sms'):
        # need to render the sms so it makes sense to users
        return {
            'from': m.from_name,
            'to': m.to_last_name or m.to_address,
            'status': m.status,
            'message': body,
            'extra': extra,
        }
    else:
        return {'raw': body}


@app.get('/{method}/aggregation/')
async def message_aggregation(method: SendMethod, session=Session, db=Depends(get_db)):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    try:
        company = Company.manager.get(db, code=session.company)
    except NoResultFound:
        raise HttpNotFound('company not found')
    return crud.get_messages_aggregated(db, company.id, method)
