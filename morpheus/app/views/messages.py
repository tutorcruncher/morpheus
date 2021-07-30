import json
import re
from typing import List, Optional

from markupsafe import Markup
from fastapi import APIRouter, Depends
from foxglove.exceptions import HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from starlette.requests import Request

from morpheus.app import crud
from morpheus.app.schema import SendMethod, Session
from morpheus.app.utils import get_db
from morpheus.app.views.sms import month_interval

app = APIRouter(route_class=KeepBodyAPIRoute)


@app.get('/{method}/')
async def messages_list(
    method: SendMethod,
    tags: Optional[List[str]] = None,
    q: Optional[str] = None,
    p_from: Optional[int] = None,
    conn=Depends(get_db),
    session=Session,
):
    sms_method = 'sms' in method
    company_id = crud.get_company_id(conn, session.company)
    data = {'messages': crud.get_messages(conn, company_id, tags=tags, q=q, p_from=p_from)}
    if sms_method:
        start, end = month_interval()
        data['spend'] = crud.get_sms_spend(conn, company_id, start, end, method)
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
    m = crud.get_message(conn, session.company, id)
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
async def message_aggregation(method: SendMethod, session=Session, conn=Depends(get_db)):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    company_id = crud.get_company_id(conn, session.company)
    return crud.get_messages_aggregated(conn, company_id, method)
