import json
import logging
from fastapi import APIRouter, Depends
from foxglove import glove
from foxglove.exceptions import HttpBadRequest, HttpConflict, HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from httpx import Response
from sqlalchemy.exc import NoResultFound
from starlette.responses import JSONResponse
from typing import Optional

from src.models import Company, Message, MessageGroup
from src.schema import SendMethod, SubaccountModel
from src.utils import AdminAuth, get_db

logger = logging.getLogger('views.subaccounts')
app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(AdminAuth)])


@app.post('/create-subaccount/{method}/')
async def create_subaccount(method: SendMethod, m: Optional[SubaccountModel] = None):
    if method != SendMethod.email_mandrill:
        return JSONResponse({'message': f'no subaccount creation required for "{method}"'})
    assert m

    r: Response = await glove.mandrill.post(
        'subaccounts/add.json', id=m.company_code, name=m.company_name, allowed_statuses=(200, 500), timeout_=12
    )
    data = r.json()
    if r.status_code == 200:
        return JSONResponse({'message': 'subaccount created'}, status_code=201)

    assert r.status_code == 500, r.status_code
    if f'A subaccount with id {m.company_code} already exists' not in data.get('message', ''):
        return JSONResponse({'message': f'error from mandrill: {json.dumps(data, indent=2)}'}, status_code=400)

    r = await glove.mandrill.get('subaccounts/info.json', id=m.company_code, timeout_=12)
    data = r.json()
    total_sent = data['sent_total']
    if total_sent > 100:
        raise HttpConflict(
            f'subaccount already exists with {total_sent} emails sent, reuse of subaccount id not permitted'
        )
    else:
        return {
            'message': f'subaccount already exists with only {total_sent} emails sent, reuse of subaccount id permitted'
        }


@app.post('/delete-subaccount/{method}/')
async def delete_subaccount(method: SendMethod, m: Optional[SubaccountModel] = None, conn=Depends(get_db)):
    """
    Delete an existing subaccount with mandrill
    """
    if method != SendMethod.email_mandrill:
        return {'message': f'no subaccount deletion required for "{method}"'}
    assert m

    r = await glove.mandrill.post(
        'subaccounts/delete.json', allowed_statuses=(200, 500), id=m.company_code, timeout_=12
    )
    data = r.json()
    if r.status_code == 200:
        m_count, g_count = 0, 0
        try:
            company = Company.manager.get(conn, code=m.company_code)
        except NoResultFound:
            pass
        else:
            m_count = Message.manager.delete(conn, company_id=company.id)
            g_count = MessageGroup.manager.delete(conn, company_id=company.id)
            Company.manager.delete(conn, id=company.id)
        msg = f'deleted_messages={m_count} deleted_message_groups={g_count}'
        logger.info('deleting company=%s %s', m.company_name, msg)
        return {'message': msg}
    if data.get('name') == 'Unknown_Subaccount':
        raise HttpNotFound(data.get('message', 'sub-account not found'))

    assert r.status_code == 500, r.status_code
    raise HttpBadRequest(f'error from mandrill: {json.dumps(data, indent=2)}')
