import json
import logging
from buildpg.asyncpg import BuildPgConnection
from fastapi import APIRouter, Depends
from foxglove import glove
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpBadRequest, HttpConflict, HttpNotFound
from foxglove.route_class import SafeAPIRoute
from httpx import Response
from starlette.responses import JSONResponse
from typing import Optional

from src.schemas.messages import SendMethod, SubaccountModel
from src.utils import AdminAuth

logger = logging.getLogger('views.subaccounts')
app = APIRouter(route_class=SafeAPIRoute, dependencies=[Depends(AdminAuth)])


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
async def delete_subaccount(method: SendMethod, m: SubaccountModel, conn: BuildPgConnection = Depends(get_db)):
    """
    Delete an existing subaccount with Mandrill
    """
    result = await conn.fetch("select id from companies where code like $1 || '%'", m.company_code)
    m_count, g_count = '0', '0'
    company_branches = [str(r['id']) for r in result]
    if company_branches:
        m_count = await conn.execute_b('delete from messages where company_id in (%s)' % ','.join(company_branches))
        g_count = await conn.execute_b(
            'delete from message_groups where company_id in (%s)' % ','.join(company_branches)
        )
        await conn.execute_b('delete from companies import where id in (%s)' % ','.join(company_branches))
    msg = f'deleted_messages={m_count.replace("DELETE ", "")} deleted_message_groups={g_count.replace("DELETE ", "")}'
    logger.info('deleting company=%s %s', m.company_name, msg)

    if method == SendMethod.email_mandrill:
        r = await glove.mandrill.post(
            'subaccounts/delete.json', allowed_statuses=(200, 500), id=m.company_code, timeout_=12
        )
        data = r.json()
        if data.get('name') == 'Unknown_Subaccount':
            raise HttpNotFound(data.get('message', 'sub-account not found'))
        elif r.status_code != 200:
            raise HttpBadRequest(f'error from mandrill: {json.dumps(data, indent=2)}')
    return {'message': msg}
