import json
import logging

from fastapi import APIRouter, Depends
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpConflict, HttpNotFound, HttpBadRequest
from foxglove.route_class import KeepBodyAPIRoute
from starlette.responses import JSONResponse
from starlette.templating import Jinja2Templates

from morpheus.app.ext import Mandrill
from morpheus.app.schema import SendMethod, SubaccountModel
from morpheus.app.settings import Settings


logger = logging.getLogger('views.common')
app = APIRouter(route_class=KeepBodyAPIRoute)
templates = Jinja2Templates(directory='templates/')


@app.post('/create-subaccount/{method}/')
async def create_subaccount(method: SendMethod, m: SubaccountModel):
    if method != SendMethod.email_mandrill:
        return JSONResponse(f'no subaccount creation required for "{method}"\n')

    mandrill = Mandrill(Settings())

    r = await mandrill.post(
        'subaccounts/add.json', id=m.company_code, name=m.company_name, allowed_statuses=(200, 500), timeout_=12
    )
    data = await r.json()
    if r.status == 200:
        return JSONResponse('subaccount created\n', status_code=201)

    assert r.status == 500, r.status
    if f'A subaccount with id {m.company_code} already exists' not in data.get('message', ''):
        return JSONResponse(f'error from mandrill: {json.dumps(data, indent=2)}\n', status_code=400)

    r = await mandrill.get('subaccounts/info.json', id=m.company_code, timeout_=12)
    data = await r.json()
    total_sent = data['sent_total']
    if total_sent > 100:
        raise HttpConflict(
            f'subaccount already exists with {total_sent} emails sent, reuse of subaccount id not permitted\n'
        )
    else:
        return f'subaccount already exists with only {total_sent} emails sent, reuse of subaccount id permitted\n'


@app.post('/delete-subaccount/{method}/')
async def delete_subaccount(method: SendMethod, m: SubaccountModel, conn=Depends(get_db)):
    """
    Delete an existing subaccount with mandrill
    """
    if method != SendMethod.email_mandrill:
        return f'no subaccount deletion required for "{method}"\n'

    mandrill = Mandrill(Settings())

    r = await mandrill.post('subaccounts/delete.json', allowed_statuses=(200, 500), id=m.company_code, timeout_=12)
    data = await r.json()
    if r.status == 200:
        company_id = await conn.fetchval('select id from companies where code=$1', m.company_code)
        if company_id:
            async with conn.transaction() as tr:
                del_messages_resp = await tr.execute('delete from messages where company_id=$1', company_id)
                del_groups_resp = await tr.execute('delete from message_groups where company_id=$1', company_id)
                await tr.execute('delete from companies where id=$1', company_id)
            del_messages_count = int(del_messages_resp.replace('DELETE ', ''))
            del_groups_count = int(del_groups_resp.replace('DELETE ', ''))
        else:
            del_messages_count = del_groups_count = 0
        msg = f'deleted_messages={del_messages_count} deleted_message_groups={del_groups_count}'
        logger.info('deleting company=%s %s', m.company_name, msg)
        return msg + '\n'

    if data.get('name') == 'Unknown_Subaccount':
        raise HttpNotFound(data.get('message', 'sub-account not found') + '\n')

    assert r.status == 500, r.status
    return HttpBadRequest(f'error from mandrill: {json.dumps(data, indent=2)}\n')
