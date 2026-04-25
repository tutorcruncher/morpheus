import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import delete, func
from sqlmodel import select

from app.common.api.errors import HTTP400, HTTP404, HTTP409
from app.common.auth import AdminAuth
from app.core.database import DBSession, get_db
from app.ext.clients import Mandrill
from app.messages.models import Company, Message, MessageGroup, SendMethod
from app.messages.schemas import SubaccountModel

logger = logging.getLogger('views.subaccounts')
router = APIRouter(dependencies=[Depends(AdminAuth)])


@router.post('/create-subaccount/{method}/')
def create_subaccount(method: SendMethod, m: Optional[SubaccountModel] = None):
    if method != SendMethod.email_mandrill:
        return JSONResponse({'message': f'no subaccount creation required for "{method.value}"'})
    assert m is not None

    r = Mandrill().post(
        'subaccounts/add.json',
        id=m.company_code,
        name=m.company_name,
        allowed_statuses=(200, 500),
        timeout_=12,
    )
    data = r.json()
    if r.status_code == 200:
        return JSONResponse({'message': 'subaccount created'}, status_code=201)

    assert r.status_code == 500, r.status_code
    if f'A subaccount with id {m.company_code} already exists' not in data.get('message', ''):
        return JSONResponse(
            {'message': f'error from mandrill: {json.dumps(data, indent=2)}'},
            status_code=400,
        )

    r = Mandrill().get('subaccounts/info.json', id=m.company_code, timeout_=12)
    data = r.json()
    total_sent = data['sent_total']
    if total_sent > 100:
        raise HTTP409(f'subaccount already exists with {total_sent} emails sent, reuse of subaccount id not permitted')
    return {
        'message': f'subaccount already exists with only {total_sent} emails sent, reuse of subaccount id permitted'
    }


@router.post('/delete-subaccount/{method}/')
def delete_subaccount(method: SendMethod, m: SubaccountModel, db: DBSession = Depends(get_db)):
    """Delete an existing subaccount with Mandrill.

    Deletes companies whose code starts with ``m.company_code`` and lets the FK CASCADE
    chain (companies → message_groups → messages → events/links) wipe their data without
    loading any rows into memory.
    """
    company_ids = db.exec(select(Company.id).where(Company.code.like(m.company_code + '%'))).all()
    m_count = g_count = 0
    if company_ids:
        m_count = db.exec(select(func.count()).select_from(Message).where(Message.company_id.in_(company_ids))).one()
        g_count = db.exec(
            select(func.count()).select_from(MessageGroup).where(MessageGroup.company_id.in_(company_ids))
        ).one()
        # FK CASCADE on messages.company_id and message_groups.company_id wipes child rows.
        db.execute(delete(Company).where(Company.id.in_(company_ids)))
        db.commit()
    msg_summary = f'deleted_messages={m_count} deleted_message_groups={g_count}'
    logger.info('deleting company=%s %s', m.company_name, msg_summary)

    if method == SendMethod.email_mandrill:
        r = Mandrill().post(
            'subaccounts/delete.json',
            allowed_statuses=(200, 500),
            id=m.company_code,
            timeout_=12,
        )
        data = r.json()
        if data.get('name') == 'Unknown_Subaccount':
            raise HTTP404(data.get('message', 'sub-account not found'))
        elif r.status_code != 200:
            raise HTTP400(f'error from mandrill: {json.dumps(data, indent=2)}')
    return {'message': msg_summary}
