import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import select

from app.common.api.errors import HTTP400, HTTP404, HTTP409
from app.common.auth import AdminAuth
from app.core.database import DBSession, get_db
from app.ext.clients import Mandrill
from app.messages.models import Company, SendMethod
from app.messages.schemas import SubaccountModel
from app.messages.tasks import delete_company_messages

logger = logging.getLogger('views.subaccounts')
router = APIRouter(dependencies=[Depends(AdminAuth)])


@router.post('/create-subaccount/{method}/')
def create_subaccount(method: SendMethod, m: Optional[SubaccountModel] = None):
    if method != SendMethod.email_mandrill:
        return JSONResponse({'message': f'no subaccount creation required for "{method.value}"'})
    assert m is not None

    # Mandrill used to return validation errors (including "already exists") as 500; it now
    # returns them as 400 with the detail wrapped in a "Validation error: {...}" message.
    # Accept both so the already-exists check below works across the changeover.
    r = Mandrill().post(
        'subaccounts/add.json',
        id=m.company_code,
        name=m.company_name,
        allowed_statuses=(200, 400, 500),
        timeout_=12,
    )
    data = r.json()
    if r.status_code == 200:
        return JSONResponse({'message': 'subaccount created'}, status_code=201)

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

    Company codes are either the bare subaccount code or ``<code>:<branch_id>``, so match on the
    part before the first colon. A plain prefix match must NOT be used here: it also matches
    companies whose code merely starts with ``m.company_code`` (deleting ``simply-learn`` used to
    wipe ``simply-learning-tuition:7664``'s entire message history).

    The history itself is purged by a worker rather than here. Counting and deleting a large
    agency's messages runs for minutes, well past the 30 seconds Heroku's router holds a request
    open for, so doing it inline tells the caller the delete failed while it goes on to succeed.
    """
    company_ids = db.exec(select(Company.id).where(func.split_part(Company.code, ':', 1) == m.company_code)).all()
    if company_ids:
        delete_company_messages.delay(list(company_ids))
    logger.info('queued deletion of company=%s companies=%s', m.company_name, company_ids)

    if method == SendMethod.email_mandrill:
        # 404 is Mandrill's answer for a subaccount that is already gone; it has to be allowed
        # through for the Unknown_Subaccount branch below to see it.
        r = Mandrill().post(
            'subaccounts/delete.json',
            allowed_statuses=(200, 400, 404, 500),
            id=m.company_code,
            timeout_=12,
        )
        data = r.json()
        if data.get('name') == 'Unknown_Subaccount':
            raise HTTP404(data.get('message', 'sub-account not found'))
        elif r.status_code != 200:
            raise HTTP400(f'error from mandrill: {json.dumps(data, indent=2)}')
    return {'message': f'queued_companies={len(company_ids)}'}
