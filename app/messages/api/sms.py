import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import select

from app.common.api.errors import HTTP400, HTTP404, HTTP409
from app.common.auth import AdminAuth
from app.core.database import DBSession, get_db
from app.messages.models import Company, Message, MessageGroup, SmsSendMethod
from app.messages.schemas import SmsNumbersModel, SmsSendModel
from app.messages.tasks import get_redis, send_sms, validate_number

logger = logging.getLogger('views.sms')
router = APIRouter(dependencies=[Depends(AdminAuth)])


def _get_or_create_company(db: DBSession, company_code: str) -> Company:
    company, _ = db.get_or_create(Company, code=company_code)
    return company


def _get_sms_spend(db: DBSession, *, company_id: int, method: str, start: datetime, end: datetime) -> Optional[float]:
    return db.exec(
        select(func.sum(Message.cost)).where(
            Message.method == method,
            Message.company_id == company_id,
            start <= Message.send_ts,
            Message.send_ts < end,
        )
    ).first()


@router.get('/billing/{method}/{company_code}/')
def sms_billing_view(
    company_code: str,
    method: SmsSendMethod,
    data: dict = Body(None),
    db: DBSession = Depends(get_db),
):
    company = db.exec(select(Company).where(Company.code == company_code)).first()
    if not company:
        raise HTTP404('company not found')
    if not data or 'start' not in data or 'end' not in data:
        raise HTTP400('request body must include "start" and "end" dates')
    start = datetime.strptime(data['start'], '%Y-%m-%d')
    end = datetime.strptime(data['end'], '%Y-%m-%d')
    spend = _get_sms_spend(db, company_id=company.id, method=method.value, start=start, end=end)  # ty:ignore[invalid-argument-type]
    return {
        'company': company_code,
        'start': start.strftime('%Y-%m-%d'),
        'end': end.strftime('%Y-%m-%d'),
        'spend': spend or 0,
    }


def month_interval() -> tuple[datetime, datetime]:
    n = datetime.utcnow().replace(tzinfo=timezone.utc)  # ty:ignore[deprecated]
    return n.replace(day=1, hour=0, minute=0, second=0, microsecond=0), n


@router.post('/send/sms/')
def send_sms_view(m: SmsSendModel, db: DBSession = Depends(get_db)):
    redis = get_redis()
    group_key = f'group:{m.uid}'
    if not redis.set(group_key, '1', ex=86400, nx=True):
        raise HTTP409(f'Send group with id "{m.uid}" already exists\n')

    month_spend = None
    company = _get_or_create_company(db, m.company_code)
    if m.cost_limit is not None:
        start, end = month_interval()
        month_spend = _get_sms_spend(db, company_id=company.id, start=start, end=end, method=m.method.value) or 0  # ty:ignore[invalid-argument-type]
        if month_spend >= m.cost_limit:
            return JSONResponse(
                content={'status': 'send limit exceeded', 'cost_limit': m.cost_limit, 'spend': month_spend},
                status_code=402,
            )
    group = MessageGroup(
        uuid=m.uid,  # ty:ignore[invalid-argument-type]
        company_id=company.id,  # ty:ignore[invalid-argument-type]
        message_method=m.method.value,
        from_name=m.from_name,
    )
    db.add(group)
    db.commit()
    db.refresh(group)
    logger.info('%s sending %d SMSs', company.id, len(m.recipients))

    recipients = m.recipients
    m_base = m.model_copy(update={'recipients': []}).model_dump(mode='json')
    for recipient in recipients:
        send_sms.delay(group.id, company.id, recipient.model_dump(mode='json'), m_base)

    return JSONResponse(content={'status': 'enqueued', 'spend': month_spend}, status_code=201)


def _to_dict(v):
    return v and asdict(v)


@router.get('/validate/sms/')
def validate_sms_view(m: SmsNumbersModel):
    return {str(k): _to_dict(validate_number(n, m.country_code)) for k, n in m.numbers.items()}
