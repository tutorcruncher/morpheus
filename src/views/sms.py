from dataclasses import asdict

import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Body, Depends
from foxglove import glove
from foxglove.exceptions import HttpConflict, HttpNotFound
from foxglove.route_class import KeepBodyAPIRoute
from sqlalchemy.exc import NoResultFound
from starlette.responses import JSONResponse
from typing import Tuple

from src.models import Company, Message, MessageGroup
from src.schema import SmsNumbersModel, SmsSendMethod, SmsSendModel
from src.utils import AdminAuth, get_db
from src.worker import validate_number

logger = logging.getLogger('views.sms')
app = APIRouter(route_class=KeepBodyAPIRoute, dependencies=[Depends(AdminAuth)])


@app.get('/billing/{method}/{company_code}/')
async def sms_billing_view(company_code: str, method: SmsSendMethod, data: dict = Body(None), conn=Depends(get_db)):
    try:
        company = Company.manager.get(conn, code=company_code)
    except NoResultFound:
        raise HttpNotFound('company not found')
    start = datetime.strptime(data['start'], '%Y-%m-%d')
    end = datetime.strptime(data['end'], '%Y-%m-%d')
    total_spend = Message.manager.get_sms_spend(conn, company.id, start, end, method)
    return {
        'company': company.code,
        'start': start.strftime('%Y-%m-%d'),
        'end': end.strftime('%Y-%m-%d'),
        'spend': total_spend,
    }


def month_interval() -> Tuple[datetime, datetime]:
    n = datetime.utcnow().replace(tzinfo=timezone.utc)
    return n.replace(day=1, hour=0, minute=0, second=0, microsecond=0), n


@app.post('/send/sms/')
async def send_sms(m: SmsSendModel, conn=Depends(get_db)):
    group_key = f'group:{m.uid}'
    v = await glove.redis.incr(group_key)
    if v > 1:
        raise HttpConflict(f'Send group with id "{m.uid}" already exists\n')
    await glove.redis.expire(group_key, 86400)

    month_spend = None
    company = Company.manager.get_or_create(conn, code=m.company_code)
    if m.cost_limit is not None:
        start, end = month_interval()
        month_spend = Message.manager.get_sms_spend(conn, company.id, start, end, m.method) or 0
        if month_spend >= m.cost_limit:
            return JSONResponse(
                content={'status': 'send limit exceeded', 'cost_limit': m.cost_limit, 'spend': month_spend},
                status_code=402,
            )
    message_group = MessageGroup.manager.create(
        conn, uuid=m.uid, company_id=company.id, message_method=m.method, from_name=m.from_name
    )
    logger.info('%s sending %d SMSs', company.id, len(m.recipients))

    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_sms', message_group.id, company.id, recipient, m_base)

    return JSONResponse(content={'status': 'enqueued', 'spend': month_spend}, status_code=201)


def _to_dict(v):
    return v and asdict(v)


@app.get('/validate/sms/')
async def validate_sms(m: SmsNumbersModel):
    return {str(k): _to_dict(validate_number(n, m.country_code)) for k, n in m.numbers.items()}