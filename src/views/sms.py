from dataclasses import asdict

import logging
from buildpg import Values
from buildpg.asyncpg import BuildPgConnection
from datetime import datetime, timezone
from fastapi import APIRouter, Body, Depends
from foxglove import glove
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpConflict, HttpNotFound
from foxglove.route_class import SafeAPIRoute
from starlette.responses import JSONResponse
from typing import Tuple

from src.schemas.messages import SmsNumbersModel, SmsSendMethod, SmsSendModel
from src.utils import AdminAuth
from src.views.utils import get_or_create_company, get_sms_spend
from src.worker.sms import validate_number

logger = logging.getLogger('views.sms')
app = APIRouter(route_class=SafeAPIRoute, dependencies=[Depends(AdminAuth)])


@app.get('/billing/{method}/{company_code}/')
async def sms_billing_view(
    company_code: str, method: SmsSendMethod, data: dict = Body(None), conn: BuildPgConnection = Depends(get_db)
):
    company_id = await conn.fetchval_b('select id from companies where code = :code', code=company_code)
    if not company_id:
        raise HttpNotFound('company not found')
    start = datetime.strptime(data['start'], '%Y-%m-%d')
    end = datetime.strptime(data['end'], '%Y-%m-%d')
    spend = await get_sms_spend(conn, company_id=company_id, method=method, start=start, end=end)
    return {
        'company': company_code,
        'start': start.strftime('%Y-%m-%d'),
        'end': end.strftime('%Y-%m-%d'),
        'spend': spend or 0,
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
    company_id = await get_or_create_company(conn, m.company_code)
    if m.cost_limit is not None:
        start, end = month_interval()
        month_spend = await get_sms_spend(conn, company_id=company_id, start=start, end=end, method=m.method) or 0
        if month_spend >= m.cost_limit:
            return JSONResponse(
                content={'status': 'send limit exceeded', 'cost_limit': m.cost_limit, 'spend': month_spend},
                status_code=402,
            )
    group_id = await conn.fetchval_b(
        'insert into message_groups (:values__names) values :values returning id',
        values=Values(uuid=m.uid, company_id=company_id, message_method=m.method, from_name=m.from_name),
    )
    logger.info('%s sending %d SMSs', company_id, len(m.recipients))

    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_sms', group_id, company_id, recipient, m_base)

    return JSONResponse(content={'status': 'enqueued', 'spend': month_spend}, status_code=201)


def _to_dict(v):
    return v and asdict(v)


@app.get('/validate/sms/')
async def validate_sms(m: SmsNumbersModel):
    return {str(k): _to_dict(validate_number(n, m.country_code)) for k, n in m.numbers.items()}
