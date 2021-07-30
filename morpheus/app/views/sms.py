import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Tuple

from fastapi import APIRouter, Depends
from foxglove import glove
from foxglove.exceptions import HttpConflict
from foxglove.route_class import KeepBodyAPIRoute
from starlette.responses import JSONResponse

from morpheus.app import crud
from morpheus.app.models import MessageGroup
from morpheus.app.schema import SmsSendModel, SmsNumbersModel, SmsSendMethod, Session
from morpheus.app.utils import get_db
from morpheus.app.worker import validate_number


logger = logging.getLogger('views.sms')
app = APIRouter(route_class=KeepBodyAPIRoute)


@app.get('/billing/{method}/{company_code}/')
async def sms_billing_view(
    session: Session, method: SmsSendMethod, start: datetime, end: datetime, conn=Depends(get_db)
):
    company_id = crud.get_company_id(conn, session.company)
    total_spend = crud.get_sms_spend(conn, company_id, start, end, method)
    return {
        'company': session.company,
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
    company_id = crud.get_create_company_id(conn, m.company_code)
    if m.cost_limit is not None:
        start, end = month_interval()
        month_spend = crud.get_sms_spend(conn, company_id, start, end, m.method)
        if month_spend >= m.cost_limit:
            return JSONResponse(
                content={'status': 'send limit exceeded', 'cost_limit': m.cost_limit, 'spend': month_spend},
                status_code=402,
            )
    message_group = MessageGroup(uuid=m.uid, company_id=company_id, message_method=m.method, from_name=m.from_name)
    message_group = crud.create_message_group(conn, message_group)
    logger.info('%s sending %d SMSs', company_id, len(m.recipients))

    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_sms', message_group.id, company_id, recipient, m_base)

    return JSONResponse(content={'status': 'enqueued', 'spend': month_spend}, status_code=201)


def _to_dict(v):
    return v and asdict(v)


@app.get('/validate/sms/')
async def validate_sms(m: SmsNumbersModel):
    return {str(k): _to_dict(validate_number(n, m.country_code)) for k, n in m.numbers.items()}
