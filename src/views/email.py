import logging
from fastapi import APIRouter, Body, Depends
from foxglove import glove
from foxglove.exceptions import HttpConflict
from foxglove.route_class import KeepBodyAPIRoute
from starlette.responses import JSONResponse

from src.models import Company, MessageGroup
from src.schema import EmailSendModel
from src.utils import get_db

logger = logging.getLogger('views.email')
app = APIRouter(route_class=KeepBodyAPIRoute)


@app.post('/send/email/')
async def email_send_view(m: EmailSendModel = Body(None), conn=Depends(get_db)):
    group_key = f'group:{m.uid}'
    v = await glove.redis.incr(group_key)
    if v > 1:
        raise HttpConflict(f'Send group with id "{m.uid}" already exists\n')
    await glove.redis.expire(group_key, 86400)

    logger.info('sending %d emails (group %s) via %s for %s', len(m.recipients), m.uid, m.method, m.company_code)
    company = Company.manager.get_or_create(conn, code=m.company_code)

    message_group = MessageGroup.manager.create(
        conn,
        uuid=str(m.uid),
        company_id=company.id,
        message_method=m.method,
        from_email=m.from_address.email,
        from_name=m.from_address.name,
    )
    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_email', message_group.id, company.id, recipient, m_base)
    return JSONResponse({'message': '201 job enqueued'}, status_code=201)
