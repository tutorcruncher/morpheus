import logging

from fastapi import APIRouter, Depends
from foxglove import glove
from foxglove.exceptions import HttpConflict
from foxglove.route_class import KeepBodyAPIRoute
from starlette.responses import JSONResponse

from morpheus.app.models import MessageGroup
from morpheus.app.schema import EmailSendModel
from morpheus.app import crud
from morpheus.app.utils import get_db

logger = logging.getLogger('views.email')
app = APIRouter(route_class=KeepBodyAPIRoute)


@app.post('/send/email/')
async def email_send_view(m: EmailSendModel, conn=Depends(get_db)):
    group_key = f'group:{m.uid}'
    v = await glove.redis.incr(group_key)
    if v > 1:
        raise HttpConflict(f'Send group with id "{m.uid}" already exists\n')
    await glove.redis.expire(group_key, 86400)

    logger.info('sending %d emails (group %s) via %s for %s', len(m.recipients), m.uid, m.method, m.company_code)
    company_id = crud.get_create_company_id(conn, m.company_code)

    message_group = MessageGroup(
        uuid=m.uid,
        company_id=company_id,
        message_method=m.method,
        from_email=m.from_address.email,
        from_name=m.from_address.name,
    )
    message_group = crud.create_message_group(conn, message_group)
    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_email', message_group.id, company_id, recipient, m_base)
    return JSONResponse('201 job enqueued\n', status_code=201)
