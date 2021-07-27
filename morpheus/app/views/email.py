import logging

from buildpg import Values
from buildpg.asyncpg import BuildPgConnection
from fastapi import APIRouter, Depends
from foxglove import glove
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpConflict
from foxglove.route_class import KeepBodyAPIRoute

from morpheus.app.schema import EmailSendModel
from morpheus.app.utils import PreResponse
from morpheus.app.crud import get_create_company_id

logger = logging.getLogger('views.email')
app = APIRouter(route_class=KeepBodyAPIRoute)


@app.post('/send/email/')
async def email_send_view(m: EmailSendModel, conn: BuildPgConnection = Depends(get_db)):
    with await glove.redis as redis:
        group_key = f'group:{m.uid}'
        v = await redis.incr(group_key)
        if v > 1:
            raise HttpConflict(f'Send group with id "{m.uid}" already exists\n')
        await redis.expire(group_key, 86400)

    logger.info('sending %d emails (group %s) via %s for %s', len(m.recipients), m.uid, m.method, m.company_code)
    company_id = await get_create_company_id(conn, m.company_code)
    group_id = await conn.fetchval_b(
        'insert into message_groups (:values__names) values :values returning id',
        values=Values(
            uuid=m.uid,
            company_id=company_id,
            message_method=m.method,
            from_email=m.from_address.email,
            from_name=m.from_address.name,
        ),
    )
    recipients = m.recipients
    m_base = m.copy(exclude={'recipients'})
    del m
    for recipient in recipients:
        await glove.redis.enqueue_job('send_email', group_id, company_id, recipient, m_base)
    return PreResponse(text='201 job enqueued\n', status=201)
