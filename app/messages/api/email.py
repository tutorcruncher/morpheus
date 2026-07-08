import logging

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse

from app.common.api.errors import HTTP409
from app.core.database import DBSession, get_db
from app.messages.api.sms import _get_or_create_company
from app.messages.models import MessageGroup
from app.messages.schemas import EmailSendModel
from app.messages.tasks import get_redis, send_email

logger = logging.getLogger('views.email')
router = APIRouter()


@router.post('/send/email/')
def email_send_view(
    m: EmailSendModel = Body(None),
    db: DBSession = Depends(get_db),
):
    redis = get_redis()
    group_key = f'group:{m.uid}'
    if not redis.set(group_key, '1', ex=86400, nx=True):
        raise HTTP409(f'Send group with id "{m.uid}" already exists\n')

    logger.info(
        'sending %d emails (group %s) via %s for %s',
        len(m.recipients),
        m.uid,
        m.method,
        m.company_code,
    )

    company = _get_or_create_company(db, m.company_code)
    company_id = company.id

    group = MessageGroup(
        uuid=m.uid,
        company_id=company_id,  # ty:ignore[invalid-argument-type]
        message_method=m.method.value,
        from_email=m.from_address.email,
        from_name=m.from_address.name,
    )
    db.add(group)
    db.commit()
    db.refresh(group)
    group_id = group.id
    # Release the pooled connection before the per-recipient enqueue loop: each send_email.delay()
    # is a broker round-trip and previously held a scarce connection open for the whole fan-out,
    # widening pool-exhaustion windows (MORPHEUS-3DNG).
    db.close()

    recipients = m.recipients
    m_base = m.model_copy(update={'recipients': []}).model_dump(mode='json')
    for recipient in recipients:
        send_email.delay(group_id, company_id, recipient.model_dump(mode='json'), m_base)
    return JSONResponse({'message': '201 job enqueued'}, status_code=201)
