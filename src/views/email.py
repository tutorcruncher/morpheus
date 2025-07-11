import logging
from buildpg import Values
from buildpg.asyncpg import BuildPgConnection
from fastapi import APIRouter, Body, Depends
from foxglove import glove
from foxglove.db.middleware import get_db
from foxglove.exceptions import HttpConflict
from foxglove.route_class import KeepBodyAPIRoute
from starlette.responses import JSONResponse

from src.schemas.messages import EmailSendModel
from src.spam.email_checker import EmailSpamChecker
from src.spam.services import OpenAISpamEmailService, SpamCacheService, SpamCheckResult

logger = logging.getLogger('views.email')
app = APIRouter(route_class=KeepBodyAPIRoute)


def get_spam_checker() -> EmailSpamChecker:  # pragma: no cover
    cache_service = SpamCacheService(glove.redis)
    spam_service = OpenAISpamEmailService()
    return EmailSpamChecker(spam_service, cache_service)


@app.post('/send/email/')
async def email_send_view(
    m: EmailSendModel = Body(None),
    conn: BuildPgConnection = Depends(get_db),
    spam_checker: EmailSpamChecker = Depends(get_spam_checker),
):
    group_key = f'group:{m.uid}'
    v = await glove.redis.incr(group_key)
    if v > 1:
        raise HttpConflict(f'Send group with id "{m.uid}" already exists\n')
    await glove.redis.expire(group_key, 86400)

    # Only check for spam if enabled in settings and more than 20 recipients
    if glove.settings.enable_spam_check and len(m.recipients) > glove.settings.min_recipients_for_spam_check:
        spam_result = await spam_checker.check_spam(m)
    else:
        logger.info(f'Skipping spam check for {len(m.recipients)} recipients')
        spam_result = SpamCheckResult(spam=False, reason='')

    logger.info('sending %d emails (group %s) via %s for %s', len(m.recipients), m.uid, m.method, m.company_code)
    company_id = await conn.fetchval_b('select id from companies where code=:code', code=m.company_code)
    if not company_id:
        company_id = await conn.fetchval_b(
            'insert into companies (code) values :values returning id', values=Values(code=m.company_code)
        )

    message_group_id = await conn.fetchval_b(
        'insert into message_groups (:values__names) values :values returning id',
        values=Values(
            uuid=str(m.uid),
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
        await glove.redis.enqueue_job('send_email', message_group_id, company_id, recipient, m_base, spam_result)
    return JSONResponse({'message': '201 job enqueued'}, status_code=201)
