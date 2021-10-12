import logging
from buildpg import V
from datetime import datetime, timedelta
from foxglove import glove

logger = logging.getLogger('worker.scheduler')


async def update_aggregation_view(ctx):
    async with glove.pg.acquire() as conn:
        await conn.execute('refresh materialized view message_aggregation')


async def delete_old_emails(ctx):
    if not glove.settings.delete_old_emails:
        logger.info('settings.delete_old_emails False, not running')
        return
    today = datetime.today()
    start, end = today - timedelta(days=368), today - timedelta(days=365)
    async with glove.pg.acquire() as conn:
        count = await conn.execute_b(
            'delete from messages where :where', where=(start <= V('send_ts')) & (V('send_ts') <= end)
        )
    logger.info('deleted %s old messages', count.replace('DELETE ', ''))
