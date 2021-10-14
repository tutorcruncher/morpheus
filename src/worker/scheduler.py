import logging
from datetime import datetime, timedelta
from foxglove import glove

logger = logging.getLogger('worker.scheduler')


async def update_aggregation_view(ctx):
    if not glove.settings.update_aggregation_view:
        logger.info('settings.delete_old_emails False, not running')
        return
    async with glove.pg.acquire() as conn:
        await conn.execute('refresh materialized view message_aggregation')


async def delete_old_emails(ctx):
    if not glove.settings.delete_old_emails:
        logger.info('settings.delete_old_emails False, not running')
        return
    async with glove.pg.acquire() as conn:
        count = await conn.execute_b(
            'delete from message_groups where id in (select id from message_groups where created_ts < :cutoff)',
            cutoff=datetime.now() - timedelta(days=365),
        )
    logger.info('deleted %s old messages', count.replace('DELETE ', ''))
