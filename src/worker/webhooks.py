import hashlib
import json
import logging
from arq.utils import to_unix_ms
from buildpg import V, Values
from datetime import timezone
from enum import Enum
from foxglove import glove
from pydantic.datetime_parse import parse_datetime
from ua_parser.user_agent_parser import Parse as ParseUserAgent

from src.schemas.messages import SendMethod
from src.schemas.webhooks import BaseWebhook, MandrillWebhook

main_logger = logging.getLogger('worker.webhooks')


class UpdateStatus(str, Enum):
    duplicate = 'duplicate'
    missing = 'missing'
    added = 'added'


async def update_mandrill_webhooks(ctx, events):
    mandrill_webhook = MandrillWebhook(events=events)
    statuses = {}
    for m in mandrill_webhook.events:
        status = await update_message_status(ctx, SendMethod.email_mandrill, m, log_each=False)
        if status in statuses:
            statuses[status] += 1
        else:
            statuses[status] = 1
    main_logger.info(
        'updating %d messages: %s', len(mandrill_webhook.events), ' '.join(f'{k}={v}' for k, v in statuses.items())
    )
    return len(mandrill_webhook.events)


async def store_click(ctx, *, link_id, ip, ts, user_agent):
    cache_key = f'click-{link_id}-{ip}'
    with await ctx['redis'] as redis:
        v = await redis.incr(cache_key)
        if v > 1:
            return 'recently_clicked'
        await redis.expire(cache_key, 60)

    url, message_id = await glove.pg.fetchrow_b(
        'select url, message_id from links where :where', where=V('id') == link_id
    )
    extra = {'target': url, 'ip': ip, 'user_agent': user_agent}
    if user_agent:
        ua_dict = ParseUserAgent(user_agent)
        platform = ua_dict['device']['family']
        if platform in {'Other', None}:
            platform = ua_dict['os']['family']
        extra['user_agent_display'] = '{user_agent[family]} {user_agent[major]} on {platform}'.format(
            platform=platform, **ua_dict
        ).strip(' ')

        ts = parse_datetime(ts)
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        status = 'click'
        await glove.pg.execute_b(
            'insert into events (:values__names) values :values',
            values=Values(message_id=message_id, status=status, ts=ts, extra=json.dumps(extra)),
        )


async def update_message_status(ctx, send_method: SendMethod, m: BaseWebhook, log_each=True) -> UpdateStatus:
    h = hashlib.md5(f'{m.message_id}-{to_unix_ms(m.ts)}-{m.status}-{m.extra_json(sort_keys=True)}'.encode())
    ref = f'event-{h.hexdigest()}'
    with await ctx['redis'] as redis:
        v = await redis.incr(ref)
        if v > 1:
            if log_each:
                main_logger.info('event already exists %s, ts: %s, status: %s. skipped', m.message_id, m.ts, m.status)
            return UpdateStatus.duplicate
        await redis.expire(ref, 86400)

    message_id = await glove.pg.fetchval_b(
        'select id from messages where :where', where=(V('external_id') == m.message_id) & (V('method') == send_method)
    )
    if not message_id:
        return UpdateStatus.missing

    if not m.ts.tzinfo:
        m.ts = m.ts.replace(tzinfo=timezone.utc)

    if log_each:
        main_logger.info('adding event %s, ts: %s, status: %s', m.message_id, m.ts, m.status)

    await glove.pg.execute_b(
        'insert into events (:values__names) values :values',
        values=Values(message_id=message_id, status=m.status, ts=m.ts, extra=m.extra_json()),
    )
    return UpdateStatus.added
