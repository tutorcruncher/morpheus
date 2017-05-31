from pathlib import Path

import msgpack
from aiohttp.web import HTTPConflict, Response

from .models import SendModel, MandrillWebhook
from .utils import ServiceView, View

THIS_DIR = Path(__file__).parent.resolve()


class SendView(ServiceView):
    async def call(self, request):
        m: SendModel = await self.request_data(SendModel)
        async with await self.sender.get_redis_conn() as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.uid}" already exists\n')
            recipients_key = f'recipients:{m.uid}'
            data = m.values
            recipients = data.pop('recipients')
            from_ = data.pop('from_address')
            data.update(
                from_email=from_.email,
                from_name=from_.name,
            )
            pipe = redis.pipeline()
            pipe.lpush(recipients_key, *[msgpack.packb(r, use_bin_type=True) for r in recipients])
            pipe.expire(group_key, 86400)
            pipe.expire(recipients_key, 86400)
            await pipe.execute()
            await self.sender.send(recipients_key, **data)
        return Response(text='201 job enqueued\n', status=201)


async def update_message_status(app, m: MandrillWebhook, es_type):
        update_uri = f'messages/{es_type}/{m.message_id}/_update'
        await app['es'].post(update_uri, doc={'update_ts': m.ts, 'status': m.event})
        data = m.values
        data.pop('message_id')
        await app['es'].post(
            update_uri,
            script={
                'lang': 'painless',
                'inline': 'ctx._source.events.add(params.event)',
                'params': {
                    'event': {
                        'ts': data.pop('ts'),
                        'status': data.pop('event'),
                        'extra': data,
                    }
                }
            }
        )


class TestWebhookView(View):
    """
    Simple view to update messages "sent" with email-test
    """
    async def call(self, request):
        m: MandrillWebhook = await self.request_data(MandrillWebhook)
        await update_message_status(request.app, m, 'email-test')
        return Response(text='message status updated\n', status=200)
