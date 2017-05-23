from pathlib import Path

import msgpack
from aiohttp.web import HTTPConflict, Response

from .models import SendModel
from .utils import ServiceView

THIS_DIR = Path(__file__).parent.resolve()


class SendView(ServiceView):
    async def call(self, request):
        m: SendModel = await self.request_data(SendModel)
        async with await self.sender.get_redis_conn() as redis:
            group_key = f'group:{m.id}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.id}" already exists\n')
            recipients_key = f'recipients:{m.id}'
            data = m.values
            recipients = data.pop('recipients')
            from_ = data.pop('from_address')
            data.update(
                from_email=from_.email,
                from_name=from_.name,
            )
            pipe = redis.pipeline()
            pipe.lpush(recipients_key, *map(self.encode_recipients, recipients))
            pipe.expire(group_key, 86400)
            pipe.expire(recipients_key, 86400)
            await pipe.execute()
            await self.sender.send(recipients_key, **data)
        return Response(text='201 job enqueued\n', status=201)

    def encode_recipients(self, recipient):
        return msgpack.packb(recipient.values, use_bin_type=True)
