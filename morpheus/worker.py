import msgpack
from aiohttp import ClientSession
from arq import Actor, BaseWorker, concurrent

from morpheus.settings import Settings


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = None

    async def startup(self):
        self.session = ClientSession(loop=self.loop)

    @concurrent
    async def send(self, recipients_key, **data):
        async with await self.get_redis_conn() as redis:
            while True:
                v = await redis.rpop(recipients_key)
                if not v:
                    break
                data = msgpack.unpackb(v, encoding='utf8')
                await self.send_single(**data)

    @concurrent
    async def send_single(self, **data):
        print(data)

    async def shutdown(self):
        self.session.close()


class Worker(BaseWorker):
    shadows = [Sender]
