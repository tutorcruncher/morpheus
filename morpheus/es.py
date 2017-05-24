import asyncio

import logging
from aiohttp import BasicAuth, ClientSession

from .settings import Settings

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearch:
    def __init__(self, settings: Settings, loop):
        self.settings = settings
        self.loop = loop or asyncio.get_event_loop()
        self.session = ClientSession(
            loop=self.loop,
            auth=BasicAuth(settings.elastic_username, settings.elastic_password)
        )
        self.root = self.settings.elastic_url.rstrip('/') + '/'

    async def get(self, uri):
        async with self.session.get(self.root + uri) as r:
            main_logger.debug('GET %s -> %s', uri, r.status)
            return await r.json()

    async def post(self, uri, **data):
        async with self.session.post(self.root + uri, json=data) as r:
            main_logger.debug('POST %s -> %s', uri, r.status)
            return await r.json()
