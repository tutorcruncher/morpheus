import asyncio
import json
import logging

from aiohttp import BasicAuth, ClientSession
from aiohttp.hdrs import METH_GET, METH_DELETE, METH_POST, METH_PUT
from aiohttp.web_response import Response

from .settings import Settings

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearchError(RuntimeError):
    pass


class ElasticSearch:
    def __init__(self, settings: Settings, loop=None):
        self.settings = settings
        self.loop = loop or asyncio.get_event_loop()
        self.session = ClientSession(
            loop=self.loop,
            auth=BasicAuth(settings.elastic_username, settings.elastic_password)
        )
        self.root = self.settings.elastic_url.rstrip('/') + '/'

    def close(self):
        self.session.close()

    async def get(self, uri):
        return await self._request(METH_GET, uri)

    async def delete(self, uri):
        return await self._request(METH_DELETE, uri)

    async def post(self, uri, **data):
        return await self._request(METH_POST, uri, **data)

    async def put(self, uri, **data):
        return await self._request(METH_PUT, uri, **data)

    async def _request(self, method, uri, **data) -> Response:
        async with self.session.request(method, self.root + uri, json=data) as r:
            if r.status not in (200, 201):
                data = await r.json()
                raise ElasticSearchError(
                    f'{method} {uri}, bad response {r.status}, response:\n{json.dumps(data, indent=2)}'
                )
            main_logger.debug('%s /%s -> %s', method, uri, r.status)
            return r

    async def create_indices(self, delete_existing=False):
        """
        Create mappings for indices
        """
        for index_name, mapping in MAPPINGS.items():
            r = await self.get(index_name)
            if r.status != 404:
                if delete_existing:
                    main_logger.warning('deleting index %s', index_name)
                    await self.delete(index_name)
                else:
                    main_logger.warning('index %s already exists, not creating', index_name)
                    continue
            main_logger.info('creating index %s...', index_name)
            await self.put(index_name, mappings={
                '_default_': {
                        'dynamic': 'strict',
                        'properties': mapping,
                    }
                }
            )


MAPPINGS = {
    'messages': {
        'group_id': {'type': 'keyword'},
        'method': {'type': 'keyword'},
        'send_ts': {'type': 'date'},
        'update_ts': {'type': 'date'},
        'status': {'type': 'keyword'},
        'to_first_name': {'type': 'keyword'},
        'to_last_name': {'type': 'keyword'},
        'to_email': {'type': 'keyword'},
        'from_email': {'type': 'keyword'},
        'from_name': {'type': 'keyword'},
        'search_tags': {'type': 'keyword'},
        'analytics_tags': {'type': 'keyword'},
        'subject': {'type': 'text'},
        'body': {'type': 'text'},
        'attachments': {'type': 'keyword'},
        'events': {
            'properties': {
                'ts': {'type': 'date'},
                'status': {'type': 'keyword'},
                'extra': {
                    'type': 'object',
                    'dynamic': 'true',
                },
            }
        },
    },
}
