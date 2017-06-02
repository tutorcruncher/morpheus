import asyncio
import json
import logging
from datetime import datetime

from aiohttp import BasicAuth, ClientSession
from aiohttp.hdrs import METH_DELETE, METH_GET, METH_POST, METH_PUT
from aiohttp.web_response import Response
from arq.utils import to_unix_ms

from .settings import Settings

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearchError(RuntimeError):
    pass


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return to_unix_ms(obj)[0]
        return super().default(obj)


class ElasticSearch:
    def __init__(self, settings: Settings, loop=None):
        self.settings = settings
        self.loop = loop or asyncio.get_event_loop()
        self.session = ClientSession(
            loop=self.loop,
            auth=BasicAuth(settings.elastic_username, settings.elastic_password),
            json_serialize=self.encode_json,
        )
        self.root = self.settings.elastic_url.rstrip('/') + '/'

    @classmethod
    def encode_json(cls, data):
        return json.dumps(data, cls=CustomJSONEncoder)

    def close(self):
        self.session.close()

    async def get(self, uri, **kwargs):
        return await self._request(METH_GET, uri, **kwargs)

    async def delete(self, uri, **kwargs):
        return await self._request(METH_DELETE, uri, **kwargs)

    async def post(self, uri, **kwargs):
        return await self._request(METH_POST, uri, **kwargs)

    async def put(self, uri, **kwargs):
        return await self._request(METH_PUT, uri, **kwargs)

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> Response:
        async with self.session.request(method, self.root + uri, json=data) as r:
            if allowed_statuses != '*' and r.status not in allowed_statuses:
                data = await r.text()
                try:
                    data = json.dumps(json.loads(data), indent=2)
                except ValueError:
                    pass
                raise ElasticSearchError(
                    f'{method} {uri}, bad response {r.status}, response:\n{data}'
                )
            main_logger.debug('%s /%s -> %s', method, uri, r.status)
            return r

    async def create_indices(self, delete_existing=False):
        """
        Create mappings for indices
        """
        for index_name, mapping in MAPPINGS.items():
            r = await self.get(index_name, allowed_statuses=(200, 404))
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


KEYWORD = {'type': 'keyword'}
DATE = {'type': 'date'}
TEXT = {'type': 'text'}
MAPPINGS = {
    'messages': {
        'group_id': KEYWORD,
        'company': KEYWORD,
        'method': KEYWORD,
        'send_ts': DATE,
        'update_ts': DATE,
        'status': KEYWORD,
        'to_first_name': KEYWORD,
        'to_last_name': KEYWORD,
        'to_email': KEYWORD,
        'from_email': KEYWORD,
        'from_name': KEYWORD,
        'tags': KEYWORD,
        'subject': TEXT,
        'body': TEXT,
        'attachments': KEYWORD,
        'events': {
            'properties': {
                'ts': DATE,
                'status': KEYWORD,
                'extra': {
                    'type': 'object',
                    'dynamic': 'true',
                },
            }
        },
    },
}
