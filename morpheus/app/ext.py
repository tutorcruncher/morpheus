import asyncio
import hashlib
import hmac
import json
import logging
from urllib.parse import urlencode

import ujson
from aiohttp import ClientResponse, ClientSession
from aiohttp.hdrs import METH_DELETE, METH_GET, METH_POST, METH_PUT

from .settings import Settings

logger = logging.getLogger('morpheus.ext')


def lenient_json(v):
    if isinstance(v, (str, bytes)):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            pass
    return v


class ApiError(RuntimeError):
    def __init__(self, method, url, response, response_text):
        self.method = method
        self.url = url
        self.status = response.status
        self.body = response_text

    def __str__(self):
        return f'{self.method} {self.url}, unexpected response {self.status}'


class ApiSession:
    def __init__(self, root_url, settings: Settings, loop=None):
        self.settings = settings
        self.loop = loop or asyncio.get_event_loop()
        self.session = ClientSession(
            loop=self.loop,
            json_serialize=ujson.dumps,
        )
        self.root = root_url.rstrip('/') + '/'

    async def close(self):
        await self.session.close()

    async def get(self, uri, *, allowed_statuses=(200,), **data):
        return await self._request(METH_GET, uri, allowed_statuses=allowed_statuses, **data)

    async def delete(self, uri, *, allowed_statuses=(200,), **data):
        return await self._request(METH_DELETE, uri, allowed_statuses=allowed_statuses, **data)

    async def post(self, uri, *, allowed_statuses=(200, 201), **data):
        return await self._request(METH_POST, uri, allowed_statuses=allowed_statuses, **data)

    async def put(self, uri, *, allowed_statuses=(200, 201), **data):
        return await self._request(METH_PUT, uri, allowed_statuses=allowed_statuses, **data)

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> ClientResponse:
        method, url, data = self._modify_request(method, self.root + str(uri).lstrip('/'), data)
        headers = data.pop('headers_', {})
        timeout = data.pop('timeout_', 30)
        async with self.session.request(method, url, json=data or None, headers=headers, timeout=timeout) as r:
            # always read entire response before closing the connection
            response_text = await r.text()

        if isinstance(allowed_statuses, int):
            allowed_statuses = allowed_statuses,
        if allowed_statuses != '*' and r.status not in allowed_statuses:
            data = {
                'request_real_url': str(r.request_info.real_url),
                'request_headers': dict(r.request_info.headers),
                'request_data': data,
                'response_headers': dict(r.headers),
                'response_content': lenient_json(response_text),
            }
            logger.warning('%s unexpected response %s /%s -> %s', self.__class__.__name__, method, uri, r.status,
                           extra={'data': data})
            raise ApiError(method, url, r, response_text)
        else:
            logger.debug('%s /%s -> %s', method, uri, r.status)
            return r

    def _modify_request(self, method, url, data):
        return method, url, data


class Mandrill(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.mandrill_url, settings, loop)

    def _modify_request(self, method, url, data):
        data['key'] = self.settings.mandrill_key
        return method, url, data


far_future = '2032-01-01T00:00:00+00'


class MorpheusUserApi(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.local_api_url, settings, loop)

    def _modify_request(self, method, url, data):
        return method, self.modify_url(url), data

    def modify_url(self, url):
        args = dict(
            company='__all__',
            expires=far_future,
        )
        body = '{company}:{expires}'.format(**args).encode()
        args['signature'] = hmac.new(self.settings.user_auth_key, body, hashlib.sha256).hexdigest()
        url = str(url)
        return url + ('&' if '?' in url else '?') + urlencode(args)


class MessageBird(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.messagebird_url, settings, loop)

    def _modify_request(self, method, url, data):
        data['headers_'] = {'Authorization': f'AccessKey {self.settings.messagebird_key}'}
        return method, url, data
