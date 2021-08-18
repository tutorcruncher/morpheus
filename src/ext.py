import json
import logging
from foxglove import glove
from httpx import Response

from .settings import Settings

logger = logging.getLogger('ext')


def lenient_json(v):
    if isinstance(v, (str, bytes)):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            pass
    return v


class ApiError(RuntimeError):
    def __init__(self, method, url, status, response_text):
        self.method = method
        self.url = url
        self.status = status
        self.body = response_text

    def __str__(self):
        return f'{self.method} {self.url}, unexpected response {self.status}'


class ApiSession:
    def __init__(self, root_url, settings: Settings):
        self.settings = settings
        self.root = root_url.rstrip('/') + '/'

    async def get(self, uri, *, allowed_statuses=(200,), **data) -> Response:
        return await self._request('GET', uri, allowed_statuses=allowed_statuses, **data)

    async def delete(self, uri, *, allowed_statuses=(200,), **data) -> Response:
        return await self._request('DELETE', uri, allowed_statuses=allowed_statuses, **data)

    async def post(self, uri, *, allowed_statuses=(200, 201), **data) -> Response:
        return await self._request('POST', uri, allowed_statuses=allowed_statuses, **data)

    async def put(self, uri, *, allowed_statuses=(200, 201), **data) -> Response:
        return await self._request('PUT', uri, allowed_statuses=allowed_statuses, **data)

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> Response:
        method, url, data = self._modify_request(method, self.root + str(uri).lstrip('/'), data)
        kwargs = {}
        headers = data.pop('headers_', None)
        if headers is not None:
            kwargs['headers'] = headers
        if timeout := data.pop('timeout_', None):
            kwargs['timeout'] = timeout
        r = await glove.http.request(method, url, json=data or None, **kwargs)
        if isinstance(allowed_statuses, int):
            allowed_statuses = (allowed_statuses,)
        if allowed_statuses != '*' and r.status_code not in allowed_statuses:
            data = {
                'request_real_url': str(r.request.url),
                'request_headers': dict(r.request.headers),
                'request_data': data,
                'response_headers': dict(r.headers),
                'response_content': lenient_json(r.text),
            }
            logger.warning(
                '%s unexpected response %s /%s -> %s',
                self.__class__.__name__,
                method,
                uri,
                r.status_code,
                extra={'data': data} if self.settings.verbose_http_errors else {},
            )
            raise ApiError(method, url, r.status_code, r.text)
        else:
            logger.debug('%s /%s -> %s', method, uri, r.status_code)
            return r

    def _modify_request(self, method, url, data):
        return method, url, data


class Mandrill(ApiSession):
    def __init__(self, settings):
        super().__init__(settings.mandrill_url, settings)

    def _modify_request(self, method, url, data):
        data['key'] = self.settings.mandrill_key
        return method, url, data


class MessageBird(ApiSession):
    def __init__(self, settings):
        super().__init__(settings.messagebird_url, settings)

    def _modify_request(self, method, url, data):
        data['headers_'] = {'Authorization': f'AccessKey {self.settings.messagebird_key}'}
        return method, url, data
