import json
import logging

import httpx

from app.core.config import Settings, settings as default_settings

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


_default_client = httpx.Client(timeout=30)


class ApiSession:
    def __init__(self, root_url: str, settings: Settings, client: httpx.Client | None = None):
        self.settings = settings
        self.root = root_url.rstrip('/') + '/'
        self.client = client or _default_client

    def get(self, uri, *, allowed_statuses=(200,), **data) -> httpx.Response:
        return self._request('GET', uri, allowed_statuses=allowed_statuses, **data)

    def delete(self, uri, *, allowed_statuses=(200,), **data) -> httpx.Response:
        return self._request('DELETE', uri, allowed_statuses=allowed_statuses, **data)

    def post(self, uri, *, allowed_statuses=(200, 201), **data) -> httpx.Response:
        return self._request('POST', uri, allowed_statuses=allowed_statuses, **data)

    def put(self, uri, *, allowed_statuses=(200, 201), **data) -> httpx.Response:
        return self._request('PUT', uri, allowed_statuses=allowed_statuses, **data)

    def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> httpx.Response:
        method, url, data = self._modify_request(method, self.root + str(uri).lstrip('/'), data)
        kwargs = {}
        headers = data.pop('headers_', None)
        if headers is not None:
            kwargs['headers'] = headers
        if timeout := data.pop('timeout_', None):
            kwargs['timeout'] = timeout
        r = self.client.request(method, url, json=data or None, **kwargs)
        if isinstance(allowed_statuses, int):
            allowed_statuses = (allowed_statuses,)
        if allowed_statuses != '*' and r.status_code not in allowed_statuses:
            extra = {
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
                extra={'data': extra},
            )
            raise ApiError(method, url, r.status_code, r.text)
        logger.debug('%s /%s -> %s', method, uri, r.status_code)
        return r

    def _modify_request(self, method, url, data):
        return method, url, data


class Mandrill(ApiSession):
    def __init__(self, settings: Settings | None = None, client: httpx.Client | None = None):
        s = settings or default_settings
        super().__init__(s.mandrill_url, s, client=client)

    def _modify_request(self, method, url, data):
        data['key'] = self.settings.mandrill_key
        return method, url, data


class MessageBird(ApiSession):
    def __init__(self, settings: Settings | None = None, client: httpx.Client | None = None):
        s = settings or default_settings
        super().__init__(s.messagebird_url, s, client=client)

    def _modify_request(self, method, url, data):
        data['headers_'] = {'Authorization': f'AccessKey {self.settings.messagebird_key}'}
        return method, url, data


def get_mandrill() -> Mandrill:
    return Mandrill()


def get_messagebird() -> MessageBird:
    return MessageBird()
