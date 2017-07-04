import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
from datetime import datetime, timezone
from enum import Enum
from functools import update_wrapper
from pathlib import Path
from random import random
from typing import Optional, Type  # noqa
from urllib.parse import urlencode

import msgpack
import ujson
from aiohttp import ClientSession
from aiohttp.hdrs import METH_DELETE, METH_GET, METH_POST, METH_PUT
from aiohttp.web import Application, HTTPBadRequest, HTTPForbidden, HTTPUnauthorized, Request, Response  # noqa
from arq.utils import to_unix_ms
from pydantic import BaseModel, ValidationError

from .settings import Settings

THIS_DIR = Path(__file__).parent.resolve()
api_logger = logging.getLogger('morpheus.external')


class ContentType(str, Enum):
    JSON = 'application/json'
    MSGPACK = 'application/msgpack'


class WebModel(BaseModel):
    def _process_values(self, values):
        try:
            return super()._process_values(values)
        except ValidationError as e:
            raise HTTPBadRequest(text=e.display_errors)


class Session(WebModel):
    company: str = ...
    expires: datetime = ...


class View:
    def __init__(self, request):
        from .worker import Sender  # noqa
        self.request: Request = request
        self.app: Application = request.app
        self.settings: Settings = self.app['settings']
        self.session: Optional[Session] = None
        self.sender: Sender = request.app['sender']

    @classmethod
    def view(cls):
        async def view(request):
            self = cls(request)
            await self.authenticate(request)
            return await self.call(request)

        view.view_class = cls

        # take name and docstring from class
        update_wrapper(view, cls, updated=())

        # and possible attributes set by decorators
        update_wrapper(view, cls.call, assigned=())
        return view

    async def authenticate(self, request):
        pass

    async def call(self, request):
        raise NotImplementedError()

    async def request_data(self, validator: Type[WebModel]=None):
        decoder = self.decode_json
        content_type = self.request.headers.get('Content-Type')
        if content_type == ContentType.MSGPACK:
            decoder = self.decode_msgpack

        try:
            data = await decoder()
        except ValueError as e:
            raise HTTPBadRequest(text=f'invalid request data for {decoder.__name__}: {e}')

        if not isinstance(data, dict):  # TODO is this necessary?
            raise HTTPBadRequest(text='request data should be a dictionary')

        if validator:
            return validator(**data)
        else:
            return data

    async def decode_msgpack(self):
        data = await self.request.read()
        return msgpack.unpackb(data, encoding='utf8')

    async def decode_json(self):
        return await self.request.json(loads=ujson.loads)

    def get_arg_int(self, name, default=None):
        v = self.request.query(name)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            raise HTTPBadRequest(text=f'invalid get argument "{name}": {v!r}')

    @classmethod
    def json_response(cls, *, status_=200, list_=None, headers_=None, **data):
        return Response(
            body=json.dumps(data if list_ is None else list_).encode(),
            status=status_,
            content_type='application/json',
            headers=headers_,
        )


class AuthView(View):
    """
    token authentication with no "Token " prefix
    """
    auth_token_field = None

    async def authenticate(self, request):
        auth_token = getattr(self.settings, self.auth_token_field)
        if not secrets.compare_digest(auth_token, request.headers.get('Authorization', '')):
            # avoid the need for constant time compare on auth key
            await asyncio.sleep(random())
            raise HTTPForbidden(text='Invalid "Authorization" header')


class ServiceView(AuthView):
    """
    Views used by services. Services are in charge and can be trusted to do "whatever they like".
    """
    auth_token_field = 'auth_key'


class UserView(View):
    """
    Views used by users via ajax
    """
    async def authenticate(self, request):
        company = request.query.get('company', None)
        expires = request.query.get('expires', None)
        body = f'{company}:{expires}'.encode()
        expected_sig = hmac.new(self.settings.user_auth_key, body, hashlib.sha256).hexdigest()
        signature = request.query.get('signature', '-')
        if not secrets.compare_digest(expected_sig, signature):
            await asyncio.sleep(random())
            raise HTTPForbidden(text='Invalid token')

        self.session = Session(
            company=company,
            expires=expires,
        )
        if self.session.expires < datetime.utcnow().replace(tzinfo=timezone.utc):
            raise HTTPForbidden(text='token expired')


class BasicAuthView(View):
    """
    Views used by admin, applies basic auth.
    """
    async def authenticate(self, request):
        token = re.sub('^Basic *', '', request.headers.get('Authorization', '')) or 'x'
        try:
            _, password = base64.b64decode(token).decode().split(':', 1)
        except (ValueError, UnicodeDecodeError):
            password = ''

        if not secrets.compare_digest(password, self.settings.admin_basic_auth_password):
            await asyncio.sleep(random())
            raise HTTPUnauthorized(text='Invalid basic auth', headers={'WWW-Authenticate': 'Basic'})


class ApiError(RuntimeError):
    def __init__(self, method, url, request_data, response, response_text):
        self.method = method
        self.url = url
        self.response = response
        self.status = response.status
        self.request_data = request_data
        try:
            self.response_text = json.dumps(ujson.loads(response_text), indent=2)
        except ValueError:
            self.response_text = response_text

    def __str__(self):
        return (
            f'{self.method} {self.url}, bad response {self.status}\n'
            f'Request data: {json.dumps(self.request_data, indent=2, cls=CustomJSONEncoder)}\n'
            f'-----------------------------\n'
            f'Response data: {self.response_text}'
        )


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return to_unix_ms(obj)
        elif isinstance(obj, set):
            return sorted(obj)
        return super().default(obj)


class ApiSession:
    def __init__(self, root_url, settings: Settings, loop=None):
        self.settings = settings
        self.loop = loop or asyncio.get_event_loop()
        self.session = ClientSession(
            loop=self.loop,
            json_serialize=self.encode_json,
        )
        self.root = root_url.rstrip('/') + '/'

    @classmethod
    def encode_json(cls, data):
        return json.dumps(data, cls=CustomJSONEncoder)

    def close(self):
        self.session.close()

    async def get(self, uri, *, allowed_statuses=(200,), **data):
        return await self._request(METH_GET, uri, allowed_statuses=allowed_statuses, **data)

    async def delete(self, uri, *, allowed_statuses=(200,), **data):
        return await self._request(METH_DELETE, uri, allowed_statuses=allowed_statuses, **data)

    async def post(self, uri, *, allowed_statuses=(200, 201), **data):
        return await self._request(METH_POST, uri, allowed_statuses=allowed_statuses, **data)

    async def put(self, uri, *, allowed_statuses=(200, 201), **data):
        return await self._request(METH_PUT, uri, allowed_statuses=allowed_statuses, **data)

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> Response:
        method, url, data = self._modify_request(method, self.root + str(uri).lstrip('/'), data)
        headers = data.pop('headers_', {})
        async with self.session.request(method, url, json=data, headers=headers) as r:
            # always read entire response before closing the connection
            response_text = await r.text()

        if isinstance(allowed_statuses, int):
            allowed_statuses = allowed_statuses,
        if allowed_statuses != '*' and r.status not in allowed_statuses:
            raise ApiError(method, url, data, r, response_text)
        else:
            api_logger.debug('%s /%s -> %s', method, uri, r.status)
            return r

    def _modify_request(self, method, url, data):
        return method, url, data


class Mandrill(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.mandrill_url, settings, loop)

    def _modify_request(self, method, url, data):
        data['key'] = self.settings.mandrill_key
        return method, url, data


class MorpheusUserApi(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.local_api_url, settings, loop)

    def _modify_request(self, method, url, data):
        return method, self.modify_url(url), data

    def modify_url(self, url):
        args = dict(
            company='__all__',
            expires=to_unix_ms(datetime(2032, 1, 1))
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
