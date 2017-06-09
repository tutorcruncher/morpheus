import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from functools import update_wrapper
from random import random
from typing import Optional, Type  # noqa

import msgpack
from aiohttp import ClientSession
from aiohttp.hdrs import METH_DELETE, METH_GET, METH_POST, METH_PUT
from aiohttp.web import Application, HTTPBadRequest, HTTPForbidden, Request, Response  # noqa
from arq.utils import to_unix_ms
from cryptography.fernet import InvalidToken
from pydantic import BaseModel, ValidationError

from .settings import Settings

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
    user_id: int = ...
    expires: datetime = ...


class View:
    def __init__(self, request):
        from .worker import Sender  # noqa
        self.request: Request = request
        self.app: Application = request.app
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
        return await self.request.json()

    def get_arg_int(self, name, default=None):
        v = self.request.GET.get(name)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            raise HTTPBadRequest(text=f'invalid get argument "{name}": {v!r}')


class ServiceView(View):
    """
    Views used by services. Services are in charge and can be trusted to do "whatever they like".
    """
    async def authenticate(self, request):
        if request.app['settings'].auth_key != request.headers.get('Authorization', ''):
            # avoid the need for constant time compare on auth key
            await asyncio.sleep(random())
            raise HTTPForbidden(text='Invalid "Authorization" header')


class UserView(View):
    """
    Views used by users via ajax, "Authorization" header is Fernet encrypted user data.
    """
    async def authenticate(self, request):
        token = request.headers.get('Authorization', '')
        try:
            raw_data = self.app['fernet'].decrypt(token.encode())
        except InvalidToken:
            await asyncio.sleep(random())
            raise HTTPForbidden(text='Invalid token')

        try:
            data = msgpack.unpackb(raw_data, encoding='utf8')
        except ValueError:
            raise HTTPBadRequest(text='bad auth data')
        self.session = Session(**data)
        if self.session.expires < datetime.utcnow().replace(tzinfo=timezone.utc):
            raise HTTPForbidden(text='token expired')


class ApiError(RuntimeError):
    def __init__(self, method, url, request_data, response, response_text):
        self.method = method
        self.url = url
        self.response = response
        self.status = response.status
        self.request_data = request_data
        try:
            self.response_text = json.dumps(json.loads(response_text), indent=2)
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
            return to_unix_ms(obj)[0]
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

    async def get(self, uri, **kwargs):
        return await self._request(METH_GET, uri, **kwargs)

    async def delete(self, uri, **kwargs):
        return await self._request(METH_DELETE, uri, **kwargs)

    async def post(self, uri, **kwargs):
        return await self._request(METH_POST, uri, **kwargs)

    async def put(self, uri, **kwargs):
        return await self._request(METH_PUT, uri, **kwargs)

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> Response:
        method, url, data = self._modify_request(method, self.root + uri.lstrip('/'), data)
        async with self.session.request(method, url, json=data) as r:
            # always read entire response before closing the connection
            response_text = await r.text()

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
