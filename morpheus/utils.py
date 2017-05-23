import asyncio
from enum import Enum
from functools import update_wrapper
from random import random
from typing import Optional, Type

import msgpack
from aiohttp.web import Application, HTTPBadRequest, Request, HTTPUnauthorized  # noqa
from pydantic import BaseModel, ValidationError


class ContentType(str, Enum):
    JSON = 'application/json'
    MSGPACK = 'application/msgpack'


class WebModel(BaseModel):
    def _process_values(self, values):
        try:
            return super()._process_values(values)
        except ValidationError as e:
            raise HTTPBadRequest(text=e.display_errors)


class Session(BaseModel):
    user_id: int = ...
    company: str = ...
    is_admin: bool = False


class View:
    def __init__(self, request):
        from .worker import Sender
        self.request: Request = request
        self.app: Application = request.app
        self.session: Optional[Session] = request.get('session')
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


class ServiceView(View):
    """
    Views used by services. Services are in charge and can be trusted to do "whatever they like".
    """
    async def authenticate(self, request):
        if request.app['settings'].auth_key != request.headers.get('Authorization', ''):
            # avoid the need for constant time compare on auth key
            await asyncio.sleep(random())
            raise HTTPUnauthorized(text='Invalid "Authorization" header')
