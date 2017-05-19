from enum import Enum
from functools import update_wrapper
from typing import Optional

from aiohttp.web import Application, HTTPBadRequest, Request  # noqa
from pydantic import BaseModel, ValidationError


class ContentType(str, Enum):
    JSON = 'application/json'


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
        self.request: Request = request
        self.app: Application = request.app
        self.session: Optional[Session] = request['session']

    @classmethod
    def view(cls):
        async def view(request):
            self = cls(request)
            await self.authenticate(request)
            return await self.dispatch(request)

        view.view_class = cls

        # take name and docstring from class
        update_wrapper(view, cls, updated=())

        # and possible attributes set by decorators
        update_wrapper(view, cls.dispatch, assigned=())
        return view

    async def authenticate(self, request):
        pass

    async def dispatch(self, request):
        raise NotImplementedError()

    async def request_data(self):
        # TODO msgpack
        try:
            data = await self.request.json()
        except ValueError as e:
            raise HTTPBadRequest(text=f'invalid request json: {e}')
        if not isinstance(data, dict):
            raise HTTPBadRequest(text='request json should be a dictionary')
        return data
