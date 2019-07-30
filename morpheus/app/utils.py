import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
from asyncio import shield
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import update_wrapper
from pathlib import Path
from typing import Dict, Optional, Type, TypeVar

import ujson
from aiohttp.hdrs import METH_GET, METH_HEAD, METH_OPTIONS
from aiohttp.web import Application, HTTPClientError, Request, Response
from aiohttp_jinja2 import render_template
from arq import ArqRedis
from markupsafe import Markup
from pydantic import BaseModel, ValidationError
from pydantic.json import pydantic_encoder

from .ext import ApiError
from .models import SendMethod
from .settings import Settings

THIS_DIR = Path(__file__).parent.resolve()
CONTENT_TYPE_JSON = 'application/json'
AModel = TypeVar('AModel', bound=BaseModel)


class Session(BaseModel):
    company: str
    expires: datetime


def pretty_lenient_json(data):
    return json.dumps(data, indent=2, default=pydantic_encoder) + '\n'


class JsonErrors:
    class _HTTPClientErrorJson(HTTPClientError):
        def __init__(self, message, *, details=None, headers=None):
            data = {'message': message}
            if details:
                data['details'] = details
            super().__init__(text=pretty_lenient_json(data), content_type=CONTENT_TYPE_JSON, headers=headers)

    class HTTPBadRequest(_HTTPClientErrorJson):
        status_code = 400

    class HTTPUnauthorized(_HTTPClientErrorJson):
        status_code = 401

    class HTTPPaymentRequired(_HTTPClientErrorJson):
        status_code = 402

    class HTTPForbidden(_HTTPClientErrorJson):
        status_code = 403

    class HTTPNotFound(_HTTPClientErrorJson):
        status_code = 404

    class HTTPConflict(_HTTPClientErrorJson):
        status_code = 409

    class HTTP470(_HTTPClientErrorJson):
        status_code = 470


@dataclass
class PreResponse:
    text: str = None
    body: bytes = None
    status: int = 200
    content_type: str = 'text/plain'
    headers: Dict[str, str] = None


class View:
    headers = None

    def __init__(self, request):
        self.request: Request = request
        self.app: Application = request.app
        self.settings: Settings = self.app['settings']
        self.session: Optional[Session] = None
        self.redis: ArqRedis = self.app['redis']

    def full_url(self, path=''):
        return Markup(f'{self.request.scheme}://{self.request.host}{path}')

    @classmethod
    def view(cls):
        async def view(request):
            self = cls(request)
            await self.authenticate(request)
            return await self._raw_call(request)

        view.view_class = cls

        # take name and docstring from class
        update_wrapper(view, cls, updated=())

        # and possible attributes set by decorators
        update_wrapper(view, cls.call, assigned=())
        return view

    async def _raw_call(self, request):
        try:
            if request.method in {METH_GET, METH_OPTIONS, METH_HEAD}:
                r = await self.call(request)
            else:
                r = await shield(self.call(request))
        except HTTPClientError as e:
            if self.headers:
                e.headers.update(self.headers)
            raise e

        return self._modify_response(request, r)

    @classmethod
    def _modify_response(cls, request, response):
        if isinstance(response, PreResponse):
            if response.text:
                body = response.text.encode()
            elif response.body:
                body = response.body
            else:
                raise RuntimeError('either body or text are required on PreResponse')
            response = Response(
                body=body, status=response.status, headers=response.headers, content_type=response.content_type
            )

        if cls.headers:
            response.headers.update(cls.headers)
        return response

    async def authenticate(self, request):
        pass

    async def call(self, request):
        raise NotImplementedError()

    async def request_data(self, model: Type[BaseModel]) -> AModel:
        error_details = None
        try:
            data = await self.request.json()
        except ValueError:
            error_msg = 'Error decoding JSON'
        else:
            try:
                return model.parse_obj(data)
            except ValidationError as e:
                error_msg = 'Invalid Data'
                error_details = e.errors()

        raise JsonErrors.HTTPBadRequest(message=error_msg, details=error_details)

    def get_arg_int(self, name, default=None):
        v = self.request.query.get(name)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            raise JsonErrors.HTTPBadRequest(f"invalid get argument '{name}': {v!r}")

    @classmethod
    def json_response(cls, *, status_=200, json_str_=None, headers_=None, **data):
        if not json_str_:
            json_str_ = ujson.dumps(data)
        return Response(text=json_str_, status=status_, content_type=CONTENT_TYPE_JSON, headers=headers_)


class AuthView(View):
    """
    token authentication with no "Token " prefix
    """

    auth_token_field = None

    async def authenticate(self, request):
        auth_token = getattr(self.settings, self.auth_token_field)
        if not secrets.compare_digest(auth_token, request.headers.get('Authorization', '')):
            raise JsonErrors.HTTPForbidden('Invalid Authorization header')


class ServiceView(AuthView):
    """
    Views used by services. Services are in charge and can be trusted to do "whatever they like".
    """

    auth_token_field = 'auth_key'


class UserView(View):
    """
    Views used by users via ajax
    """

    headers = {'Access-Control-Allow-Origin': '*'}

    async def authenticate(self, request):
        company = request.query.get('company', None)
        expires = request.query.get('expires', None)
        body = f'{company}:{expires}'.encode()
        expected_sig = hmac.new(self.settings.user_auth_key, body, hashlib.sha256).hexdigest()
        signature = request.query.get('signature', '-')
        if not secrets.compare_digest(expected_sig, signature):
            raise JsonErrors.HTTPForbidden('Invalid token', headers=self.headers)

        try:
            self.session = Session(company=company, expires=expires)
        except ValidationError as e:
            raise JsonErrors.HTTPBadRequest(message='Invalid Data', details=e.errors(), headers=self.headers)
        if self.session.expires < datetime.utcnow().replace(tzinfo=timezone.utc):
            raise JsonErrors.HTTPForbidden('token expired', headers=self.headers)


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
            raise JsonErrors.HTTPUnauthorized('Invalid basic auth', headers={'WWW-Authenticate': 'Basic'})


class TemplateView(View):
    template = None

    @classmethod
    def _modify_response(cls, request, context):
        status = context.pop('http_status_', None)
        response = render_template(cls.template, request, context)
        if status:
            response.set_status(status)
        return super()._modify_response(request, response)


class AdminView(TemplateView, BasicAuthView):
    template = 'admin-list.jinja'

    async def get_context(self, morpheus_api):
        raise NotImplementedError()

    async def call(self, request):
        morpheus_api = self.app['morpheus_api']
        ctx = dict(
            methods=[m.value for m in SendMethod],
            method=self.request.match_info.get(
                'method', self.request.query.get('method', SendMethod.email_mandrill.value)
            ),
        )
        try:
            ctx.update(await self.get_context(morpheus_api))
        except ApiError as e:
            raise JsonErrors.HTTPBadRequest(str(e))
        return ctx
