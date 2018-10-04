import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
from asyncio import shield
from datetime import datetime, timezone
from functools import update_wrapper
from pathlib import Path
from typing import Dict, Optional, Type, TypeVar
from urllib.parse import urlencode

import ujson
from aiohttp import ClientSession
from aiohttp.hdrs import METH_DELETE, METH_GET, METH_HEAD, METH_OPTIONS, METH_POST, METH_PUT
from aiohttp.web import Application, HTTPClientError, Request, Response
from aiohttp_jinja2 import render_template
from arq.utils import to_unix_ms
from dataclasses import dataclass
from markupsafe import Markup
from pydantic import BaseModel, ValidationError
from pydantic.json import pydantic_encoder

from .models import SendMethod
from .settings import Settings

THIS_DIR = Path(__file__).parent.resolve()
api_logger = logging.getLogger('morpheus.external')

CONTENT_TYPE_JSON = 'application/json'
AModel = TypeVar('AModel', bound=BaseModel)


class Session(BaseModel):
    company: str
    expires: datetime


def pretty_lenient_json(data):
    return json.dumps(data, indent=2, default=pydantic_encoder) + '\n'


class OkCancelError(asyncio.CancelledError):
    pass


class JsonErrors:
    class _HTTPClientErrorJson(HTTPClientError):
        def __init__(self, message, *, details=None, headers=None):
            data = {'message': message}
            if details:
                data['details'] = details
            super().__init__(
                text=pretty_lenient_json(data),
                content_type=CONTENT_TYPE_JSON,
                headers=headers,
            )

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
        from .worker import Sender  # noqa
        self.request: Request = request
        self.app: Application = request.app
        self.settings: Settings = self.app['settings']
        self.session: Optional[Session] = None
        self.sender: Sender = request.app['sender']

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
        except asyncio.CancelledError as e:
            # either the request was shielded or request didn't need shielding
            raise OkCancelError from e
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
            response = Response(body=body, status=response.status, headers=response.headers,
                                content_type=response.content_type)

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

        raise JsonErrors.HTTPBadRequest(
            message=error_msg,
            details=error_details,
        )

    async def decode_json(self):
        return await self.request.json(loads=ujson.loads)

    def get_arg_int(self, name, default=None):
        v = self.request.query.get(name)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            raise JsonErrors.HTTPBadRequest(f'invalid get argument "{name}": {v!r}')

    @classmethod
    def json_response(cls, *, status_=200, json_str_=None, headers_=None, **data):
        if not json_str_:
            json_str_ = ujson.dumps(data)
        return Response(
            text=json_str_,
            status=status_,
            content_type=CONTENT_TYPE_JSON,
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
            raise JsonErrors.HTTPForbidden('Invalid token')

        try:
            self.session = Session(
                company=company,
                expires=expires,
            )
        except ValidationError as e:
            raise JsonErrors.HTTPBadRequest(message='Invalid Data', details=e.errors())
        if self.session.expires < datetime.utcnow().replace(tzinfo=timezone.utc):
            raise JsonErrors.HTTPForbidden('token expired')


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
                'method',
                self.request.query.get('method', SendMethod.email_mandrill.value)
            ),
        )
        try:
            ctx.update(await self.get_context(morpheus_api))
        except ApiError as e:
            raise JsonErrors.HTTPBadRequest(str(e))
        return ctx


def lenient_json(v):
    if isinstance(v, (str, bytes)):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            pass
    return v


class ApiError(RuntimeError):
    def __init__(self, method, url, response):
        self.method = method
        self.url = url
        self.status = response.status

    def __str__(self):
        return f'{self.method} {self.url}, unexpected response {self.status}'


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

    async def _request(self, method, uri, allowed_statuses=(200, 201), **data) -> Response:
        method, url, data = self._modify_request(method, self.root + str(uri).lstrip('/'), data)
        headers = data.pop('headers_', {})
        timeout = data.pop('timeout_', 300)
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
            api_logger.warning('%s unexpected response %s /%s -> %s', self.__class__.__name__, method, uri, r.status,
                               extra={'data': data})
            raise ApiError(method, url, r)
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
