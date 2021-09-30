from dataclasses import dataclass

from foxglove import glove
from foxglove.exceptions import HttpForbidden
from starlette.requests import Request


@dataclass(init=False)
class AdminAuth:
    def __init__(self, request: Request):
        if request.headers.get('Authorization', '') != glove.settings.auth_key:
            raise HttpForbidden('Invalid token')
