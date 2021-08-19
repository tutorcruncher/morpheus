from dataclasses import dataclass

from foxglove import glove
from foxglove.exceptions import HttpForbidden
from starlette.requests import Request

from src.db import SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@dataclass(init=False)
class AdminAuth:
    def __init__(self, request: Request):
        if request.headers.get('Authorization', '') != glove.settings.auth_key:
            raise HttpForbidden('Invalid token')