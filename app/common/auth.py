import hashlib
import hmac
from datetime import datetime, timezone

from fastapi import Request
from pydantic import BaseModel, field_validator, model_validator

from app.common.api.errors import HTTP403
from app.core.config import settings


class AdminAuth:
    def __init__(self, request: Request):
        if request.headers.get('Authorization', '') != settings.auth_key:
            raise HTTP403('Invalid token')


class UserSession(BaseModel):
    company: str
    expires: datetime
    signature: str

    @field_validator('expires')
    @classmethod
    def expires_check(cls, v: datetime) -> datetime:
        now = datetime.now().replace(tzinfo=timezone.utc)
        if v < now:
            raise HTTP403('Token expired')
        return v

    @model_validator(mode='after')
    def sig_check(self) -> 'UserSession':
        expected_sig = hmac.new(
            settings.user_auth_key,
            f'{self.company}:{self.expires.timestamp():.0f}'.encode(),
            hashlib.sha256,
        ).hexdigest()
        if self.signature != expected_sig:
            raise HTTP403('Invalid token')
        return self
