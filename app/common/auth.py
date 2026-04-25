import hashlib
import hmac
from datetime import datetime, timezone
from typing import Optional

from fastapi import Request
from pydantic import BaseModel, ValidationError, field_validator

from app.common.api.errors import HTTP403
from app.core.config import settings


class AdminAuth:
    def __init__(self, request: Request):
        if request.headers.get('Authorization', '') != settings.auth_key:
            raise HTTP403('Invalid token')


class _UserSessionData(BaseModel):
    company: str
    expires: datetime
    signature: str

    @field_validator('expires')
    @classmethod
    def add_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v


class UserSession:
    """Validates a signed query-string session and exposes `company`/`expires`.

    Implemented as a callable dependency rather than a Pydantic model so that
    auth failures surface as a 403 with a `{'message': ...}` body, not a
    Pydantic 422 ValidationError. Pydantic v2 catches HTTPException raised
    inside validators and re-wraps as ValidationError, which is the wrong
    failure mode here.
    """

    def __init__(
        self,
        company: Optional[str] = None,
        expires: Optional[datetime] = None,
        signature: Optional[str] = None,
    ):
        try:
            data = _UserSessionData(company=company, expires=expires, signature=signature)
        except ValidationError:
            raise HTTP403('Invalid token')

        if data.expires < datetime.now(tz=timezone.utc):
            raise HTTP403('Token expired')

        expected_sig = hmac.new(
            settings.user_auth_key,
            f'{data.company}:{data.expires.timestamp():.0f}'.encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(data.signature, expected_sig):
            raise HTTP403('Invalid token')

        self.company = data.company
        self.expires = data.expires
        self.signature = data.signature
