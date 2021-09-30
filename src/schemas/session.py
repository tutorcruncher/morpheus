import hashlib
import hmac
from datetime import datetime, timezone
from foxglove import glove
from foxglove.exceptions import HttpForbidden
from pydantic import BaseModel, validator


class Session(BaseModel):
    company: str
    expires: datetime


class UserSession(BaseModel):
    company: str
    expires: datetime
    signature: str

    @validator('expires')
    def expires_check(cls, v):
        if v < datetime.now().replace(tzinfo=timezone.utc):
            raise HttpForbidden('Token expired')
        return v

    @validator('signature')
    def sig_check(cls, v, values):
        if exp := values.get('expires'):
            expected_sig = hmac.new(
                glove.settings.user_auth_key, f'{values["company"]}:{exp.timestamp():.0f}'.encode(), hashlib.sha256
            ).hexdigest()
            if v != expected_sig:
                raise HttpForbidden('Invalid token')
