from typing import List

from pydantic import BaseSettings


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_db = 0
    redis_password: str = None

    auth_keys: List[str] = []
