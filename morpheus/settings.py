from arq import RedisSettings
from pydantic import BaseSettings, Module


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

    auth_key: str = ...

    sender_cls: Module = 'morpheus.worker.Sender'

    @property
    def redis_settings(self) -> RedisSettings:
        return RedisSettings(
            host=self.redis_host,
            port=self.redis_port,
            database=self.redis_database,
            password=self.redis_password,
        )
