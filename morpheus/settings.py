from pathlib import Path

from pydantic import BaseSettings, PyObject

from arq import RedisSettings


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

    auth_key: str = ...

    sender_cls: PyObject = 'morpheus.worker.Sender'
    mandrill_key: str = ...
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    raven_dsn: str = None
    log_level = 'INFO'
    commit: str = '-'
    release_date: str = '-'
    server_name = '-'
    elastic_url = 'http://localhost:9200'
    elastic_username = 'elastic'
    elastic_password = 'changeme'
    user_fernet_key = b'i am not secure but 32 bits long'
    test_output: Path = '/tmp/morpheus/tests'

    @property
    def redis_settings(self) -> RedisSettings:
        return RedisSettings(
            host=self.redis_host,
            port=self.redis_port,
            database=self.redis_database,
            password=self.redis_password,
        )
