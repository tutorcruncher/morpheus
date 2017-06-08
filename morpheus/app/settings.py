from pathlib import Path

from arq import RedisSettings
from pydantic import BaseSettings, NoneStr, PyObject


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

    auth_key: str = 'testing'

    host_name = 'localhost'
    sender_cls: PyObject = 'morpheus.app.worker.Sender'
    mandrill_key: str = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    raven_dsn: str = None
    log_level = 'INFO'
    commit: str = '-'
    release_date: str = '-'
    elastic_host = 'localhost'
    elastic_port = 9200
    user_fernet_key = b'i am not secure but 32 bits long'
    test_output: Path = '/tmp/morpheus/tests'
    pdf_generation_url: NoneStr = 'http://pdf/generate.pdf'

    @property
    def redis_settings(self) -> RedisSettings:
        return RedisSettings(
            host=self.redis_host,
            port=self.redis_port,
            database=self.redis_database,
            password=self.redis_password,
        )

    @property
    def elastic_url(self):
        return f'http://{self.elastic_host}:{self.elastic_port}'
