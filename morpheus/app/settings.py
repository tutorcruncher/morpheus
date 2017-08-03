from pathlib import Path

from arq import RedisSettings
from pydantic import BaseSettings, NoneStr, PyObject


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

    auth_key: str = 'testing'

    host_name: NoneStr = 'localhost'
    click_host_name: str = 'click.example.com'
    sender_cls: PyObject = 'morpheus.app.worker.Sender'
    mandrill_key: str = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    raven_dsn: str = None
    log_level = 'INFO'
    commit: str = '-'
    release_date: str = '-'
    elastic_host = 'localhost'
    elastic_port = 9200
    user_auth_key: bytes = b'insecure'
    admin_basic_auth_password = 'testing'
    test_output: Path = None
    pdf_generation_url: str = 'http://pdf/generate.pdf'
    local_api_url: str = 'http://localhost:8000'
    public_local_api_url: str = 'http://localhost:5000'

    # WARNING without setting a token here the stats page will be publicly viewable
    stats_token: str = ''
    max_request_stats = int(1e5)

    # used for es snapshots
    s3_access_key: str = None
    s3_secret_key: str = None
    snapshot_repo_name = 'morpheus'

    # message bird
    messagebird_key: str = ''
    messagebird_url: str = 'https://rest.messagebird.com'

    messagebird_pricing_api = 'https://api.mobiletulip.com/api/coverage/json/'
    messagebird_pricing_username: str = None
    messagebird_pricing_password: str = None

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
