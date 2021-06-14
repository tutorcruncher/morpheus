from urllib.parse import urlparse

from arq.connections import RedisSettings
from atoolbox import BaseSettings
from pathlib import Path
from pydantic import NoneStr
from typing import List

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    create_app = 'app.main.create_app'
    worker_func = 'app.worker.run_worker'
    redis_url = 'redis://localhost:6379'
    pg_dsn = 'postgres://postgres:postgres@localhost:5432/morpheus'
    sql_path: Path = THIS_DIR / 'models.sql'
    patch_paths: List[str] = ['app.patches']

    cookie_name = 'morpheus'
    auth_key = 'testing'

    locale = ''  # Required, don't delete
    host_name: NoneStr = 'localhost'
    click_host_name = 'click.example.com'
    mandrill_key = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    mandrill_timeout = 30.0
    raven_dsn: str = None
    log_level = 'INFO'
    verbose_http_errors = True
    commit = 'unknown'
    build_time = 'unknown'
    release_date = 'unknown'
    user_auth_key: bytes = b'insecure'
    admin_basic_auth_password = 'testing'
    test_output: Path = None
    pdf_generation_url = 'http://pdf/generate.pdf'
    local_api_url = 'http://localhost:8000'
    public_local_api_url = 'http://localhost:5000'

    # WARNING without setting a token here the stats page will be publicly viewable
    stats_token = ''
    max_request_stats = int(1e5)

    # message bird
    messagebird_key = ''
    messagebird_url = 'https://rest.messagebird.com'

    # Have to use a US number as the originator to send to the US
    # https://support.messagebird.com/hc/en-us/articles/208747865-United-States
    us_send_number = '15744445663'

    @property
    def redis_settings(self):
        conf = urlparse(self.redis_url)
        return RedisSettings(
            host=conf.hostname, port=conf.port, password=conf.password, database=int((conf.path or '0').strip('/'))
        )

    class Config:
        fields = {
            'port': {'env': 'PORT'},
            'pg_dsn': {'env': 'DATABASE_URL'},
            'redis_url': {'env': 'REDIS_URL'},
            'commit': {'env': 'COMMIT'},
            'build_time': {'env': 'BUILD_TIME'},
            'messagebird_key': {'env': 'MESSAGEBIRD_KEY'},
            'stats_token': {'env': 'STATS_TOKEN'},
            'click_host_name': {'env': 'CLICK_HOST_NAME'},
            'sentry_dsn': {'env': 'SENTRY_DSN'},
        }
