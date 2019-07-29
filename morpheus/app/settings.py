import re
from pathlib import Path

from arq.connections import RedisSettings
from pydantic import BaseSettings, NoneStr, PyObject, validator

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent


class Settings(BaseSettings):
    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

    pg_dsn: str = 'postgres://postgres:waffle@localhost:5432/morpheus'
    pg_host: str = None
    pg_port: int = None
    pg_name: str = None

    auth_key: str = 'testing'

    deploy_name = 'testing'
    host_name: NoneStr = 'localhost'
    click_host_name: str = 'click.example.com'
    mandrill_key: str = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    raven_dsn: str = None
    log_level = 'INFO'
    commit: str = '-'
    release_date: str = '-'
    user_auth_key: bytes = b'insecure'
    admin_basic_auth_password = 'testing'
    test_output: Path = None
    pdf_generation_url: str = 'http://pdf/generate.pdf'
    local_api_url: str = 'http://localhost:8000'
    public_local_api_url: str = 'http://localhost:5000'

    # WARNING without setting a token here the stats page will be publicly viewable
    stats_token: str = ''
    max_request_stats = int(1e5)

    # message bird
    messagebird_key: str = ''
    messagebird_url: str = 'https://rest.messagebird.com'

    messagebird_pricing_api = 'https://api.mobiletulip.com/api/coverage/json/'
    messagebird_pricing_username: str = None
    messagebird_pricing_password: str = None

    # Have to use a US number as the originator to send to the US
    # https://support.messagebird.com/hc/en-us/articles/208747865-United-States
    us_send_number = '15744445663'

    @property
    def redis_settings(self) -> RedisSettings:
        return RedisSettings(
            host=self.redis_host, port=self.redis_port, database=self.redis_database, password=self.redis_password
        )

    @validator('pg_host', always=True, pre=True)
    def set_pg_host(cls, v, values, **kwargs):
        return re.search(r'@(.+?):', values['pg_dsn']).group(1)

    @validator('pg_port', always=True, pre=True)
    def set_pg_port(cls, v, values, **kwargs):
        return int(re.search(r':(\d+)', values['pg_dsn']).group(1))

    @validator('pg_name', always=True, pre=True)
    def set_pg_name(cls, v, values, **kwargs):
        return re.search(r'\d+/(\w+)$', values['pg_dsn']).group(1)

    @property
    def models_sql(self):
        return (THIS_DIR / 'sql' / 'models.sql').read_text()

    @property
    def logic_sql(self):
        return (THIS_DIR / 'sql' / 'logic.sql').read_text()
