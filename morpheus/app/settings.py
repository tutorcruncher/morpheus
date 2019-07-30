from pathlib import Path

from arq.connections import RedisSettings
from atoolbox import BaseSettings
from pydantic import NoneStr

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent


class Settings(BaseSettings):
    create_app: str = 'app.main.create_app'
    worker_func = 'app.worker.run_worker'
    pg_dsn: str = 'postgres://postgres:waffle@localhost:5432/morpheus'
    sql_path: Path = 'app/sql/models.sql'
    logic_sql_path: Path = 'app/sql/logic.sql'
    patch_paths = ['app.patches']

    redis_settings: RedisSettings = 'redis://localhost:6379'

    cookie_name = 'morpheus'

    auth_key: str = 'testing'

    locale = ''

    deploy_name = 'testing'
    host_name: NoneStr = 'localhost'
    click_host_name: str = 'click.example.com'
    mandrill_key: str = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    mandrill_timeout = 30.0
    raven_dsn: str = None
    log_level = 'INFO'
    commit: str = 'unknown'
    build_time: str = 'unknown'
    release_date: str = 'unknown'
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

    class Config:
        fields = {'port': 'PORT', 'pg_dsn': 'APP_PG_DSN', 'redis_settings': 'APP_REDIS_SETTINGS'}
