from foxglove import BaseSettings
from pathlib import Path
from pydantic import NoneStr, validator
from typing import List

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    pg_dsn = 'postgresql://postgres@localhost:5432/morpheus'
    sql_path: Path = THIS_DIR / 'models.sql'
    patch_paths: List[str] = ['app.patches']

    cookie_name = 'morpheus'
    auth_key = 'insecure'
    app: str = 'src.main:app'

    locale = ''  # Required, don't delete
    host_name: NoneStr = 'localhost'
    click_host_name = 'click.example.com'
    mandrill_key = ''
    mandrill_url = 'https://mandrillapp.com/api/1.0'
    mandrill_webhook_key: str = ''
    log_level = 'INFO'
    verbose_http_errors = True
    user_auth_key: bytes = b'insecure'
    # Used to sign Mandrill webhooks
    webhook_auth_key: bytes = b'insecure'

    worker_func = 'src.worker:main'
    admin_basic_auth_password = 'testing'
    test_output: Path = None

    delete_old_emails: bool = False
    update_aggregation_view: bool = False
    pg_server_settings: dict = {}

    # messagebird
    messagebird_key = ''
    messagebird_url = 'https://rest.messagebird.com'

    # Have to use a US number as the originator to send to the US
    # https://support.messagebird.com/hc/en-us/articles/208747865-United-States
    us_send_number = '15744445663'
    canada_send_number = '12048170659'
    tc_registered_originator = 'TtrCrnchr'

    @validator('pg_dsn')
    def heroku_ready_pg_dsn(cls, v):
        return v.replace('gres://', 'gresql://')

    @property
    def mandrill_webhook_url(self):
        return f'https://{self.host_name}/webhook/mandrill/'

    class Config:
        fields = {
            'port': {'env': 'PORT'},
            'pg_dsn': {'env': 'DATABASE_URL'},
            'redis_settings': {'env': ['REDISCLOUD_URL', 'REDIS_URL']},
            'sentry_dsn': {'env': 'SENTRY_DSN'},
            'delete_old_emails': {'env': 'DELETE_OLD_EMAILS'},
            'update_aggregation_view': {'env': 'UPDATE_AGGREGATION_VIEW'},
            'release': {'env': ['COMMIT', 'RELEASE', 'HEROKU_SLUG_COMMIT']},
            'messagebird_key': {'env': 'MESSAGEBIRD_KEY'},
            'mandrill_key': {'env': 'MANDRILL_KEY'},
            'stats_token': {'env': 'STATS_TOKEN'},
            'click_host_name': {'env': 'CLICK_HOST_NAME'},
            'mandrill_webhook_key': {'env': 'MANDRILL_WEBHOOK_KEY'},
            'auth_key': {'env': 'AUTH_KEY'},
            'user_auth_key': {'env': 'USER_AUTH_KEY'},
            'host_name': {'env': 'HOST_NAME'},
        }
