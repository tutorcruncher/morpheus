from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

THIS_DIR = Path(__file__).parent.parent.resolve()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    database_url: str = 'postgresql://postgres@localhost:5432/morpheus'
    # Heroku's rediscloud add-on sets REDISCLOUD_URL; honour it as the source of truth and
    # only fall back to REDIS_URL. AliasChoices checks the names in order, first found wins,
    # matching the legacy foxglove env=['REDISCLOUD_URL', 'REDIS_URL'] precedence.
    redis_url: str = Field(
        default='redis://localhost:6379/0',
        validation_alias=AliasChoices('REDISCLOUD_URL', 'REDIS_URL'),
    )

    auth_key: str = 'insecure'
    user_auth_key: bytes = b'insecure'
    webhook_auth_key: bytes = b'insecure'

    host_name: str | None = 'localhost'
    click_host_name: str = 'click.example.com'

    log_level: str = 'INFO'

    mandrill_key: str = ''
    mandrill_url: str = 'https://mandrillapp.com/api/1.0'
    mandrill_webhook_key: str = ''

    messagebird_key: str = ''
    messagebird_url: str = 'https://rest.messagebird.com'

    us_send_number: str = '15744445663'
    canada_send_number: str = '12048170659'
    tc_registered_originator: str = 'TtrCrnchr'

    admin_basic_auth_password: str = 'testing'
    test_output: Path | None = None

    delete_old_emails: bool = False
    update_aggregation_view: bool = False

    sentry_dsn: str | None = None
    logfire_token: str | None = None
    release: str | None = None
    commit: str | None = None
    release_date: str | None = None
    build_time: str | None = None

    testing: bool = False
    dev_mode: bool = False

    @field_validator('database_url')
    @classmethod
    def heroku_ready_database_url(cls, v: str) -> str:
        return v.replace('postgres://', 'postgresql://')

    @property
    def mandrill_webhook_url(self) -> str:
        return f'https://{self.host_name}/webhook/mandrill/'

    @property
    def pg_host(self) -> str:
        from urllib.parse import urlparse

        return urlparse(self.database_url).hostname or 'localhost'

    @property
    def pg_port(self) -> int:
        from urllib.parse import urlparse

        return urlparse(self.database_url).port or 5432

    @property
    def pg_name(self) -> str:
        from urllib.parse import urlparse

        return (urlparse(self.database_url).path or '/').lstrip('/')


settings = Settings()
