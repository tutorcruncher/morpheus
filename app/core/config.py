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

    # DB connection pool. The old defaults (pool_size=5 + max_overflow=10 = 15) sat far below the
    # request concurrency of the sync endpoints (Starlette's thread pool defaults to 40), so cutover
    # bursts exhausted the pool and connection checkout blocked for pool_timeout seconds before
    # failing (MORPHEUS-3DNG). These are env-tunable: size them against RDS max_connections given the
    # number of web + worker processes sharing the database.
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_timeout: int = 30
    # Bootstrap DDL (create_db_and_tables) runs DROP/CREATE TRIGGER on the hot messages/events
    # tables, taking ACCESS EXCLUSIVE locks. Running it on every web boot against the shared prod
    # DB meant one long-lived lock holder turned a dyno boot into a site-wide lock-queue dam
    # (see issue #511). Off by default: production schema changes go through a deliberate one-off
    # (e.g. `heroku run python -c "from app.core.database import create_db_and_tables; create_db_and_tables()"`),
    # not implicitly on dyno start. Set true for first-time local/dev setup.
    db_bootstrap_on_startup: bool = False
    # Celery prefork children each run ONE task at a time (worker_prefetch_multiplier=1) and open
    # one session at a time, so a child needs only a couple of connections. The web pool is per
    # process; a worker sized at the web pool would multiply (children × dynos) and, next to both
    # blue-green web colours, blow past RDS max_connections during cutover (MORPHEUS-3DNG). Workers
    # rebuild the engine post-fork with these; see database.configure_worker_engine.
    db_worker_pool_size: int = 2
    db_worker_max_overflow: int = 2

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
    # Defaults on: the email-analytics aggregation endpoint reads the message_aggregation
    # materialized view, which is only kept current by the hourly refresh task. With this off
    # the view goes stale and analytics shows no data.
    update_aggregation_view: bool = True

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
