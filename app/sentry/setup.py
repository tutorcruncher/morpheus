import sentry_sdk

from app.core.config import settings


def init_sentry() -> None:
    if not settings.sentry_dsn:
        return
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        release=settings.release,
        environment='production' if not settings.dev_mode else 'development',
        send_default_pii=False,
    )
