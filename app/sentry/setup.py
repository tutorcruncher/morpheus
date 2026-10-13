import sentry_sdk
from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

from app.core.config import settings

# DB driver and SQLAlchemy frame locals hold connection settings and bound SQL values; filter by name at any depth.
SCRUBBED_KEYS = [
    *DEFAULT_DENYLIST,
    'dsn',
    'parameters',
    'effective_parameters',
    'distilled_parameters',
    'distilled_params',
    'event_params',
    'args',
    'rows',
]


def init_sentry() -> None:
    if not settings.sentry_dsn:
        return
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        release=settings.release,
        environment='production' if not settings.dev_mode else 'development',
        send_default_pii=False,
        event_scrubber=EventScrubber(denylist=SCRUBBED_KEYS, recursive=True),
    )
