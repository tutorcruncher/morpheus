import logging
import sys

from app.core.config import settings


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stdout,
    )


def configure_logfire() -> None:
    if not settings.logfire_token:
        return
    import logfire

    logfire.configure(token=settings.logfire_token, service_name='morpheus')
    logfire.instrument_httpx()
    # Per-process RSS/VMS so we can see what each web/worker process holds on the dyno
    logfire.instrument_system_metrics({'process.memory.usage': None, 'process.memory.virtual': None}, base='basic')
