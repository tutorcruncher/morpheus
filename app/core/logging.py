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
    # Task spans on the worker (runs post-fork via worker_process_init) and publish
    # spans on the web producer, so celery activity is visible in Logfire.
    logfire.instrument_celery()
    # Forward stdlib logging to Logfire so task-level warnings (e.g. skipped
    # unrecognised mandrill webhook events) are visible, not just stdout.
    logging.getLogger().addHandler(logfire.LoggingHandler())
    # Per-process RSS/VMS so we can see what each web/worker process holds on the dyno
    logfire.instrument_system_metrics({'process.memory.usage': None, 'process.memory.virtual': None}, base='basic')
