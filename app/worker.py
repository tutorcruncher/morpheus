"""Celery worker entry point that ensures all tasks are imported."""

from celery.signals import worker_process_init

from app.core.celery import celery_app
from app.messages import tasks  # noqa: F401  -- registers tasks with celery


@worker_process_init.connect
def init_worker_process(**kwargs):
    """Initialise Sentry and Logfire post-fork."""
    from app.core.logging import configure_logfire
    from app.sentry.setup import init_sentry

    init_sentry()
    configure_logfire()


app = celery_app


if __name__ == '__main__':
    celery_app.start()
