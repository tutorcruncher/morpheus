"""Celery worker entry point that ensures all tasks are imported."""

from celery.signals import worker_init, worker_process_init

from app.core.celery import celery_app
from app.messages import tasks  # noqa: F401  -- registers tasks with celery


@worker_init.connect
def init_worker_sentry(**kwargs):
    """Initialise Sentry before the prefork pool forks.

    Sentry's CeleryIntegration captures task exceptions by patching celery.app.trace.build_tracer
    at sentry_sdk.init() time. worker_init fires in the main process before the pool forks, so the
    patch is installed before any task tracer is built and is inherited by every child. Initialised
    later in worker_process_init (post-fork) the patch lands after the tracer is built and worker
    task exceptions never reach Sentry (issue #514).
    """
    from app.sentry.setup import init_sentry

    init_sentry()


@worker_process_init.connect
def init_worker_process(**kwargs):
    """Initialise Logfire and shrink the DB pool, per prefork child (post-fork)."""
    from app.core.database import configure_worker_engine
    from app.core.logging import configure_logfire

    configure_logfire()
    # A prefork child needs only a couple of connections; rebuild the engine off the parent's
    # web-sized pool so many children don't exhaust RDS max_connections (MORPHEUS-3DNG).
    configure_worker_engine()


app = celery_app


if __name__ == '__main__':
    celery_app.start()
