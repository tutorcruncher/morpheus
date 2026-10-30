"""Regression tests for MORPHEUS-3DWV / MORPHEUS-3DWW: the in-flight task lost on Heroku's daily restart.

Heroku sends SIGTERM to every process in the worker dyno, not just the celery parent. Each prefork
child's SIGTERM handler was billiard's _shutdown_cleanup, so the children died mid-task, the parent
marked the busy child's job WorkerLostError and, with task_acks_late but no reject_on_worker_lost,
acked it: the task was dropped. The parent's own warm shutdown already finishes and acks in-flight
work, so the children must ignore SIGTERM and leave shutdown to the parent.
"""

import signal

from billiard.pool import Pool
from celery.concurrency.prefork import process_initializer

from app.worker import celery_app


def _sigterm_handler():
    return repr(signal.getsignal(signal.SIGTERM))


def test_prefork_child_ignores_sigterm(monkeypatch):
    """Fork a real pool child, so this also fails if a celery or billiard upgrade reorders the
    signal resets that run either side of the worker_process_init hook."""
    monkeypatch.setattr('app.core.logging.configure_logfire', lambda: None)
    monkeypatch.setattr('app.core.database.configure_worker_engine', lambda: None)

    pool = Pool(1, initializer=process_initializer, initargs=(celery_app, 'test'))
    try:
        assert pool.apply_async(_sigterm_handler).get(timeout=10) == repr(signal.SIG_IGN)
    finally:
        pool.close()
        pool.join()
