"""Regression tests for MORPHEUS-3DWV / MORPHEUS-3DWW: the in-flight task lost on Heroku's daily restart.

Heroku sends SIGTERM to every process in the worker dyno, not just the celery parent. Each prefork
child's SIGTERM handler was billiard's _shutdown_cleanup, so the children died mid-task, the parent
marked the busy child's job WorkerLostError and, with task_acks_late but no reject_on_worker_lost,
acked it: the task was dropped. The parent's own warm shutdown already finishes and acks in-flight
work, so the children must ignore SIGTERM and leave shutdown to the parent.
"""

import signal

from billiard.common import reset_signals
from celery.signals import worker_process_init

import app.worker  # noqa: F401  -- connects the worker_process_init receiver
from app.core import database, logging


def test_prefork_child_ignores_sigterm(monkeypatch):
    """Celery resets a child's SIGTERM to SIG_DFL, sends worker_process_init, then billiard's
    reset_signals() installs _shutdown_cleanup on any SIGTERM that isn't SIG_IGN. Replay that order
    and check the child ends up ignoring SIGTERM."""
    monkeypatch.setattr(logging, 'configure_logfire', lambda: None)
    monkeypatch.setattr(database, 'configure_worker_engine', lambda: None)

    saved = signal.getsignal(signal.SIGTERM)
    try:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        worker_process_init.send(sender=None)
        reset_signals()
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    finally:
        signal.signal(signal.SIGTERM, saved)
