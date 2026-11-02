"""Celery must not store task results in Redis.

Every send is already recorded in Postgres as a Message row and nothing reads Celery results. With a
Redis result backend and task_track_started, each task wrote a STARTED state and a
celery-task-meta-<id> key kept for a day, so a burst of ~26k send_email tasks pushed Redis to
maxmemory and every endpoint that enqueues a task returned 500.
"""

from celery.app.trace import build_tracer
from celery.backends.base import DisabledBackend

from app.core.celery import celery_app
from app.core.config import settings
from app.messages.tasks import get_redis, update_aggregation_view


def test_celery_ignores_results():
    assert celery_app.conf.task_ignore_result is True
    assert celery_app.conf.task_track_started is False
    assert isinstance(celery_app.backend, DisabledBackend)
    app_tasks = {name: t for name, t in celery_app.tasks.items() if name.startswith('app.')}
    assert app_tasks
    assert {name: t.ignore_result for name, t in app_tasks.items()} == {name: True for name in app_tasks}


def test_worker_task_run_stores_no_result(monkeypatch):
    """Run a task through the worker's (non-eager) tracer and check nothing lands in Redis."""
    monkeypatch.setattr(settings, 'update_aggregation_view', False)
    tracer = build_tracer(update_aggregation_view.name, update_aggregation_view, app=celery_app, eager=False)
    tracer('result-test-task-id', (), {}, {})
    assert get_redis().keys('celery-task-meta-*') == []
