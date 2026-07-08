"""Regression tests for issue #511: boot-time bootstrap DDL blocked messages/events.

The FastAPI lifespan ran create_db_and_tables() on every web boot, which takes ACCESS EXCLUSIVE
locks on the hot messages/events tables via DROP/CREATE TRIGGER. When a long-lived lock holder was
present, the boot hung on the lock, dammed the whole lock queue behind the waiting exclusive
request, and Heroku SIGKILLed the dyno at the boot deadline — a site-wide outage.

These tests pin the two fixes: bootstrap is off by default in the lifespan, and when it does run it
carries a lock_timeout so it fails loudly instead of hanging.
"""

import anyio
import psycopg2
import pytest

from app import main as app_main
from app.core import database as db_module
from app.core.config import settings


def _enter_lifespan() -> None:
    async def _run():
        async with app_main.lifespan(app_main.app):
            pass

    anyio.run(_run)


def test_lifespan_skips_bootstrap_when_flag_off(monkeypatch):
    """Default production config: the lifespan must not touch the DB before binding the port."""
    called = []
    monkeypatch.setattr(app_main, 'create_db_and_tables', lambda: called.append(True))
    monkeypatch.setattr(settings, 'db_bootstrap_on_startup', False)
    _enter_lifespan()
    assert called == []


def test_lifespan_runs_bootstrap_when_flag_on(monkeypatch):
    """With the flag on (first-time/dev setup) the lifespan runs the bootstrap."""
    called = []
    monkeypatch.setattr(app_main, 'create_db_and_tables', lambda: called.append(True))
    monkeypatch.setattr(settings, 'db_bootstrap_on_startup', True)
    _enter_lifespan()
    assert called == [True]


@pytest.mark.timeout(15)
def test_create_db_and_tables_fails_fast_under_conflicting_lock(monkeypatch):
    """A conflicting ACCESS EXCLUSIVE lock on events must make bootstrap raise within the
    lock_timeout, not hang for the whole boot deadline (issue #511)."""
    monkeypatch.setattr(db_module, 'BOOTSTRAP_LOCK_TIMEOUT', '200ms')

    blocker = db_module.engine.raw_connection()
    try:
        with blocker.cursor() as cur:
            cur.execute('BEGIN')
            cur.execute('LOCK TABLE events IN ACCESS EXCLUSIVE MODE')
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                db_module.create_db_and_tables()
    finally:
        blocker.rollback()
        blocker.close()
