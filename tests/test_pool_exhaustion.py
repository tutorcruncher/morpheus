"""Regression tests for MORPHEUS-3DNG: DB connection-pool exhaustion under load.

The sync-endpoint morpheus instance served requests from Starlette's thread pool (default 40
threads) against SQLAlchemy's default 15-connection pool. Under cutover load — amplified by the
comms 503-retry storm and an async click handler that blocked the event loop on a synchronous DB
call — connection checkout stalled for the full 30s pool_timeout and collapsed.

These tests are written test-first and pin the fixes: a larger, configurable pool; the request
thread pool bounded to the pool's capacity; and a non-blocking (sync) click handler.
"""

import inspect

import anyio

from app.core.config import settings
from app.core.database import engine
from app.main import app, lifespan
from app.messages.api.common import click_redirect_view


def test_db_pool_is_configurable_and_larger_than_legacy_default():
    """The engine must honour the configurable pool settings, sized above the old 15 ceiling."""
    assert engine.pool.size() == settings.db_pool_size
    assert engine.pool._max_overflow == settings.db_max_overflow
    # Old default pool_size=5 + max_overflow=10 = 15 was far below the ~40-thread concurrency.
    assert settings.db_pool_size + settings.db_max_overflow > 15


def test_click_redirect_does_not_block_the_event_loop():
    """click_redirect_view runs a synchronous db.exec(); as an async route it ran on the event
    loop and froze every request during pool exhaustion. It must be a sync (threadpool) route."""
    assert not inspect.iscoroutinefunction(click_redirect_view)


def test_lifespan_caps_threadpool_to_pool_capacity():
    """Sync requests must not out-number the connections they each check out, otherwise surplus
    threads block for pool_timeout on checkout. The thread pool is bounded to the pool capacity."""

    async def _run():
        async with lifespan(app):
            limiter = anyio.to_thread.current_default_thread_limiter()
            assert limiter.total_tokens == settings.db_pool_size + settings.db_max_overflow

    anyio.run(_run)


def test_worker_pool_is_much_smaller_than_web_pool():
    """A celery prefork child (worker_prefetch_multiplier=1) runs one task at a time and opens one
    session at a time via get_session(), so it needs a tiny pool. Sizing every child at the web
    pool (20+10) multiplied per child × per worker dyno and, alongside both blue-green web colours,
    threatened RDS max_connections during cutover (MORPHEUS-3DNG)."""
    web_capacity = settings.db_pool_size + settings.db_max_overflow
    worker_capacity = settings.db_worker_pool_size + settings.db_worker_max_overflow
    assert worker_capacity < web_capacity
    # A prefork child only ever holds one connection at once; keep the per-child footprint small.
    assert worker_capacity <= 5


def test_configure_worker_engine_rebinds_a_small_pool():
    """After a worker forks, configure_worker_engine() must rebuild the engine + sessionmaker with
    the worker pool so get_session() draws from the small pool, not the inherited web pool."""
    from app.core import database as db_module

    saved = (db_module.engine, db_module.SessionLocal, db_module.SessionCls)
    try:
        db_module.configure_worker_engine()
        assert db_module.engine is not saved[0]
        assert db_module.engine.pool.size() == settings.db_worker_pool_size
        assert db_module.engine.pool._max_overflow == settings.db_worker_max_overflow
        # get_session() must hand out sessions bound to the new small-pool engine.
        session = db_module.SessionCls()
        try:
            assert session.get_bind() is db_module.engine
        finally:
            session.close()
    finally:
        db_module.engine, db_module.SessionLocal, db_module.SessionCls = saved


def test_click_redirect_releases_connection_before_enqueue(monkeypatch):
    """The click endpoint must close its DB session before the store_click.delay() broker
    round-trip and the redirect response, so the highest-volume endpoint doesn't hold a scarce
    connection idle for the rest of the request (MORPHEUS-3DNG)."""
    from app.messages.api import common

    events: list[str] = []

    class _FakeExec:
        def first(self):
            return (123, 'https://example.com/dest')

    class _FakeDB:
        def exec(self, *args, **kwargs):
            return _FakeExec()

        def close(self):
            events.append('close')

    def _fake_delay(**kwargs):
        events.append('delay')

    monkeypatch.setattr(common.store_click, 'delay', _fake_delay)

    common.click_redirect_view(
        token='abc',
        request=None,  # ty:ignore[invalid-argument-type]  -- unused on the link-found path
        u=None,
        X_Forwarded_For=None,
        X_Request_Start='.',
        User_Agent=None,
        db=_FakeDB(),  # ty:ignore[invalid-argument-type]
    )
    # The connection must be released before the broker round-trip, not after.
    assert events == ['close', 'delay']
