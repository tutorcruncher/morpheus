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
