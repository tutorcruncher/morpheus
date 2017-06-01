import pytest

from morpheus.main import create_app
from morpheus.settings import Settings


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def cli(loop, test_client, settings):
    async def modify_startup(app):
        # app['pg_engine'] = TestEngine(db_conn)
        app['sender']._concurrency_enabled = False
        await app['sender'].startup()
        # app['worker'].pg_engine = app['pg_engine']
        redis_pool = await app['sender'].get_redis_pool()
        async with redis_pool.get() as redis:
            await redis.flushdb()

    async def shutdown(app):
        await app['sender'].shutdown()

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    app.on_shutdown.append(shutdown)
    return loop.run_until_complete(test_client(app))
