import pytest

from morpheus.main import create_app
from morpheus.settings import Settings


@pytest.fixture
def settings(tmpdir):
    return Settings(
        auth_key='testing-key',
        test_output=str(tmpdir),
        mandrill_key='testing',
        log_level='ERROR',
    )


@pytest.fixture
def cli(loop, test_client, settings):
    async def modify_startup(app):
        app['sender']._concurrency_enabled = False
        await app['sender'].startup()
        redis_pool = await app['sender'].get_redis_pool()
        async with redis_pool.get() as redis:
            await redis.flushdb()
        await app['es'].create_indices(True)

    async def shutdown(app):
        await app['sender'].shutdown()

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    app.on_shutdown.append(shutdown)
    return loop.run_until_complete(test_client(app))


@pytest.fixture
def message_id(loop, cli):
    data = {
        'uid': 'x' * 20,
        'markdown_template': 'this is a test',
        'main_template': '<body>\n{{{ message }}}\n</body>',
        'company_code': 'foobar',
        'from_address': 'Sender Name <sender@example.com>',
        'method': 'email-test',
        'subject_template': 'test message',
        'recipients': [{'address': f'foobar@testing.com'}]
    }
    loop.run_until_complete(cli.post('/send/', json=data, headers={'Authorization': 'testing-key'}))
    return 'x' * 20 + '-foobartestingcom'
