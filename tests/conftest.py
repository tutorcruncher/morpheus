import asyncio
import re
import uuid

import pytest
from aiohttp.test_utils import teardown_test_loop
from buildpg import asyncpg

from morpheus.app.db import SimplePgPool, prepare_database
from morpheus.app.main import create_app
from morpheus.app.settings import Settings

from .dummy_server import create_external_app


def pytest_addoption(parser):
    parser.addoption(
        '--reuse-db', action='store_true', default=False, help='keep the existing database if it exists'
    )


pg_settings = dict(
    pg_dsn='postgres://postgres:waffle@localhost:5432/morpheus_test',
    pg_name=None,
)


@pytest.fixture(scope='session', name='clean_db')
def _fix_clean_db(request):
    # loop fixture has function scope so can't be used here.
    settings = Settings(**pg_settings)
    loop = asyncio.new_event_loop()
    loop.run_until_complete(prepare_database(settings, not request.config.getoption('--reuse-db')))
    teardown_test_loop(loop)


@pytest.fixture(name='db_conn')
async def _fix_db_conn(loop, settings, clean_db):
    conn = await asyncpg.connect_b(dsn=settings.pg_dsn, loop=loop)

    tr = conn.transaction()
    await tr.start()

    await conn.execute("set client_min_messages = 'log'")

    yield conn

    await tr.rollback()
    await conn.close()


@pytest.fixture
async def mock_external(test_server):
    app = create_external_app()
    server = await test_server(app)
    app['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def settings(tmpdir, mock_external):
    return Settings(
        **pg_settings,
        auth_key='testing-key',
        test_output=str(tmpdir),
        pdf_generation_url=mock_external.app['server_name'] + '/generate.pdf',
        mandrill_key='good-mandrill-testing-key',
        log_level='ERROR',
        mandrill_url=mock_external.app['server_name'] + '/mandrill',
        host_name=None,
        click_host_name='click.example.com',
        messagebird_key='good-messagebird-testing-key',
        messagebird_url=mock_external.app['server_name'] + '/messagebird',
        messagebird_pricing_api=mock_external.app['server_name'] + '/messagebird-pricing',
        messagebird_pricing_username='mb-username',
        messagebird_pricing_password='mb-password',
        stats_token='test-token',
        max_request_stats=10,
    )


@pytest.fixture(name='cli')
async def _fix_cli(loop, test_client, settings, db_conn):
    async def modify_startup(app):
        app['sender']._concurrency_enabled = False
        await app['sender'].startup()
        redis = await app['sender'].get_redis()
        await redis.flushdb()

    async def shutdown(app):
        await app['sender'].shutdown()

    app = create_app(loop, settings=settings)
    app.update(
        pg=SimplePgPool(db_conn),
        webhook_auth_key=b'testing'
    )
    app.on_startup.append(modify_startup)
    app.on_shutdown.append(shutdown)
    cli = await test_client(app)
    cli.server.app['morpheus_api'].root = f'http://localhost:{cli.server.port}/'
    return cli


@pytest.fixture
def send_email(cli):
    async def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='<body>\n{{{ message }}}\n</body>',
            company_code='foobar',
            from_address='Sender Name <sender@example.com>',
            method='email-test',
            subject_template='test message',
            context={
                'message': 'this is a test'
            },
            recipients=[{'address': 'foobar@testing.com'}]
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == 201
        if len(data['recipients']) != 1:
            return NotImplemented
        else:
            return re.sub(r'[^a-zA-Z0-9\-]', '', f'{data["uid"]}-{data["recipients"][0]["address"]}')
    return _send_message


@pytest.fixture
def send_sms(cli):
    async def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='this is a test {{ variable }}',
            company_code='foobar',
            from_name='FooBar',
            method='sms-test',
            context={
                'variable': 'apples'
            },
            recipients=[{'number': '07896541236'}]
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == 201
        return data['uid'] + '-447896541236'
    return _send_message
