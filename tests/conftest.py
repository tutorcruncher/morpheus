import asyncio
import re
import uuid

import pytest
from aiohttp.test_utils import teardown_test_loop
from aioredis import create_redis
from arq import ArqRedis, Worker
from atoolbox.db import prepare_database
from atoolbox.db.helpers import DummyPgPool
from atoolbox.test_utils import DummyServer, create_dummy_server
from buildpg import Values, asyncpg

from morpheus.app.main import create_app
from morpheus.app.models import EmailSendModel, SendMethod
from morpheus.app.settings import Settings
from morpheus.app.worker import startup as worker_startup, worker_functions

from . import dummy_server


def pytest_addoption(parser):
    parser.addoption('--reuse-db', action='store_true', default=False, help='keep the existing database if it exists')


pg_settings = dict(APP_PG_DSN='postgres://postgres:waffle@localhost:5432/morpheus_test')


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


@pytest.yield_fixture
async def redis(loop, settings):
    addr = settings.redis_settings.host, settings.redis_settings.port

    redis = await create_redis(addr, db=settings.redis_settings.database, encoding='utf8', commands_factory=ArqRedis)
    await redis.flushdb()

    yield redis

    redis.close()
    await redis.wait_closed()


@pytest.fixture(name='dummy_server')
async def _fix_dummy_server(aiohttp_server):
    ctx = {'mandrill_subaccounts': {}}
    return await create_dummy_server(aiohttp_server, extra_routes=dummy_server.routes, extra_context=ctx)


@pytest.fixture
def settings(tmpdir, dummy_server: DummyServer):
    return Settings(
        **pg_settings,
        auth_key='testing-key',
        test_output=str(tmpdir),
        pdf_generation_url=dummy_server.server_name + '/generate.pdf',
        mandrill_key='good-mandrill-testing-key',
        log_level='ERROR',
        mandrill_url=dummy_server.server_name + '/mandrill',
        mandrill_timeout=0.5,
        host_name=None,
        click_host_name='click.example.com',
        messagebird_key='good-messagebird-testing-key',
        messagebird_url=dummy_server.server_name + '/messagebird',
        messagebird_pricing_api=dummy_server.server_name + '/messagebird-pricing',
        messagebird_pricing_username='mb-username',
        messagebird_pricing_password='mb-password',
        stats_token='test-token',
        max_request_stats=10,
    )


@pytest.fixture(name='cli')
async def _fix_cli(loop, test_client, settings, db_conn, redis):
    async def pre_startup(app):
        app.update(redis=redis, pg=DummyPgPool(db_conn))

    app = create_app(settings=settings)
    app.update(pg=DummyPgPool(db_conn), webhook_auth_key=b'testing')
    app.on_startup.insert(0, pre_startup)
    cli = await test_client(app)
    cli.server.app['morpheus_api'].root = f'http://localhost:{cli.server.port}/'
    return cli


@pytest.fixture
def send_email(cli, worker):
    async def _send_message(status_code=201, **extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='<body>\n{{{ message }}}\n</body>',
            company_code='foobar',
            from_address='Sender Name <sender@example.com>',
            method='email-test',
            subject_template='test message',
            context={'message': 'this is a test'},
            recipients=[{'address': 'foobar@testing.com'}],
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == status_code
        await worker.run_check()
        if len(data['recipients']) != 1:
            return NotImplemented
        else:
            return re.sub(r'[^a-zA-Z0-9\-]', '', f'{data["uid"]}-{data["recipients"][0]["address"]}')

    return _send_message


@pytest.fixture
def send_sms(cli, worker):
    async def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='this is a test {{ variable }}',
            company_code='foobar',
            from_name='FooBar',
            method='sms-test',
            context={'variable': 'apples'},
            recipients=[{'number': '07896541236'}],
        )
        # assert all(e in data for e in extra), f'{extra.keys()} fields not in {data.keys()}'
        data.update(**extra)
        r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status == 201
        await worker.run_check()
        return data['uid'] + '-447896541236'

    return _send_message


@pytest.yield_fixture(name='worker_ctx')
async def _fix_worker_ctx(settings, db_conn):
    ctx = dict(settings=settings, pg=DummyPgPool(db_conn))
    await worker_startup(ctx)

    yield ctx

    await asyncio.gather(ctx['session'].close(), ctx['mandrill'].close(), ctx['messagebird'].close())


@pytest.yield_fixture(name='worker')
async def _fix_worker(cli, worker_ctx):
    worker = Worker(
        functions=worker_functions, redis_pool=cli.server.app['redis'], burst=True, poll_delay=0.01, ctx=worker_ctx
    )

    yield worker

    worker.pool = None
    await worker.close()


@pytest.fixture(name='call_send_emails')
def _fix_call_send_emails(db_conn):
    async def run(**kwargs):
        base_kwargs = dict(
            uid=str(uuid.uuid4()),
            subject_template='hello',
            company_code='test',
            from_address='testing@example.com',
            method=SendMethod.email_mandrill,
            recipients=[],
        )
        m = EmailSendModel(**dict(base_kwargs, **kwargs))
        group_id = await db_conn.fetchval_b(
            'insert into message_groups (:values__names) values :values returning id',
            values=Values(
                uuid=m.uid,
                company=m.company_code,
                method=m.method.value,
                from_email=m.from_address.email,
                from_name=m.from_address.name,
            ),
        )
        return group_id, m

    return run
