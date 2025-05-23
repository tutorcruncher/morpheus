import asyncio
import os
import pytest
import re
import uuid
from arq import Worker
from buildpg import Values, asyncpg
from buildpg.asyncpg import BuildPgConnection
from foxglove import glove
from foxglove.db import PgMiddleware, prepare_database
from foxglove.db.helpers import DummyPgPool, SyncDb
from foxglove.test_server import create_dummy_server
from httpx import URL, AsyncClient
from pathlib import Path
from starlette.testclient import TestClient
from typing import Any, Callable
from urllib.parse import urlencode

from src.schemas.messages import EmailSendModel, SendMethod
from src.settings import Settings
from src.worker import shutdown, startup, worker_settings

from . import dummy_server


@pytest.fixture(name='loop')
def fix_loop(settings):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


DB_DSN = os.getenv('DATABASE_URL', 'postgresql://postgres:waffle@localhost:5432/morpheus_test')


@pytest.fixture(name='settings')
def fix_settings(tmpdir):
    settings = Settings(
        dev_mode=False,
        test_mode=True,
        pg_dsn=DB_DSN,
        test_output=Path(tmpdir),
        delete_old_emails=True,
        update_aggregation_view=True,
        mandrill_url='http://localhost:8000/mandrill/',
        messagebird_url='http://localhost:8000/messagebird/',
        mandrill_key='good-mandrill-testing-key',
        mandrill_webhook_key='testing-mandrill-api-key',
        messagebird_key='good-messagebird-testing-key',
        auth_key='testing-key',
        secret_key='testkey',
        origin='https://example.com',
    )
    assert not settings.dev_mode
    glove._settings = settings

    yield settings
    glove._settings = None


@pytest.fixture(name='await_')
def fix_await(loop):
    return loop.run_until_complete


@pytest.fixture(name='raw_conn')
def fix_raw_conn(settings, await_: Callable):
    await_(prepare_database(settings, overwrite_existing=True, run_migrations=False))

    conn = await_(asyncpg.connect_b(dsn=settings.pg_dsn, server_settings={'jit': 'off'}))

    yield conn

    await_(conn.close())


@pytest.fixture(name='db_conn')
def fix_db_conn(settings, raw_conn: BuildPgConnection, await_: Callable):
    async def start():
        tr_ = raw_conn.transaction()
        await tr_.start()
        return tr_

    tr = await_(start())
    yield DummyPgPool(raw_conn)

    async def end():
        if not raw_conn.is_closed():
            await tr.rollback()

    await_(end())


@pytest.fixture(name='sync_db')
def fix_sync_db(db_conn, loop):
    return SyncDb(db_conn, loop)


@pytest.fixture(name='cli')
def fix_client(glove, settings: Settings, sync_db, worker):
    app = settings.create_app()
    app.user_middleware = []
    app.add_middleware(PgMiddleware)
    app.middleware_stack = app.build_middleware_stack()
    app.state.webhook_auth_key = b'testing'
    glove._settings = settings
    with TestClient(app) as client:
        yield client


class CustomAsyncClient(AsyncClient):
    def __init__(self, *args, settings, local_server, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings: Settings = settings
        self.scheme, host_port = local_server.split('://')
        self.host, port = host_port.split(':')
        self.port = int(port)

    def request(self, method, url, **kwargs):
        new_url = URL(url).copy_with(scheme=self.scheme, host=self.host, port=self.port)
        return super().request(method, new_url, **kwargs)


@pytest.fixture(name='dummy_server')
def _fix_dummy_server(loop, settings):
    ctx = {'mandrill_subaccounts': {}}
    ds = loop.run_until_complete(create_dummy_server(loop, extra_routes=dummy_server.routes, extra_context=ctx))

    custom_client = CustomAsyncClient(settings=settings, local_server=ds.server_name)
    glove._http = custom_client
    yield ds

    loop.run_until_complete(ds.stop())


class Worker4Testing(Worker):
    def test_run(self, max_jobs: int = None) -> int:
        return self.loop.run_until_complete(self.run_check(max_burst_jobs=max_jobs))

    def test_close(self) -> None:
        # pool is closed by glove, so don't want to mess with it here
        self._pool = None
        self.loop.run_until_complete(self.close())


@pytest.fixture(name='glove')
def fix_glove(db_conn, await_: Callable[..., Any]):
    glove.pg = db_conn

    async def start():
        await glove.startup(run_migrations=False)
        await glove.redis.flushdb()

    await_(start())

    yield glove

    await_(glove.shutdown())


@pytest.fixture(name='worker_ctx')
def _fix_worker_ctx(loop, settings):
    ctx = dict(settings=settings)
    loop.run_until_complete(startup(ctx))
    yield ctx


@pytest.fixture(name='worker')
def fix_worker(db_conn, glove, worker_ctx):
    functions = worker_settings['functions']
    worker = Worker4Testing(
        functions=functions,
        redis_pool=glove.redis,
        on_startup=startup,
        on_shutdown=shutdown,
        burst=True,
        poll_delay=0.001,
        ctx=worker_ctx,
    )

    yield worker

    worker.test_close()


@pytest.fixture()
def send_email(cli, worker, loop):
    def _send_email(status_code=201, **extra):
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
        data.update(**extra)
        r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status_code == status_code
        worker.test_run()
        if len(data['recipients']) != 1:
            return NotImplemented
        else:
            return re.sub(r'[^a-zA-Z0-9\-]', '', f'{data["uid"]}-{data["recipients"][0]["address"]}')

    return _send_email


@pytest.fixture
def send_sms(cli, worker, loop):
    def _send_message(**extra):
        data = dict(
            uid=str(uuid.uuid4()),
            main_template='this is a test {{ variable }}',
            company_code='foobar',
            from_name='FooBar',
            method='sms-test',
            context={'variable': 'apples'},
            recipients=[{'number': '07896541236'}],
        )
        data.update(**extra)
        r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
        assert r.status_code == 201
        worker.test_run()
        return data['uid'] + '-447896541236'

    return _send_message


@pytest.fixture
def send_webhook(cli, worker, loop):
    def _send_webhook(ext_id, price, **extra):
        url_args = {
            'id': ext_id,
            'reference': 'morpheus',
            'recipient': '447896541236',
            'status': 'delivered',
            'statusDatetime': '2032-06-06T12:00:00',
            'price[amount]': price,
        }

        url_args.update(**extra)
        r = cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
        assert r.status_code == 200
        worker.test_run()

    return _send_webhook


@pytest.fixture(name='call_send_emails')
def _fix_call_send_emails(glove, sync_db):
    def run(**kwargs):
        base_kwargs = dict(
            uid=str(uuid.uuid4()),
            subject_template='hello',
            company_code='test',
            from_address='testing@example.com',
            method=SendMethod.email_mandrill,
            recipients=[],
        )
        m = EmailSendModel(**dict(base_kwargs, **kwargs))
        company_id = sync_db.fetchval('insert into companies (code) values ($1) returning id', m.company_code)
        group_id = sync_db.fetchval_b(
            'insert into message_groups (:values__names) values :values returning id',
            values=Values(
                uuid=m.uid,
                company_id=company_id,
                message_method=m.method.value,
                from_email=m.from_address.email,
                from_name=m.from_address.name,
            ),
        )
        return group_id, company_id, m

    return run
