import asyncio

import arq
import pytest
import re
import uuid

from arq import create_pool, Worker
from foxglove.test_server import create_dummy_server
from httpx import AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from foxglove.testing import Client as TestClient

from src.db import prepare_database
from src.main import app, glove
from src.models import MessageGroup, Company
from src.schema import EmailSendModel, SendMethod
from src.settings import Settings
from src.utils import get_db
from src.worker import startup as worker_startup, worker_functions

from . import dummy_server


@pytest.fixture(name='loop')
def fix_loop(settings):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


DB_DSN = 'postgresql://postgres@localhost:5432/morpheus_test'

engine = create_engine(DB_DSN)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(name='settings')
def fix_settings(tmpdir):
    settings = Settings(
        dev_mode=False,
        test_mode=True,
        pg_dsn=DB_DSN,
        test_output=str(tmpdir),
        mandrill_url='http://localhost:8000/mandrill/',
        mandrill_key='good-mandrill-testing-key',
        auth_key='testing-key',
        secret_key='testkey',
        origin='https://example.com',
    )
    assert not settings.dev_mode
    glove._settings = settings

    yield settings
    glove.pg = TestingSessionLocal()
    glove._settings = None


@pytest.fixture(name='db')
def clean_db(settings, loop):
    loop.run_until_complete(prepare_database(settings, True))
    yield from override_get_db()


@pytest.fixture(name='cli')
def client(loop):
    app.user_middleware = []
    app.middleware_stack = app.build_middleware_stack()
    app.state.webhook_auth_key = b'testing'
    with TestClient(app) as cli:
        yield cli


@pytest.fixture
def redis(loop, settings):
    redis = loop.run_until_complete(create_pool(settings.redis_settings))
    yield redis


class CustomAsyncClient(AsyncClient):
    def __init__(self, *args, settings, local_server, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings: Settings = settings
        self.scheme, host_port = local_server.split('://')
        self.host, port = host_port.split(':')
        self.port = int(port)

    def request(self, method, url, **kwargs):
        url = url.replace('8000', str(self.port))
        return super().request(method, url, **kwargs)


@pytest.fixture(name='dummy_server')
def _fix_dummy_server(loop, settings):
    ctx = {'mandrill_subaccounts': {}}
    ds = loop.run_until_complete(create_dummy_server(loop, extra_routes=dummy_server.routes, extra_context=ctx))

    custom_client = CustomAsyncClient(settings=settings, local_server=ds.server_name)
    glove._http = custom_client
    yield ds

    loop.run_until_complete(ds.stop())


@pytest.fixture(name='worker_ctx')
def _fix_worker_ctx(loop, settings):
    ctx = dict(settings=settings, pg=TestingSessionLocal())
    loop.run_until_complete(worker_startup(ctx))
    yield ctx


@pytest.fixture(name='worker')
def _fix_worker(loop, cli, worker_ctx):
    worker = Worker(
        functions=worker_functions,
        redis_pool=loop.run_until_complete(arq.create_pool(glove.settings.redis_settings)),
        burst=True,
        poll_delay=0.01,
        ctx=worker_ctx,
    )
    yield worker
    loop.run_until_complete(worker.close())


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
        loop.run_until_complete(worker.run_check())
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
        loop.run_until_complete(worker.run_check())
        return data['uid'] + '-447896541236'

    return _send_message


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
        company = Company.manager.create(db_conn, code=m.company_code)
        group = MessageGroup.manager.create(
            db_conn,
            uuid=m.uid,
            company_id=company.id,
            message_method=m.method.value,
            from_email=m.from_address.email,
            from_name=m.from_address.name,
        )
        return group.id, company.id, m

    return run
