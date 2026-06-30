import asyncio
import json
import re
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import pytest
from celery import current_app as celery_current_app
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core import database as db_module
from app.core.celery import celery_app
from app.core.config import settings as app_settings
from app.core.database import SessionLocal, engine, get_db
from app.ext import clients as clients_module
from app.main import app
from app.messages.models import Company, MessageGroup
from app.messages.schemas import EmailSendModel
from app.messages.tasks import SendEmail, get_redis
from tests import dummy_server

THIS_DIR = Path(__file__).parent.resolve()


def _truncate_all(conn) -> None:
    conn.execute(text('TRUNCATE TABLE links, events, messages, message_groups, companies RESTART IDENTITY CASCADE'))
    # The materialized view caches per-company message counts. Without this, tests that
    # re-use auto-incremented company IDs see stale aggregation data from earlier tests.
    conn.execute(text('REFRESH MATERIALIZED VIEW message_aggregation'))


@pytest.fixture(scope='session', autouse=True)
def _create_schema():
    """Create the schema once per test session. Tests reset state via TRUNCATE between tests."""
    db_module.create_db_and_tables()
    yield


@pytest.fixture(autouse=True)
def _eager_celery():
    celery_current_app.conf.task_always_eager = True
    celery_current_app.conf.task_eager_propagates = True
    yield
    celery_current_app.conf.task_always_eager = False


_TASK_COUNTER = {'count': 0}


@pytest.fixture(autouse=True)
def _reset_task_counter():
    _TASK_COUNTER['count'] = 0
    yield


@pytest.fixture(autouse=True)
def _patch_task_counter(monkeypatch):
    """Wrap each registered Celery task so we can count invocations."""
    originals = {}
    for task_name, task in list(celery_app.tasks.items()):
        if not task_name.startswith('app.'):
            continue
        original_run = task.run
        originals[task_name] = original_run

        def make_wrapped(orig):
            def _w(*args, **kwargs):
                _TASK_COUNTER['count'] += 1
                return orig(*args, **kwargs)

            return _w

        task.run = make_wrapped(original_run)

    yield

    for task_name, original_run in originals.items():
        celery_app.tasks[task_name].run = original_run


@pytest.fixture(autouse=True)
def _clean_redis():
    redis = get_redis()
    redis.flushdb()
    yield
    redis.flushdb()


class _SyncLoop:
    """Compatibility shim: tests use `loop.run_until_complete(coro)` extensively.

    The new app is sync, but legacy worker tests still build coroutines. This shim runs them inline.
    """

    def run_until_complete(self, awaitable):
        if asyncio.iscoroutine(awaitable) or asyncio.isfuture(awaitable):
            return asyncio.get_event_loop().run_until_complete(awaitable)
        return awaitable


@pytest.fixture
def loop():
    return _SyncLoop()


class _LegacyRetry(Exception):
    """Raised by run_send_email when the SendEmail logic asks for a retry.

    Mimics the legacy arq.Retry shape (`defer_score` in ms) so existing test assertions
    (`assert exc_info.value.defer_score == 5_000`) keep working.
    """

    def __init__(self, defer_score: int) -> None:
        self.defer_score = defer_score


class _LegacyTask:
    """Synthetic celery-task stand-in passed to SendEmail in direct-call tests."""

    def __init__(self, job_try: int) -> None:
        self.request = type('Req', (), {'retries': max(job_try - 1, 0)})()

    def retry(self, exc: Exception | None = None, countdown: int | None = None) -> None:
        raise _LegacyRetry(int((countdown or 0) * 1000))


@pytest.fixture
def worker_ctx(settings):
    """Compatibility shim: legacy tests expected an arq-style ctx dict.

    The new Celery worker doesn't use this — but tests still pass it through the
    `run_send_email` helper, which reads `job_try` from this dict to drive retry behaviour.
    """
    return {'job_try': 1, 'redis': None}


def _run_send_email(ctx, group_id, company_id, recipient, m):
    """Execute the SendEmail logic synchronously, mimicking the legacy worker function signature."""
    task = _LegacyTask(job_try=ctx.get('job_try', 1))
    SendEmail(task, group_id, company_id, recipient, m).run()


@pytest.fixture
def worker_send_email():
    return _run_send_email


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(app_settings, 'test_output', tmp_path)
    monkeypatch.setattr(app_settings, 'mandrill_url', 'http://dummy/mandrill/')
    monkeypatch.setattr(app_settings, 'messagebird_url', 'http://dummy/messagebird/')
    monkeypatch.setattr(app_settings, 'mandrill_key', 'good-mandrill-testing-key')
    monkeypatch.setattr(app_settings, 'mandrill_webhook_key', 'testing-mandrill-api-key')
    monkeypatch.setattr(app_settings, 'messagebird_key', 'good-messagebird-testing-key')
    monkeypatch.setattr(app_settings, 'auth_key', 'testing-key')
    monkeypatch.setattr(app_settings, 'host_name', 'localhost')
    monkeypatch.setattr(app_settings, 'click_host_name', 'click.example.com')
    monkeypatch.setattr(app_settings, 'delete_old_emails', True)
    monkeypatch.setattr(app_settings, 'update_aggregation_view', True)
    return app_settings


class _DummyServer:
    """Wraps the mock transport state and provides a `.log` of formatted request strings.

    Legacy aiohttp test server exposed log via `dummy_server.app['log']`. We mimic that.
    """

    def __init__(self) -> None:
        self.state = dummy_server.DummyState()
        self.log: list[str] = []
        self.server_name = 'http://dummy'
        self.app: dict[str, Any] = {'log': self.log, 'mandrill_subaccounts': self.state.mandrill_subaccounts}

    def record(self, method: str, path: str, status: int) -> None:
        self.log.append(f'{method} {path} > {status}')


@pytest.fixture
def dummy_state(_dummy_server) -> dummy_server.DummyState:
    return _dummy_server.state


@pytest.fixture(name='dummy_server')
def _dummy_server_fixture(_dummy_server):
    return _dummy_server


@pytest.fixture
def _dummy_server():
    return _DummyServer()


@pytest.fixture(autouse=True)
def _patch_http_clients(_dummy_server, monkeypatch):
    """Replace the shared httpx.Client with one routed to the dummy mock transport."""
    handler = dummy_server.make_handler(_dummy_server.state)

    def wrapped(request: httpx.Request) -> httpx.Response:
        response = handler(request)
        _dummy_server.record(request.method, request.url.path, response.status_code)
        return response

    transport = httpx.MockTransport(wrapped)
    mock_client = httpx.Client(transport=transport)
    monkeypatch.setattr(clients_module, '_default_client', mock_client)
    yield
    mock_client.close()


@pytest.fixture
def db(settings):
    """Per-test database session. Truncates tables on entry, leaves DB clean on exit."""
    with engine.begin() as conn:
        _truncate_all(conn)
    session = SessionLocal()
    yield session
    session.close()
    with engine.begin() as conn:
        _truncate_all(conn)


@pytest.fixture
def cli(settings, db):
    """Sync TestClient with `db` overriding the get_db dependency."""

    def _override():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = _override
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.pop(get_db, None)


def _stringify_json(value: Any) -> Any:
    """Mimic the legacy SyncDb behaviour: JSONB columns come back as text strings, not dicts."""
    if isinstance(value, dict):
        return json.dumps(value)
    return value


class SyncDb:
    """Lightweight test helper that mimics the old foxglove SyncDb shape using a fresh session per call."""

    def fetchval(self, sql: str, *args) -> Any:
        with SessionLocal() as s:
            row = s.execute(text(_pg_to_named(sql)), _named_args(args)).first()
            if row is None:
                return None
            return _stringify_json(row[0])

    def fetchrow(self, sql: str, *args) -> Any:
        with SessionLocal() as s:
            row = s.execute(text(_pg_to_named(sql)), _named_args(args)).mappings().first()
            if row is None:
                return None
            return {k: _stringify_json(v) for k, v in dict(row).items()}

    def fetch(self, sql: str, *args) -> list:
        with SessionLocal() as s:
            return [
                {k: _stringify_json(v) for k, v in dict(r).items()}
                for r in s.execute(text(_pg_to_named(sql)), _named_args(args)).mappings()
            ]

    def execute(self, sql: str, *args) -> int:
        with SessionLocal() as s:
            res = s.execute(text(_pg_to_named(sql)), _named_args(args))
            s.commit()
            return res.rowcount


def _pg_to_named(sql: str) -> str:
    """Translate `$1, $2 ...` placeholders to `:p1, :p2 ...` for SQLAlchemy `text()`."""
    return re.sub(r'\$(\d+)', lambda m: f':p{m.group(1)}', sql)


def _named_args(args: tuple) -> dict:
    return {f'p{i + 1}': v for i, v in enumerate(args)}


@pytest.fixture
def sync_db(db):
    return SyncDb()


@pytest.fixture
def worker():
    """Compatibility shim: Celery is eager so jobs run synchronously when enqueued.

    `test_run()` returns the number of tasks the test expected to run; we just return that count.
    Tests use it to assert work happened — since tasks are inline already, it's a no-op count.
    """

    class _EagerWorker:
        def test_run(self, max_jobs: int | None = None) -> int:
            return _TASK_COUNTER['count']

    return _EagerWorker()


@pytest.fixture
def send_email(cli, worker):
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
        assert r.status_code == status_code, r.text
        worker.test_run()
        if len(data['recipients']) != 1:
            return NotImplemented
        return re.sub(r'[^a-zA-Z0-9\-]', '', f'{data["uid"]}-{data["recipients"][0]["address"]}')

    return _send_email


@pytest.fixture
def send_sms(cli, worker):
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
        assert r.status_code == 201, r.text
        worker.test_run()
        return data['uid'] + '-447896541236'

    return _send_message


@pytest.fixture
def send_webhook(cli, worker):
    def _send_webhook(ext_id, price, **extra):
        url_args = {
            'id': ext_id,
            'reference': 'morpheus',
            'recipient': '447896541236',
            'status': 'delivered',
            'statusDatetime': '2032-06-06T12:00:00',
            'price[amount]': price,
            'test': True,
        }
        url_args.update(**extra)
        r = cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
        assert r.status_code == 200, r.text
        worker.test_run()

    return _send_webhook


@pytest.fixture
def call_send_emails(db):
    def run(**kwargs):
        base_kwargs = dict(
            uid=str(uuid.uuid4()),
            subject_template='hello',
            company_code='test',
            from_address='testing@example.com',
            method='email-mandrill',
            recipients=[],
        )
        m = EmailSendModel(**dict(base_kwargs, **kwargs))
        company = Company(code=m.company_code)
        db.add(company)
        db.commit()
        db.refresh(company)
        group = MessageGroup(
            uuid=m.uid,
            company_id=company.id,
            message_method=m.method.value,
            from_email=m.from_address.email,
            from_name=m.from_address.name,
        )
        db.add(group)
        db.commit()
        db.refresh(group)
        return group.id, company.id, m

    return run
