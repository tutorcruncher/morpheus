# Morpheus — guide for Claude Code

Morpheus is TutorCruncher's email and SMS dispatch service. One HTTP request sends a batch of
emails (Mandrill) or SMSs (MessageBird). Morpheus renders each message, sends it from a Celery
worker, records delivery events from provider webhooks, tracks link clicks and serves a searchable
message history. TutorCruncher (TC2) is the main API consumer.

**This repository is public.** Everything committed or posted to GitHub — code, tests, fixtures,
commit messages, issues, PR descriptions, screenshots — is world-readable. Never include customer
or agency names, production company codes, real names, emails or phone numbers, internal
hostnames, or secrets. Use placeholders such as `<company_code>`, `foobar@example.org` and
`billing-test`. Machine-specific details (local database names, passwords, ports) belong in `.env`
or `.claude/`, never in this file.

## Stack

- FastAPI, mostly sync endpoints, served by uvicorn (`Procfile` `web`).
- SQLModel / SQLAlchemy 2 over Postgres via psycopg2. No migrations framework — see
  [Database and schema](#database-and-schema).
- Celery worker and beat over Redis (`app/core/celery.py`, entry point `app/worker.py`).
- httpx for Mandrill and MessageBird (`app/ext/clients.py`).
- Sentry (`app/sentry/setup.py`) and Logfire (`app/core/logging.py`), each off unless its env var
  is set.
- Python 3.12, dependencies managed with `uv`.

The `foxglove` run instructions in `README.md` predate the move to FastAPI + Celery; the `Makefile`
and `Procfile` are current.

## Commands

```bash
make install    # uv sync
make format     # ruff format + ruff check --fix on app/ and tests/
make lint       # ruff check, ruff format --check, ty check app — CI runs this
make test       # pytest tests/ --cov=app
make dev        # uvicorn app.main:app --reload
make worker     # celery worker
make beat       # celery beat
make reset-db   # DROPS and recreates the local `morpheus` database, then runs create_db_and_tables
```

Run only the test files you change: `uv run pytest tests/test_sms.py`. Tests need a local Postgres
and Redis. pytest-env (`[tool.pytest.ini_options]` in `pyproject.toml`) defaults `DATABASE_URL` to
`postgresql://postgres@localhost:5432/morpheus_test` and `REDIS_URL` to `redis://localhost:6379/4`;
export either variable to override it (for example when your Postgres user needs a password). The
test session creates the schema itself; every test flushes that Redis database, and every test that
uses the `db` fixture truncates all tables.

Settings come from environment variables or `.env` (`app/core/config.py`, pydantic-settings).
`.env` is gitignored.

## Layout

| Path | Contents |
|------|----------|
| `app/main.py` | App assembly, routers, CORS, static files, lifespan (thread-pool cap, no DDL) |
| `app/core/config.py` | `Settings` |
| `app/core/database.py` | Engine, `DBSession` (`get_or_create`), `get_db`, `create_db_and_tables`, `configure_worker_engine` |
| `app/core/bootstrap.sql`, `post_bootstrap.sql` | Enums and functions before `create_all`; triggers and the `message_aggregation` view after it |
| `app/core/celery.py` | Celery app and beat schedule |
| `app/worker.py` | Worker entry point: Sentry before fork; Logfire and the small DB pool after fork |
| `app/common/auth.py` | `AdminAuth`, `UserSession` |
| `app/common/api/errors.py` | `HTTP400`/`403`/`404`/`409`/`422` and the `{"message": ...}` error handler |
| `app/ext/clients.py` | `Mandrill`, `MessageBird`, shared `_default_client` |
| `app/messages/models.py` | `Company`, `MessageGroup`, `Message`, `Event`, `Link`, send-method and status enums |
| `app/messages/schemas.py` | Request and webhook models (`EmailSendModel`, `SmsSendModel`, `MandrillSingleWebhook`, `MessageBirdWebHook`, …) |
| `app/messages/api/email.py` | `POST /send/email/` |
| `app/messages/api/sms.py` | `POST /send/sms/`, `GET /validate/sms/`, `GET /billing/{method}/{company_code}/`, `_get_or_create_company` |
| `app/messages/api/subaccounts.py` | Mandrill subaccount create and delete |
| `app/messages/api/messages.py` | `/messages/{method}/` list, aggregation and detail views |
| `app/messages/api/webhooks.py` | `/webhook/mandrill/`, `/webhook/messagebird/`, `/webhook/test/` |
| `app/messages/api/common.py` | Index page and the `/l{token}` click redirect |
| `app/messages/tasks.py` | Celery tasks: send, webhook status updates, click storage, aggregation refresh, deletes |
| `app/render/` | Message rendering (chevron, mistune, libsass); also published as `morpheus-mail` |
| `app/templates/`, `app/static/` | Jinja2 pages and static files |
| `tests/conftest.py`, `tests/dummy_server.py` | Test fixtures and the mocked Mandrill / MessageBird routes |
| `packaging/morpheus-mail/` | PyPI packaging for `app/render/` |

## Rules

### API

- Endpoints are mostly sync `def` and run in Starlette's thread pool. The lifespan in `app/main.py`
  caps that pool at `db_pool_size + db_max_overflow`, so anything that makes a request hold its DB
  connection longer reduces concurrency for the whole process.
- Get a session with `db: DBSession = Depends(get_db)`. On high-volume endpoints, call `db.close()`
  before slow work such as a Celery enqueue loop or template rendering (see `email.py`, `sms.py`,
  `common.py`; MORPHEUS-3DNG).
- Raise the classes in `app/common/api/errors.py`, not bare `HTTPException`; they produce the
  `{"message": ...}` body clients expect.
- Protect TC2-facing endpoints with `AdminAuth` (the `Authorization` header must equal `AUTH_KEY`),
  attached to the router: `APIRouter(dependencies=[Depends(AdminAuth)])`. `/messages/*` uses `UserSession`
  (HMAC-signed `company`, `expires` and `signature` query parameters); the company always comes from
  the signed session, never from other request input.
- Company codes are either a bare code or `<code>:<branch_id>`. Company rows are created lazily by
  `_get_or_create_company`.
- A change to the response shape or status codes of an endpoint TC2 calls is a cross-service
  contract change. State the impact in the PR.

### Database and schema

- No DDL at web boot (issue #511). `create_db_and_tables()` runs out-of-band: `make reset-db`
  locally, a deliberate one-off in production. `tests/test_boot_bootstrap.py` guards this.
- `create_db_and_tables()` runs `bootstrap.sql`, then `SQLModel.metadata.create_all`, then
  `post_bootstrap.sql`. `create_all` only creates missing tables; it never alters an existing one.
  A new column, a new index on an existing table, or a new enum value in production needs
  hand-written SQL run as a one-off. Put that SQL in the PR description.
- The Postgres enums `send_methods` and `message_statuses` are created by `bootstrap.sql` only when
  missing, and the models declare them with `create_type=False`. Adding a value means editing
  `bootstrap.sql` and the Python enum in `app/messages/models.py`, and running
  `ALTER TYPE ... ADD VALUE` once in production.
- `message_aggregation` is created with `CREATE MATERIALIZED VIEW IF NOT EXISTS`, so changing its
  definition needs a manual drop during deploy. The `update_aggregation_view` beat task refreshes it
  hourly while `UPDATE_AGGREGATION_VIEW` is on (the default).
- `Message.vector` (tsvector) is filled by the `set_message_vector` trigger, not by Python.

### Celery

- Tasks live in `app/messages/tasks.py` with an explicit `name='app.messages.tasks.<name>'`.
  Arguments are JSON-serialised.
- Keep task names and signatures backward compatible: tasks queued by the previous release can be
  run by the new worker during a deploy.
- Each prefork child rebuilds the DB engine with `db_worker_pool_size` / `db_worker_max_overflow`
  (`configure_worker_engine`) and runs one task at a time (`worker_prefetch_multiplier=1`). Keep
  that pool small; the comment in `app/core/config.py` explains why.
- The beat schedule is in `app/core/celery.py`.

### `app/render/`

`app/render/` is also published to PyPI as `morpheus-mail` on every deploy tag, so changes there
affect external users. Bump `VERSION` in `packaging/morpheus-mail/setup.py` when the render module
changes; an unchanged version skips the upload.

### Tests

- Fixtures in `tests/conftest.py`: `cli` (`TestClient` with `get_db` overridden to `db`), `db`
  (session; tables truncated before and after), `settings` (patches keys and provider URLs;
  `auth_key` is `testing-key`), `dummy_server` (records calls to the mocked providers), and the
  `send_email` / `send_sms` / `send_webhook` helpers.
- Autouse fixtures run Celery tasks eagerly and route the shared httpx client to the
  `httpx.MockTransport` handlers in `tests/dummy_server.py`. Tests never make real external HTTP
  calls; add any new provider route to `tests/dummy_server.py`.
- Warnings are errors (`filterwarnings = error`) and each test times out after 30 seconds.
- Assert the status code and the response body.
- Test data uses placeholders only (public repository).

### Style

- ruff with line length 120, single quotes, and isort with combined `as` imports. `ty` type-checks
  `app/`; use a targeted `# ty:ignore[<rule>]` only where the existing code would.
- Match the surrounding code. Comments explain why, citing the issue number or Sentry id where one
  exists.

## CI and deploy

- `.github/workflows/main.yml` runs on every PR and every push to `master`: `make lint`, then pytest
  with coverage against Postgres 15 and Redis, with coverage uploaded to Codecov.
- Pushing a tag runs the test job, then deploys `master` to Heroku and publishes `morpheus-mail`.
- The base branch is `master`. Open PRs against `tutorcruncher/morpheus`.

## Local Claude tooling (`.claude/`)

`.claude/` is gitignored, so its contents exist only in clones where someone has added them. Where
present:

- **`project-context` skill** (`.claude/skills/project-context/SKILL.md`): stack, layout, commands,
  local databases, test infrastructure, the TC2 contract and the operational rules above. The
  review agents in `.claude/agents/` load it through `skills: project-context`. Keep it consistent
  with this file.
- **`terminal-record` skill** (`.claude/skills/terminal-record/`): captures real terminal
  screenshots (a PTY rendered with xterm.js and screenshotted through the Playwright MCP) and posts
  them under `### Testing` in a PR. Screenshots upload to the `pr-screenshots` branch through the
  GitHub REST API, never a git push. They are public, so they show synthetic data only.
- Commands `/full-review`, `/fix-issue`, `/pr-uat` and `/pr-record` live in `.claude/commands/`;
  `/fix-issue` creates worktrees under `.claude/worktrees/`.
