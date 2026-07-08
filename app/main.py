import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.common.api.errors import HttpMessageError, http_message_error_handler
from app.core.config import settings
from app.core.database import create_db_and_tables, engine
from app.core.logging import configure_logfire, configure_logging
from app.messages.api import (
    common as common_api,
    email as email_api,
    messages as messages_api,
    sms as sms_api,
    subaccounts as subaccounts_api,
    webhooks as webhooks_api,
)
from app.sentry.setup import init_sentry

configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    init_sentry()
    # Sync endpoints run in Starlette's thread pool (default 40 threads). Each holds one DB
    # connection for its duration, so with more threads than connections a burst leaves surplus
    # threads blocked on checkout for the full pool_timeout, which under the comms retry storm
    # collapsed the service (MORPHEUS-3DNG). Bound the thread pool to the pool capacity so excess
    # requests queue cheaply for a thread instead of holding nothing while stuck on a 30s checkout.
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = settings.db_pool_size + settings.db_max_overflow
    yield


app = FastAPI(
    title='Morpheus',
    lifespan=lifespan,
    docs_url='/docs' if (settings.dev_mode or settings.testing) else None,
    redoc_url='/redoc' if (settings.dev_mode or settings.testing) else None,
    openapi_url='/openapi.json' if (settings.dev_mode or settings.testing) else None,
)
app.add_middleware(CORSMiddleware, allow_origins=['*'])
app.add_exception_handler(HttpMessageError, http_message_error_handler)  # ty:ignore[invalid-argument-type]

app.include_router(common_api.router, tags=['common'])
app.include_router(email_api.router, tags=['email'])
app.include_router(sms_api.router, tags=['sms'])
app.include_router(subaccounts_api.router, tags=['subaccounts'])
app.include_router(messages_api.router, prefix='/messages', tags=['messages'])
app.include_router(webhooks_api.router, prefix='/webhook', tags=['webhooks'])

app.mount('/', StaticFiles(directory='app/static'), name='static')

# Observability must be configured BEFORE the app starts serving. Instrumenting inside
# the lifespan runs after Starlette has built the middleware stack, which stops the OTel
# request-span middleware from wrapping requests and orphans every downstream DB/httpx
# span into its own trace. See tests/test_parity.py::test_logfire_sql_spans_nest_under_request_span.
configure_logfire()
if settings.logfire_token:  # pragma: no cover  -- prod-only instrumentation
    import logfire

    logfire.instrument_fastapi(app)
    logfire.instrument_sqlalchemy(engine=engine)
