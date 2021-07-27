from fastapi import FastAPI, Request
import logging

from foxglove import glove, exceptions
from foxglove.db import PgMiddleware
from foxglove.middleware import ErrorMiddleware
from foxglove.route_class import KeepBodyAPIRoute
from starlette.staticfiles import StaticFiles

from .settings import Settings
from .views import common, email, sms, subaccounts, messages, webhooks

logger = logging.getLogger('morpheus.main')
settings = Settings()

app = FastAPI(
    title='Morpheus',
    middleware=[ErrorMiddleware, PgMiddleware],
    on_startup=[glove.startup],
    on_shutdown=[glove.shutdown],
)
app.mount('/', StaticFiles(directory='static'), name='static')
app.router.route_class = KeepBodyAPIRoute


@app.exception_handler(exceptions.HttpMessageError)
async def foxglove_exception_handler(request: Request, exc: exceptions.HttpMessageError):
    return exceptions.HttpMessageError.handle(exc)


app.include_router(common.app)
app.include_router(email.app)
app.include_router(sms.app)
app.include_router(subaccounts.app)
app.include_router(messages.app)
app.include_router(webhooks.app)
app.state.update(server_up_wait=5)
