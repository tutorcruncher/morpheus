from fastapi import FastAPI, Request
import logging

from foxglove import glove, exceptions
from foxglove.middleware import ErrorMiddleware
from foxglove.route_class import KeepBodyAPIRoute
from starlette.staticfiles import StaticFiles

from .management import SessionLocal
from .settings import Settings
from .views import common, email, sms, subaccounts, messages, webhooks

logger = logging.getLogger('morpheus.main')
settings = Settings()

app = FastAPI(
    title='Morpheus',
    middleware=[ErrorMiddleware],
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
app.include_router(messages.app, prefix='user/')
app.include_router(webhooks.app, prefix='webhook/')
app.state.update(server_up_wait=5)

glove.pg = SessionLocal()
glove._settings = Settings()
