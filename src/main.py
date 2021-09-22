import arq
import asyncio
import logging
import uvicorn as uvicorn
from fastapi import FastAPI, Request
from foxglove import exceptions, glove
from foxglove.middleware import ErrorMiddleware
from foxglove.route_class import KeepBodyAPIRoute
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from src.ext import Mandrill
from src.settings import Settings
from src.views import common, email, messages, sms, subaccounts, webhooks

logger = logging.getLogger('main')
settings = Settings()

glove._settings = Settings()


async def startup():
    from foxglove.logs import setup_sentry

    setup_sentry()
    if not hasattr(glove, 'redis') and glove.settings.redis_settings:
        glove.redis = await arq.create_pool(glove.settings.redis_settings)
    if not hasattr(glove, 'mandrill'):
        glove.mandrill = Mandrill(glove.settings)


async def shutdown():
    coros = []
    if http := getattr(glove, 'http', None):
        coros.append(http.aclose())
    if redis := getattr(glove, 'redis', None):
        redis.close()
        coros.append(redis.wait_closed())
    await asyncio.gather(*coros)
    for prop in '_http', 'redis', 'mandrill':
        if hasattr(glove, prop):
            delattr(glove, prop)


app = FastAPI(
    title='Morpheus',
    on_startup=[startup],
    on_shutdown=[shutdown],
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(ErrorMiddleware)
app.add_middleware(CORSMiddleware, allow_origins=['*'])
app.router.route_class = KeepBodyAPIRoute


@app.exception_handler(exceptions.HttpMessageError)
async def foxglove_exception_handler(request: Request, exc: exceptions.HttpMessageError):
    return exceptions.HttpMessageError.handle(exc)


app.include_router(common.app, tags=['common'])
app.include_router(email.app, tags=['email'])
app.include_router(sms.app, tags=['sms'])
app.include_router(subaccounts.app, tags=['subaccounts'])
app.include_router(messages.app, prefix='/messages', tags=['messages'])
app.include_router(webhooks.app, prefix='/webhook', tags=['webhooks'])
# This has to come last
app.mount('/', StaticFiles(directory='src/static'), name='static')
app.state.server_up_wait = 5


if __name__ == '__main__':
    uvicorn.run(app, host='localhost', port=8000, log_level='debug')
