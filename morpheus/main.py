from aiohttp.web import Application

from .es import ElasticSearch
from .logs import setup_logging
from .settings import Settings
from .views import SendView, TestWebhookView


async def app_startup(app):
    settings = app['settings']
    app.update(
        sender=settings.sender_cls(settings=settings, loop=app.loop),
        es=ElasticSearch(settings=settings, loop=app.loop)
    )


async def app_cleanup(app):
    await app['sender'].close()
    app['es'].close()


def create_app(loop, settings: Settings=None):
    settings = settings or Settings()
    setup_logging(settings)
    app = Application(client_max_size=1024**2*100)  # TODO middleware
    app['settings'] = settings

    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)

    app.router.add_post('/send/', SendView.view(), name='send')
    app.router.add_post('/webhook/test/', TestWebhookView.view(), name='webhook-test')
    return app
