import base64

from aiohttp.web import Application
from cryptography.fernet import Fernet

from .es import ElasticSearch
from .logs import setup_logging
from .models import SendMethod
from .settings import Settings
from .views import SendView, TestWebhookView, UserAggregationView, UserMessageView


async def app_startup(app):
    settings = app['settings']
    app.update(
        sender=settings.sender_cls(settings=settings, loop=app.loop),
        es=ElasticSearch(settings=settings, loop=app.loop),
        fernet=Fernet(base64.urlsafe_b64encode(settings.user_fernet_key)),
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
    methods = '|'.join(m.value for m in SendMethod)
    app.router.add_get('/user/messages/{method:%s}/' % methods, UserMessageView.view(), name='user-messages')
    app.router.add_get('/user/aggregation/{method:%s}/' % methods, UserAggregationView.view(), name='user-aggregation')
    return app
