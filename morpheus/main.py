import base64
import re
from html import escape

from aiohttp.web import Application
from cryptography.fernet import Fernet

from .es import ElasticSearch
from .logs import setup_logging
from .models import SendMethod
from .settings import Settings
from .views import (THIS_DIR, SendView, TestWebhookView, UserAggregationView, UserMessageView, UserTaggedMessageView,
                    favicon, index, robots_txt)


async def app_cleanup(app):
    await app['sender'].close()
    app['es'].close()


def create_app(loop, settings: Settings=None):
    settings = settings or Settings()
    setup_logging(settings)
    app = Application(client_max_size=1024**2*100)

    index_html = (THIS_DIR / 'extra/index.html').read_text()
    for key in ('commit', 'release_date'):
        index_html = re.sub(r'\{\{ ?%s ?\}\}' % key, escape(settings.values[key] or ''), index_html)

    app.update(
        index_html=index_html,
        settings=settings,
        sender=settings.sender_cls(settings=settings, loop=loop),
        es=ElasticSearch(settings=settings, loop=loop),
        fernet=Fernet(base64.urlsafe_b64encode(settings.user_fernet_key)),
    )

    app.on_cleanup.append(app_cleanup)

    app.router.add_get('/', index, name='index')
    app.router.add_get('/robots.txt', robots_txt, name='robots-txt')
    app.router.add_get('/favicon.ico', favicon, name='favicon')

    app.router.add_post('/send/', SendView.view(), name='send')
    app.router.add_post('/webhook/test/', TestWebhookView.view(), name='webhook-test')

    user_prefix = '/user/{method:%s}/' % '|'.join(m.value for m in SendMethod)
    app.router.add_get(user_prefix, UserMessageView.view(), name='user-messages')
    app.router.add_get(user_prefix + '/tag/', UserTaggedMessageView.view(), name='user-tagged-messages')
    app.router.add_get(user_prefix + '/aggregation/', UserAggregationView.view(), name='user-aggregation')
    return app
