import asyncio
import logging

import async_timeout
from aiohttp.web import Application

from .es import ElasticSearch
from .logs import setup_logging
from .middleware import ErrorLoggingMiddleware
from .models import SendMethod
from .settings import Settings
from .utils import Mandrill, MorpheusUserApi
from .views import (THIS_DIR, AdminAggregatedView, AdminGetView, AdminListView, EmailSendView, MandrillWebhookView,
                    SmsSendView, SmsValidateView, TestWebhookView, UserAggregationView, UserMessagePreviewView,
                    UserMessageView, index)

logger = logging.getLogger('morpheus.main')


async def get_mandrill_webhook_key(app):
    try:
        settings, mandrill_webhook_url = app['settings'], app['mandrill_webhook_url']
        if not (settings.mandrill_key and settings.host_name):
            return

        mandrill = Mandrill(settings=settings, loop=app.loop)
        webhook_auth_key = None

        r = await mandrill.get('webhooks/list.json')
        for hook in await r.json():
            if hook['url'] == mandrill_webhook_url:
                webhook_auth_key = hook['auth_key']
                logger.info('using existing mandrill webhook "%s", key %s', hook['description'], webhook_auth_key)
                break

        if not webhook_auth_key:
            logger.info('creating mandrill webhook entry via API')
            data = {
                'url': mandrill_webhook_url,
                'description': 'morpheus (auto created)',
                # infuriatingly this list appears to differ from those the api returns or actually submits in hooks
                'events': (
                   'send', 'hard_bounce', 'soft_bounce', 'open', 'click', 'spam', 'unsub', 'reject',
                   'blacklist', 'whitelist',
                   # 'deferral' can't request this.
                ),
            }
            logger.info('about to create webhook entry via API, wait for morpheus API to be up...')
            await asyncio.sleep(5)
            r = await mandrill.post('webhooks/add.json', **data)
            if r.status != 200:
                raise RuntimeError('invalid mandrill webhook list response {}:\n{}'.format(r.status, await r.text()))
            data = await r.json()
            webhook_auth_key = data['auth_key']
            logger.info('created new mandrill webhook "%s", key %s', data['description'], webhook_auth_key)
        mandrill.close()
        app['webhook_auth_key'] = webhook_auth_key.encode()
    except Exception as e:
        logger.exception('error in get_mandrill_webhook_key, %s: %s', e.__class__.__name__, e)
        raise e


async def app_startup(app):
    # weird effect with aiohttp-devtools
    loop = app.loop or asyncio.get_event_loop()
    with async_timeout.timeout(5, loop=loop):
        redis_pool = await app['sender'].get_redis_pool()
        async with redis_pool.get() as redis:
            info = await redis.info()
            logger.info('redis version: %s', info['server']['redis_version'])
    loop.create_task(get_mandrill_webhook_key(app))


async def app_cleanup(app):
    await app['sender'].close()
    app['es'].close()
    app['morpheus_api'].close()


def create_app(loop, settings: Settings=None):
    settings = settings or Settings()
    setup_logging(settings)
    app = Application(client_max_size=1024**2*100, middlewares=(ErrorLoggingMiddleware(),))

    app.update(
        settings=settings,
        sender=settings.sender_cls(settings=settings, loop=loop),
        es=ElasticSearch(settings=settings, loop=loop),
        mandrill_webhook_url=f'https://{settings.host_name}/webhook/mandrill/',
        webhook_auth_key=None,
        morpheus_api=MorpheusUserApi(settings=settings, loop=loop),
    )

    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)

    app.router.add_get('/', index, name='index')

    app.router.add_post('/send/email/', EmailSendView.view(), name='send-emails')
    app.router.add_post('/send/sms/', SmsSendView.view(), name='send-smss')
    app.router.add_get('/validate/sms/', SmsValidateView.view(), name='validate-smss')

    app.router.add_post('/webhook/test/', TestWebhookView.view(), name='webhook-test')
    app.router.add_head('/webhook/mandrill/', index, name='webhook-mandrill-head')
    app.router.add_post('/webhook/mandrill/', MandrillWebhookView.view(), name='webhook-mandrill')

    user_prefix = '/user/{method:%s}/' % '|'.join(m.value for m in SendMethod)
    app.router.add_get(user_prefix, UserMessageView.view(), name='user-messages')
    app.router.add_get(user_prefix + 'aggregation/', UserAggregationView.view(), name='user-aggregation')
    app.router.add_get(user_prefix + '{id}/preview/', UserMessagePreviewView.view(), name='user-preview')
    app.router.add_get('/admin/', AdminAggregatedView.view(), name='admin')
    app.router.add_get('/admin/list/', AdminListView.view(), name='admin-list')
    app.router.add_get('/admin/get/{method}/{id}/', AdminGetView.view(), name='admin-get')
    app.router.add_static('/', str(THIS_DIR / 'static'))
    return app
