import asyncio
import logging

import aiohttp_jinja2
import jinja2
from aiohttp.web import Application
from atoolbox.create_app import cleanup, startup
from atoolbox.middleware import error_middleware

from .ext import Mandrill, MorpheusUserApi
from .models import SendMethod, SmsSendMethod
from .settings import Settings
from .utils import THIS_DIR
from .views import (
    AdminAggregatedView,
    AdminGetView,
    AdminListView,
    ClickRedirectView,
    CreateSubaccountView,
    DeleteSubaccountView,
    EmailSendView,
    MandrillWebhookView,
    MessageBirdWebhookView,
    MessageStatsView,
    SmsBillingView,
    SmsSendView,
    SmsValidateView,
    TestWebhookView,
    UserAggregationView,
    UserMessageDetailView,
    UserMessageListView,
    UserMessagePreviewView,
    UserMessagesJsonView,
    index,
)

logger = logging.getLogger('morpheus.main')


async def get_mandrill_webhook_key(app):
    try:
        settings, mandrill_webhook_url = app['settings'], app['mandrill_webhook_url']
        if not settings.mandrill_key or settings.host_name in {None, 'localhost'}:
            return

        mandrill = app['mandrill']
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
                # blacklist and whitelist are skipped since they are "sync events" not "message events"
                'events': (
                    'send',
                    'hard_bounce',
                    'soft_bounce',
                    'open',
                    'click',
                    'spam',
                    'unsub',
                    'reject'
                    # 'deferral' can't request this.
                ),
            }
            logger.info('about to create webhook entry via API, wait for morpheus API to be up...')
            await asyncio.sleep(app['server_up_wait'])
            r = await mandrill.post('webhooks/add.json', **data)
            data = await r.json()
            webhook_auth_key = data['auth_key']
            logger.info('created new mandrill webhook "%s", key %s', data['description'], webhook_auth_key)
        app['webhook_auth_key'] = webhook_auth_key.encode()
    except Exception as e:
        logger.exception('error in get_mandrill_webhook_key, %s: %s', e.__class__.__name__, e)
        raise e


async def extra_startup(app):
    asyncio.get_event_loop().create_task(get_mandrill_webhook_key(app))


async def extra_cleanup(app):
    await asyncio.gather(app['morpheus_api'].close(), app['mandrill'].close())


def create_app(settings: Settings = None):
    settings = settings or Settings()
    app = Application(client_max_size=settings.max_request_size, middlewares=(error_middleware,))
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(THIS_DIR / 'templates')),
        autoescape=jinja2.select_autoescape(['html', 'xml', 'jinja']),
    )

    app.update(
        settings=settings,
        mandrill_webhook_url=f'https://{settings.host_name}/webhook/mandrill/',
        mandrill=Mandrill(settings=settings),
        webhook_auth_key=None,
        morpheus_api=MorpheusUserApi(settings=settings),
        stats_request_count='request-stats-count',
        stats_request_list='request-stats-list',
        server_up_wait=5,
    )

    app.on_startup.append(startup)
    app.on_startup.append(extra_startup)

    app.on_cleanup.append(cleanup)
    app.on_cleanup.append(extra_cleanup)

    app.router.add_get('/', index, name='index')
    app.router.add_get(r'/l{token}{_:/?}', ClickRedirectView.view(), name='click-redirect')

    app.router.add_post('/send/email/', EmailSendView.view(), name='send-emails')
    app.router.add_post('/send/sms/', SmsSendView.view(), name='send-smss')

    app.router.add_get('/validate/sms/', SmsValidateView.view(), name='validate-smss')

    sms_methods = r'{method:%s}' % '|'.join(m.value for m in SmsSendMethod)
    app.router.add_get('/billing/' + sms_methods + '/{company_code}/', SmsBillingView.view(), name='billing-sms')

    methods = r'/{method:%s}/' % '|'.join(m.value for m in SendMethod)
    app.router.add_post('/create-subaccount' + methods, CreateSubaccountView.view(), name='create-subaccount')
    app.router.add_post('/delete-subaccount' + methods, DeleteSubaccountView.view(), name='delete-subaccount')

    app.router.add_post('/webhook/test/', TestWebhookView.view(), name='webhook-test')
    app.router.add_head('/webhook/mandrill/', index, name='webhook-mandrill-head')
    app.router.add_post('/webhook/mandrill/', MandrillWebhookView.view(), name='webhook-mandrill')
    app.router.add_get('/webhook/messagebird/', MessageBirdWebhookView.view(), name='webhook-messagebird')

    app.router.add_get('/user' + methods + 'messages.json', UserMessagesJsonView.view(), name='user-messages')
    app.router.add_get(
        '/user' + methods + r'message/{id:\d+}.html', UserMessageDetailView.view(), name='user-message-get'
    )
    app.router.add_get('/user' + methods + 'messages.html', UserMessageListView.view(), name='user-message-list')
    app.router.add_get('/user' + methods + 'aggregation.json', UserAggregationView.view(), name='user-aggregation')
    app.router.add_get('/user' + methods + r'{id:\d+}/preview/', UserMessagePreviewView.view(), name='user-preview')

    app.router.add_get('/admin/', AdminAggregatedView.view(), name='admin')
    app.router.add_get('/admin/list/', AdminListView.view(), name='admin-list')
    app.router.add_get(r'/admin/get/{method}/{id:\d+}/', AdminGetView.view(), name='admin-get')

    app.router.add_get('/stats/messages/', MessageStatsView.view(), name='message-stats')
    app.router.add_static('/', str(THIS_DIR / 'static'))
    return app
