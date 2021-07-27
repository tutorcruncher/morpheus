import asyncio

from morpheus.app.main import logger


async def get_mandrill_webhook_key(app):
    try:
        settings, mandrill_webhook_url = app.state.settings, app.state.mandrill_webhook_url
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
            await asyncio.sleep(app.state.server_up_wait)
            r = await mandrill.post('webhooks/add.json', **data)
            data = await r.json()
            webhook_auth_key = data['auth_key']
            logger.info('created new mandrill webhook "%s", key %s', data['description'], webhook_auth_key)
        app.state.webhook_auth_key = webhook_auth_key.encode()
    except Exception as e:
        logger.exception('error in get_mandrill_webhook_key, %s: %s', e.__class__.__name__, e)
        raise e
