import asyncio
import logging.config
import os
import random
import re

from aiohttp import web
from aiohttp.web import Response, json_response

nginx_500 = """\
<html>
<head><title>500 Internal Server Error</title></head>
<body bgcolor="white">
<center><h1>500 Internal Server Error</h1></center>
<hr><center>nginx/1.12.2</center>
</body>
</html>\
"""


async def mandrill_send_view(request):
    data = await request.json()
    choice = random.choices(['ok', '502', '500'], weights=[5, 1, 1])[0]

    if choice == '502':
        return Response(status=502)
    elif choice == '500':
        return Response(text=nginx_500, status=500)

    to_email = data['message']['to'][0]['email']
    return json_response(
        [{'email': to_email, '_id': re.sub(r'[^a-zA-Z0-9\-]', '', f'mandrill-{to_email}'), 'status': 'queued'}]
    )


async def mandrill_sub_account_add(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    sa_id = data['id']
    if sa_id == 'broken':
        return json_response({'error': 'snap something unknown went wrong'}, status=500)
    elif sa_id in request.app['mandrill_subaccounts']:
        return json_response({'message': f'A subaccount with id {sa_id} already exists'}, status=500)
    else:
        request.app['mandrill_subaccounts'][sa_id] = data
        return json_response({'message': "subaccount created (this isn't the same response as mandrill)"})


async def mandrill_sub_account_info(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    sa_id = data['id']
    sa_info = request.app['mandrill_subaccounts'].get(sa_id)
    if sa_info:
        return json_response({'subaccount_info': sa_info, 'sent_total': 200 if sa_id == 'lots-sent' else 42})


async def mandrill_webhook_list(request):
    return json_response(
        [
            {
                'url': 'https://example.com/webhook/mandrill/',
                'auth_key': 'existing-auth-key',
                'description': 'testing existing key',
            }
        ]
    )


async def mandrill_webhook_add(request):
    data = await request.json()
    if 'fail' in data['url']:
        return Response(status=400)
    return json_response({'auth_key': 'new-auth-key', 'description': 'testing new key'})


def create_app():
    app = web.Application()
    app.add_routes(
        [
            web.post('/messages/send.json', mandrill_send_view),
            web.post('/subaccounts/add.json', mandrill_sub_account_add),
            web.get('/subaccounts/info.json', mandrill_sub_account_info),
            web.get('/webhooks/list.json', mandrill_webhook_list),
            web.post('/webhooks/add.json', mandrill_webhook_add),
        ]
    )
    return app


logging_config = {
    'version': 1,
    'handlers': {'main': {'level': 'INFO', 'class': 'logging.StreamHandler'}},
    'loggers': {'aiohttp.access': {'handlers': ['main'], 'level': 'INFO'}},
}


if __name__ == '__main__':
    logging.config.dictConfig(logging_config)
    app = create_app()
    port = int(os.getenv('PORT') or 8002)
    web.run_app(app, port=port)
