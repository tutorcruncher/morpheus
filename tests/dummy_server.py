import asyncio
import re
from aiohttp import web
from aiohttp.web import Response, json_response


async def mandrill_send_view(request):
    data = await request.json()

    message = data.get('message') or {}
    if message.get('subject') == '__slow__':
        await asyncio.sleep(30)
    elif message.get('subject') == '__502__':
        return Response(status=502)
    elif message.get('subject') == '__500_nginx__':
        return Response(text='<hr><center>nginx/1.12.2</center>', status=500)
    elif message.get('subject') == '__500__':
        return Response(text='foobar', status=500)

    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    to_email = message['to'][0]['email']
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


async def mandrill_sub_account_delete(request):
    data = await request.json()
    if data['key'] != 'good-mandrill-testing-key':
        return json_response({'auth': 'failed'}, status=403)
    sa_id = data['id']
    if sa_id == 'broken1' or sa_id not in request.app['mandrill_subaccounts']:
        return json_response({'error': 'snap something unknown went wrong'}, status=500)
    elif 'name' not in request.app['mandrill_subaccounts'][sa_id]:
        return json_response(
            {'message': f"No subaccount exists with the id '{sa_id}'", 'name': 'Unknown_Subaccount'}, status=500
        )
    else:
        request.app['mandrill_subaccounts'][sa_id] = data
        return json_response({'message': "subaccount deleted (this isn't the same response as mandrill)"})


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


async def messagebird_hlr_post(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    data = await request.json()
    return json_response(
        status=201,
        data={
            'id': data['msisdn'],
            'href': 'https://example.com/messagebird/hlr/testing1234',
            'msisdn': data['msisdn'],
        },
    )


async def messagebird_lookup(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    if '447888888888' in request.path:
        return json_response({})
    elif '447777777777' in request.path:
        request_number = len(request.app['log'])
        if request_number == 2:
            return json_response({'status': 'active', 'network': 'o2'})
        return json_response({})
    return json_response({'status': 'active', 'network': 23430})


async def messagebird_send(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    data = await request.json()
    return json_response(
        {'id': '6a23b2037595620ca8459a3b00026003', 'recipients': {'totalCount': len(data['recipients'])}}, status=201
    )


async def messagebird_pricing(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    return json_response(
        {
            'prices': [
                {'mcc': '0', 'countryName': 'Default rate', 'price': '0.0400'},
                {'mcc': '0', 'countryName': 'United Kingdom', 'price': '0.0200'},
            ]
        }
    )


routes = [
    web.post('/mandrill/messages/send.json', mandrill_send_view),
    web.post('/mandrill/subaccounts/add.json', mandrill_sub_account_add),
    web.post('/mandrill/subaccounts/delete.json', mandrill_sub_account_delete),
    web.get('/mandrill/subaccounts/info.json', mandrill_sub_account_info),
    web.get('/mandrill/webhooks/list.json', mandrill_webhook_list),
    web.post('/mandrill/webhooks/add.json', mandrill_webhook_add),
    web.post('/messagebird/hlr', messagebird_hlr_post),
    web.get('/messagebird/hlr/{id}', messagebird_lookup),
    web.post('/messagebird/messages', messagebird_send),
    web.get('/messagebird/pricing/sms/outbound', messagebird_pricing),
]
