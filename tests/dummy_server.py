import asyncio
import re

from aiohttp import web
from aiohttp.web import HTTPForbidden, Response, json_response


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
        return json_response({'message': f'A subaccount with id {sa_id} does not exist'}, status=500)
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
    return Response(status=201)


async def messagebird_lookup(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    if '447888888888' in request.path:
        return json_response({})
    elif '447777777777' in request.path:
        return json_response({'hlr': {'status': 'active'}})
    return json_response({'hlr': {'status': 'active', 'network': 23430}})


async def messagebird_send(request):
    assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
    data = await request.json()
    return json_response(
        {'id': '6a23b2037595620ca8459a3b00026003', 'recipients': {'totalCount': len(data['recipients'])}}, status=201
    )


async def messagebird_pricing(request):
    if not request.query.get('username') == 'mb-username':
        raise HTTPForbidden(text='bad username')
    if not request.query.get('password') == 'mb-password':
        raise HTTPForbidden(text='bad password')
    return json_response(
        [
            {'mcc': '0', 'country_name': 'Default rate', 'rate': '0.0400'},
            {'mcc': '234', 'country_name': 'United Kingdom', 'rate': '0.0200'},
        ]
    )


async def generate_pdf(request):
    assert request.headers['pdf_zoom'] == '1.25'
    data = await request.read()
    if not data:
        return Response(text='request was empty', status=400)
    elif b'binary' in data:
        return Response(body=b'binary-\xfe', content_type='application/pdf')
    else:
        return Response(body=data, content_type='application/pdf')


routes = [
    web.post('/mandrill/messages/send.json', mandrill_send_view),
    web.post('/mandrill/subaccounts/add.json', mandrill_sub_account_add),
    web.post('/mandrill/subaccounts/delete.json', mandrill_sub_account_delete),
    web.get('/mandrill/subaccounts/info.json', mandrill_sub_account_info),
    web.get('/mandrill/webhooks/list.json', mandrill_webhook_list),
    web.post('/mandrill/webhooks/add.json', mandrill_webhook_add),
    web.post('/messagebird/lookup/{number}/hlr', messagebird_hlr_post),
    web.get('/messagebird/lookup/{number}', messagebird_lookup),
    web.post('/messagebird/messages', messagebird_send),
    web.get('/messagebird-pricing', messagebird_pricing),
    web.route('*', '/generate.pdf', generate_pdf),
]
