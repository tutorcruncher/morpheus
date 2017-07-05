import hashlib
import hmac
import json
import uuid
from datetime import datetime
from urllib.parse import urlencode

from arq.utils import to_unix_ms


def modify_url(url, settings, company='foobar'):
    args = dict(
        company=company,
        expires=to_unix_ms(datetime(2032, 1, 1))
    )
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    return str(url) + ('&' if '?' in str(url) else '?') + urlencode(args)


async def test_user_list(cli, settings, send_email):
    await cli.server.app['es'].create_indices(True)

    expected_msg_ids = []
    for i in range(4):
        uid = str(uuid.uuid4())
        await send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    await send_email(uid=str(uuid.uuid4()), company_code='different1')
    await send_email(uid=str(uuid.uuid4()), company_code='different2')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get(modify_url('/user/email-test/', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert data['hits']['total'] == 4
    msg_ids = [h['_id'] for h in data['hits']['hits']]
    print(msg_ids)
    assert msg_ids == list(reversed(expected_msg_ids))
    assert len(data['hits']['hits']) == 4
    hit = data['hits']['hits'][0]
    assert hit['_source']['company'] == 'whoever'
    assert hit['_source']['status'] == 'send'

    r = await cli.get(modify_url('/user/email-test/', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 6


async def test_user_search(cli, settings, send_email):
    msgs = {}
    for i, subject in enumerate(['apple', 'banana', 'cherry', 'durian']):
        uid = str(uuid.uuid4())
        await send_email(uid=uid, company_code='whoever',
                         recipients=[{'address': f'{i}@t.com'}], subject_template=subject)
        msgs[subject] = f'{uid}-{i}tcom'

    await send_email(uid=str(uuid.uuid4()), company_code='different1', subject_template='eggplant')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get(modify_url('/user/email-test/?q=cherry', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 1
    hit = data['hits']['hits'][0]
    assert hit['_id'] == msgs['cherry']
    assert hit['_index'] == 'messages'
    assert hit['_source']['subject'] == 'cherry'
    r = await cli.get(modify_url('/user/email-test/?q=eggplant', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert data['hits']['total'] == 0


async def test_user_aggregate(cli, settings, send_email):
    await cli.server.app['es'].create_indices(True)

    for i in range(4):
        await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'{i}@t.com'}])

    await send_email(uid=str(uuid.uuid4()), company_code='different')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get(modify_url('/user/email-test/aggregation/', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    buckets = data['aggregations']['_']['buckets']
    assert len(buckets) == 1
    assert buckets[0]['doc_count'] == 4
    assert buckets[0]['send']['doc_count'] == 4
    assert buckets[0]['open']['doc_count'] == 0
    r = await cli.get(modify_url('/user/email-test/aggregation/', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['aggregations']['_']['buckets'][0]['doc_count'] == 5


async def test_user_tags(cli, settings, send_email):
    uid1 = str(uuid.uuid4())
    await send_email(
        uid=uid1,
        company_code='tagtest',
        tags=['trigger:broadcast', 'broadcast:123'],
        recipients=[
            {'address': '1@t.com', 'tags': ['user:1', 'shoesize:10']},
            {'address': '2@t.com', 'tags': ['user:2', 'shoesize:8']},
        ]
    )
    uid2 = str(uuid.uuid4())
    await send_email(
        uid=uid2,
        company_code='tagtest',
        tags=['trigger:other'],
        recipients=[
            {'address': '3@t.com', 'tags': ['user:3', 'shoesize:10']},
            {'address': '4@t.com', 'tags': ['user:4', 'shoesize:8']},
        ]
    )

    await send_email(uid=str(uuid.uuid4()), company_code='different1')
    await send_email(uid=str(uuid.uuid4()), company_code='different2')
    await cli.server.app['es'].get('messages/_refresh')

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'broadcast:123')])
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 2, json.dumps(data, indent=2)
    assert {h['_id'] for h in data['hits']['hits']} == {f'{uid1}-1tcom', f'{uid1}-2tcom'}

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'user:2')])
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 1, json.dumps(data, indent=2)
    assert data['hits']['hits'][0]['_id'] == f'{uid1}-2tcom'

    query = [('tags', 'trigger:other'), ('tags', 'shoesize:8')]
    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query(query)
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert data['hits']['total'] == 1
    assert data['hits']['hits'][0]['_id'] == f'{uid2}-4tcom'


async def test_message_preview(cli, settings, send_email):
    msg_id = await send_email(company_code='preview')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get(modify_url(f'/user/email-test/{msg_id}/preview/', settings, 'preview'))
    assert r.status == 200, await r.text()
    assert '<body>\nthis is a test\n</body>' == await r.text()


async def test_user_sms(cli, settings, send_sms):
    await cli.server.app['es'].create_indices(True)
    await send_sms(company_code='snapcrap')

    await send_sms(uid=str(uuid.uuid4()), company_code='flip')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get(modify_url('/user/sms-test/', settings, 'snapcrap'))
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert data['hits']['total'] == 1
    hit = data['hits']['hits'][0]
    assert hit['_index'] == 'messages'
    assert hit['_type'] == 'sms-test'
    assert hit['_source']['company'] == 'snapcrap'
    assert hit['_source']['status'] == 'send'
    assert hit['_source']['from_name'] == 'FooBar'
    assert hit['_source']['body'] == 'this is a test apples'
    assert hit['_source']['cost'] == 0.012
    assert hit['_source']['events'] == []

    r = await cli.get(modify_url('/user/sms-test/', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 2
