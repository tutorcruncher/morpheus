import base64
import json
import uuid
from datetime import datetime

import msgpack
from arq.utils import to_unix_ms
from cryptography.fernet import Fernet


def user_auth(settings, company='foobar'):
    session_data = {
        'company': company,
        'expires': to_unix_ms(datetime(2032, 1, 1))[0]
    }
    f = Fernet(base64.urlsafe_b64encode(settings.user_fernet_key))
    return f.encrypt(msgpack.packb(session_data, encoding='utf8')).decode()


async def test_user_list(cli, settings, send_message):
    await cli.server.app['es'].create_indices(True)

    expected_msg_ids = []
    for i in range(4):
        uid = str(uuid.uuid4())
        await send_message(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    await send_message(uid=str(uuid.uuid4()), company_code='different1')
    await send_message(uid=str(uuid.uuid4()), company_code='different2')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get('/user/email-test/', headers={'Authorization': user_auth(settings, company='whoever')})
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

    r = await cli.get('/user/email-test/', headers={'Authorization': user_auth(settings, company='__all__')})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 6


async def test_user_search(cli, settings, send_message):
    msgs = {}
    for i, subject in enumerate(['apple', 'banana', 'cherry', 'durian']):
        uid = str(uuid.uuid4())
        await send_message(uid=uid, company_code='whoever',
                           recipients=[{'address': f'{i}@t.com'}], subject_template=subject)
        msgs[subject] = f'{uid}-{i}tcom'

    await send_message(uid=str(uuid.uuid4()), company_code='different1', subject_template='eggplant')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get('/user/email-test/?q=cherry',
                      headers={'Authorization': user_auth(settings, company='whoever')})
    assert r.status == 200, await r.text()
    data = await r.json()
    if not data['hits']['total']:
        print('no results from cherry search, db...')
        r = await cli.server.app['es'].get('messages/email-test')
        print(json.dumps(await r.json(), indent=2))
    assert data['hits']['total'] == 1
    hit = data['hits']['hits'][0]
    assert hit['_id'] == msgs['cherry']
    assert hit['_index'] == 'messages'
    assert hit['_source']['subject'] == 'cherry'
    r = await cli.get('/user/email-test/?q=eggplant',
                      headers={'Authorization': user_auth(settings, company='whoever')})
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert data['hits']['total'] == 0


async def test_user_aggregate(cli, settings, send_message):
    await cli.server.app['es'].create_indices(True)

    for i in range(4):
        await send_message(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'{i}@t.com'}])

    await send_message(uid=str(uuid.uuid4()), company_code='different')
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.get('/user/email-test/aggregation/', headers={'Authorization': user_auth(settings, 'whoever')})
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    buckets = data['aggregations']['_']['_']['buckets']
    assert len(buckets) == 1
    assert buckets[0]['doc_count'] == 4
    assert buckets[0]['send']['doc_count'] == 4
    assert buckets[0]['open']['doc_count'] == 0
    r = await cli.get('/user/email-test/aggregation/', headers={'Authorization': user_auth(settings, '__all__')})
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['aggregations']['_']['_']['buckets'][0]['doc_count'] == 5


async def test_user_tags(cli, settings, send_message):
    uid1 = str(uuid.uuid4())
    await send_message(
        uid=uid1,
        company_code='tagtest',
        tags=['trigger:broadcast', 'broadcast:123'],
        recipients=[
            {'address': '1@t.com', 'tags': ['user:1', 'shoesize:10']},
            {'address': '2@t.com', 'tags': ['user:2', 'shoesize:8']},
        ]
    )
    uid2 = str(uuid.uuid4())
    await send_message(
        uid=uid2,
        company_code='tagtest',
        tags=['trigger:other'],
        recipients=[
            {'address': '3@t.com', 'tags': ['user:3', 'shoesize:10']},
            {'address': '4@t.com', 'tags': ['user:4', 'shoesize:8']},
        ]
    )

    await send_message(uid=str(uuid.uuid4()), company_code='different1')
    await send_message(uid=str(uuid.uuid4()), company_code='different2')
    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get(
        cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'broadcast:123')]),
        headers={'Authorization': user_auth(settings, company='tagtest')}
    )
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 2, json.dumps(data, indent=2)
    assert {h['_id'] for h in data['hits']['hits']} == {f'{uid1}-1tcom', f'{uid1}-2tcom'}

    r = await cli.get(
        cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'user:2')]),
        headers={'Authorization': user_auth(settings, company='tagtest')}
    )
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 1, json.dumps(data, indent=2)
    assert data['hits']['hits'][0]['_id'] == f'{uid1}-2tcom'

    query = [('tags', 'trigger:other'), ('tags', 'shoesize:8')]
    r = await cli.get(
        cli.server.app.router['user-messages'].url_for(method='email-test').with_query(query),
        headers={'Authorization': user_auth(settings, company='tagtest')}
    )
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['hits']['total'] == 1, json.dumps(data, indent=2)
    assert data['hits']['hits'][0]['_id'] == f'{uid2}-4tcom'
