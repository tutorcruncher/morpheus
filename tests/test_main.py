import base64
import hashlib
import hmac
import json
from datetime import datetime

import msgpack
from arq.utils import to_unix_ms
from cryptography.fernet import Fernet


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert 'Morpheus - The Greek God' in await r.text()


async def test_index_head(cli):
    r = await cli.head('/')
    assert r.status == 200
    assert '' == await r.text()


async def test_robots(cli):
    r = await cli.get('/robots.txt')
    assert r.status == 200
    assert 'User-agent: *' in await r.text()


async def test_favicon(cli):
    r = await cli.get('/favicon.ico', allow_redirects=False)
    assert r.status == 301
    assert r.headers['Location'] == 'https://secure.tutorcruncher.com/favicon.ico'


async def test_send_message(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'markdown_template': '# hello\n\nThis is a **{{ b }}**.\n',
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {
            'a': 'Apple',
            'b': f'Banana',
        },
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': f'bar',
                'address': f'foobar@example.com',
            }
        ]
    }
    r = await cli.post('/send/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join('xxxxxxxxxxxxxxxxxxxx-foobarexamplecom.txt').read()
    print(msg_file)
    assert '\nsubject: test email Apple\n' in msg_file
    assert '\n<p>This is a <strong>Banana</strong>.</p>\n' in msg_file
    assert '"from_email": "s@muelcolvin.com",\n' in msg_file
    assert '"to_email": "foobar@example.com",\n' in msg_file


async def test_webhook(cli, message_id):
    r = await cli.server.app['es'].get('messages/email-test/xxxxxxxxxxxxxxxxxxxx-foobartestingcom')
    data = await r.json()
    assert data['_source']['status'] == 'send'
    first_update_ts = data['_source']['update_ts']
    assert data['_source']['send_ts'] == first_update_ts
    assert len(data['_source']['events']) == 0
    data = {
        'ts': int(1e10),
        'event': 'open',
        '_id': message_id,
        'foobar': ['hello', 'world']
    }
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    r = await cli.server.app['es'].get('messages/email-test/xxxxxxxxxxxxxxxxxxxx-foobartestingcom')
    data = await r.json()
    assert data['_source']['status'] == 'open'
    assert len(data['_source']['events']) == 1
    assert data['_source']['update_ts'] > first_update_ts


async def test_mandrill_send(cli, send_message):
    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobartestingcom', allowed_statuses='*')
    assert r.status == 404, await r.text()
    await send_message(method='email-mandrill')

    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobartestingcom', allowed_statuses='*')
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['_source']['to_email'] == 'foobar@testing.com'


async def test_mandrill_webhook(cli):
    await cli.server.app['es'].post(
        f'messages/email-mandrill/test-webhook',
        company='foobar',
        send_ts=123,
        update_ts=123,
        status='send',
        to_email='testing@example.com',
        events=[]
    )
    r = await cli.server.app['es'].get('messages/email-mandrill/test-webhook')
    assert r.status == 200
    data = await r.json()
    assert len(data['_source']['events']) == 0
    messages = [{'ts': int(1e10), 'event': 'open', '_id': 'test-webhook', 'foobar': ['hello', 'world']}]
    data = {'mandrill_events': json.dumps(messages)}

    sig = base64.b64encode(
        hmac.new(
            b'testing',
            msg=(b'https://None/webhook/mandrill/mandrill_events[{"ts": 10000000000, '
                 b'"event": "open", "_id": "test-webhook", "foobar": ["hello", "world"]}]'),
            digestmod=hashlib.sha1
        ).digest()
    )
    r = await cli.post('/webhook/mandrill/', data=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status == 200, await r.text()
    r = await cli.server.app['es'].get('messages/email-mandrill/test-webhook')
    assert r.status == 200
    data = await r.json()
    assert len(data['_source']['events']) == 1
    assert data['_source']['update_ts'] == 1e13
    assert data['_source']['status'] == 'open'


def user_auth(settings, company='foobar'):
    session_data = {
        'company': company,
        'user_id': 123,
        'expires': to_unix_ms(datetime(2032, 1, 1))[0]
    }
    f = Fernet(base64.urlsafe_b64encode(settings.user_fernet_key))
    return f.encrypt(msgpack.packb(session_data, encoding='utf8')).decode()


async def test_user_list_messages(cli, settings, send_message):
    msg_id = await send_message(company_code='whoever')
    await cli.server.app['es'].post('messages/_refresh')
    r = await cli.get('/user/email-test/', headers={'Authorization': user_auth(settings, company='whoever')})
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    assert len(data['hits']['hits']) == 1
    hit = data['hits']['hits'][0]
    assert hit['_id'] == msg_id
    assert hit['_source']['company'] == 'whoever'
    assert hit['_source']['status'] == 'send'


async def test_user_aggregate(cli, settings, send_message):
    await send_message(company_code='whichever')
    await cli.server.app['es'].post('messages/_refresh')
    r = await cli.get('/user/email-test/aggregation/', headers={'Authorization': user_auth(settings, 'whichever')})
    assert r.status == 200, await r.text()
    data = await r.json()
    print(json.dumps(data, indent=2))
    buckets = data['aggregations']['_']['_']['buckets']
    assert len(buckets) == 1
    assert buckets[0]['doc_count'] == 1
    assert buckets[0]['send']['doc_count'] == 1
    assert buckets[0]['open']['doc_count'] == 0
