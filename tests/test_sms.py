import re
import uuid
from urllib.parse import urlencode

from .test_email import get_events


async def test_send_message(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'method': 'sms-test',
        'from_name': 'foobar send',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [
            {
                'number': '07891123856',
                'context': {
                    'foo': 'bar',
                }
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    f = 'xxxxxxxxxxxxxxxxxxxx-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert ("to: Number(number='+447891123856', country_code='44', "
            "number_formatted='+44 7891 123856', descr=None, is_mobile=True)") in msg_file
    assert '\nfrom_name: foobar send\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


async def test_send_message_usa(cli, settings, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'country_code': 'US',
        'from_name': 'foobar send',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [
            {
                'number': '+1 818 337 3095',
                'context': {
                    'foo': 'bar',
                }
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    f = 'xxxxxxxxxxxxxxxxxxxx-18183373095.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert ("to: Number(number='+18183373095', country_code='1', "
            "number_formatted='+1 818-337-3095', descr=None, is_mobile=True)") in msg_file
    assert f'\nfrom_name: {settings.us_send_number}\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


async def test_validate_number(cli, tmpdir):
    data = {
        'country_code': 'US',
        'numbers': {
            123: 'xxxxx',
            234: '1 818 337 3095',
            345: '+447891123856',
            456: '+44 (0) 207 1128 953',
            567: '+12001230101',  # not possible
        }
    }
    r = await cli.get('/validate/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    data = await r.json()
    # json keys are always strings
    import json
    print(json.dumps(data, indent=2))
    assert {
        '123': None,
        '234': {
            'number': '+18183373095',
            'country_code': '1',
            'number_formatted': '+1 818-337-3095',
            'descr': 'California, United States',
            'is_mobile': True
        },
        '345': {
            'number': '+447891123856',
            'country_code': '44',
            'number_formatted': '+44 7891 123856',
            'descr': 'United Kingdom',
            'is_mobile': True
        },
        '456': {
            'number': '+442071128953',
            'country_code': '44',
            'number_formatted': '+44 20 7112 8953',
            'descr': 'London, United Kingdom',
            'is_mobile': False
        },
        '567': None,
    } == data


async def test_repeat_uuid(cli, tmpdir):
    data = {
        'uid': 'a' * 20,
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': '07891123856'}]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    assert str(tmpdir.listdir()[0]).endswith('aaaaaaaaaaaaaaaaaaaa-447891123856.txt')
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 409, await r.text()
    assert 'Send group with id "aaaaaaaaaaaaaaaaaaaa" already exists\n' in await r.text()


async def test_invalid_number(cli, tmpdir):
    data = {
        'uid': 'a' * 20,
        'company_code': 'foobar',
        'country_code': 'US',
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [
            {'number': '+447891123856'},  # uk mobile
            {'number': '+44 (0) 207 1128 953'},  # not mobile
            {'number': '1 818 337 3095'},  # US mobile or fix
            {'number': '+12001230101'},  # not possible
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 2
    files = {str(f).split('/')[-1] for f in tmpdir.listdir()}
    assert files == {'aaaaaaaaaaaaaaaaaaaa-18183373095.txt', 'aaaaaaaaaaaaaaaaaaaa-447891123856.txt'}


async def test_exceed_cost_limit(cli, tmpdir):
    d = {
        'company_code': 'cost-test',
        'cost_limit': 0.1,
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': f'0789112385{i}'} for i in range(4)]
    }
    r = await cli.post('/send/sms/', json=dict(uid=str(uuid.uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert {'status': 'enqueued', 'spend': 0.0} == await r.json()
    assert len(tmpdir.listdir()) == 4
    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.post('/send/sms/', json=dict(uid=str(uuid.uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert {'status': 'enqueued', 'spend': 0.048} == await r.json()
    assert len(tmpdir.listdir()) == 8

    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.post('/send/sms/', json=dict(uid=str(uuid.uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    obj = await r.json()
    assert 0.095 < obj['spend'] < 0.097
    assert len(tmpdir.listdir()) == 12

    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.post('/send/sms/', json=dict(uid=str(uuid.uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 402, await r.text()
    obj = await r.json()
    assert 0.143 < obj['spend'] < 0.145
    assert obj['cost_limit'] == 0.1
    assert len(tmpdir.listdir()) == 12


async def test_send_messagebird(cli, tmpdir, mock_external):
    data = {
        'uid': str(uuid.uuid4()),
        'company_code': 'foobar',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'number': '07801234567'}]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert [
        'POST /messagebird/lookup/447801234567/hlr > 201',
        'GET /messagebird/lookup/447801234567 > 200',
        'GET /messagebird-pricing?username=mb-username&password=mb-password > 200',
        'POST /messagebird/messages > 201',
    ] == mock_external.app['request_log']
    mock_external.app['request_log'] = []

    # send again, this time hlr look and pricing requests shouldn't occur
    data = dict(data, uid=str(uuid.uuid4()))
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert [
        'POST /messagebird/messages > 201',
    ] == mock_external.app['request_log']


async def test_messagebird_webhook(cli, mock_external):
    data = {
        'uid': str(uuid.uuid4()),
        'company_code': 'webhook-test',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [
            {
                'first_name': 'John',
                'last_name': 'Doe',
                'user_link': 4321,
                'number': '07801234567'
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()

    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.server.app['es'].get('messages/sms-messagebird/_search?q=company:webhook-test')
    response_data = await r.json()
    # import json
    # print(json.dumps(response_data, indent=2))
    assert response_data['hits']['total'] == 1
    source = response_data['hits']['hits'][0]['_source']
    assert source['status'] == 'send'
    assert source['to_first_name'] == 'John'
    assert source['to_last_name'] == 'Doe'
    assert source['to_user_link'] == '4321'
    assert source['to_address'] == '+44 7801 234567'
    assert source['from_name'] == 'Morpheus'
    assert source['body'] == 'this is a message'
    assert source['cost'] == 0.02
    assert len(source['tags']) == 1  # just group_id
    events = await get_events(cli, response_data['hits']['hits'][0]['_id'], es_type='sms-messagebird')
    assert events['hits']['total'] == 0

    url_args = {
        'id': response_data['hits']['hits'][0]['_id'],
        'reference': 'morpheus',
        'recipient': '447801234567',
        'status': 'delivered',
        'statusDatetime': '2032-06-06T12:00:00',
    }
    r = await cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
    assert r.status == 200, await r.text()

    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.server.app['es'].get('messages/sms-messagebird/_search?q=company:webhook-test')
    response_data = await r.json()
    assert response_data['hits']['total'] == 1
    source = response_data['hits']['hits'][0]['_source']

    assert source['status'] == 'delivered'
    events = await get_events(cli, response_data['hits']['hits'][0]['_id'], es_type='sms-messagebird')
    assert events['hits']['total'] == 1
    assert events['hits']['hits'][0]['_source']['status'] == 'delivered'


async def test_failed_render(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'test_failed_render',
        'method': 'sms-test',
        'context': {'foo': 'FOO'},
        'main_template': 'this is a message {{ foo }',
        'recipients': [{'number': '07891123856'}]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 0

    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.server.app['es'].get('messages/sms-test/_search?q=company:test_failed_render')
    response_data = await r.json()
    assert response_data['hits']['total'] == 1
    source = response_data['hits']['hits'][0]['_source']
    assert source['status'] == 'render_failed'


async def test_link_shortening(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'sms_test_link_shortening',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [
            {
                'number': '07891123856',
                'context': {'foo': 'http://whatever.com/foo/bar'}
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    f = 'xxxxxxxxxxxxxxxxxxxx-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert '\nfrom_name: Morpheus\n' in msg_file
    assert '\nmessage:\nthis is a message click.example.com/l' in msg_file
    token = re.search('message click.example.com/l(.+?)\n', msg_file).groups()[0]
    assert len(token) == 12

    await cli.server.app['es'].get('links/_refresh')
    r = await cli.server.app['es'].get('links/c/_search?q=company:sms_test_link_shortening')
    response_data = await r.json()
    assert response_data['hits']['total'] == 1
    v = response_data['hits']['hits'][0]['_source']
    assert v['url'] == 'http://whatever.com/foo/bar'
    assert v['token'] == token

    r = await cli.get('/l' + token, allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'http://whatever.com/foo/bar'


async def test_send_multi_part(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}\n' * 10,
        'recipients': [
            {
                'number': '07891123856',
                'context': {
                    'foo': 'bar',
                }
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    f = 'xxxxxxxxxxxxxxxxxxxx-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file: str = tmpdir.join(f).read()
    print(msg_file)
    assert '\nlength: SmsLength(length=230, parts=2)\n' in msg_file
    assert msg_file.count('this is a message bar') == 10


async def test_send_too_long(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'x' * 1500,
        'recipients': [
            {
                'number': '07891123856',
                'context': {
                    'foo': 'bar',
                }
            }
        ]
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    # no messages sent:
    assert len(tmpdir.listdir()) == 0
