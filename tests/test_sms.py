import re
from urllib.parse import urlencode
from uuid import uuid4


async def test_send_message(cli, tmpdir, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d71',
        'company_code': 'foobar',
        'method': 'sms-test',
        'from_name': 'foobar send',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d71-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert (
        "to: Number(number='+447891123856', country_code='44', "
        "number_formatted='+44 7891 123856', descr=None, is_mobile=True)"
    ) in msg_file
    assert '\nfrom_name: foobar send\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


async def test_send_message_usa(cli, settings, tmpdir, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d72',
        'company_code': 'foobar',
        'country_code': 'US',
        'from_name': 'foobar send',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '+1 818 337 3095', 'context': {'foo': 'bar'}}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d72-18183373095.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert (
        "to: Number(number='+18183373095', country_code='1', "
        "number_formatted='+1 818-337-3095', descr=None, is_mobile=True)"
    ) in msg_file
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
        },
    }
    r = await cli.get('/validate/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    data = await r.json()
    # debug(data)
    assert {
        '123': None,
        '234': {
            'number': '+18183373095',
            'country_code': '1',
            'number_formatted': '+1 818-337-3095',
            'descr': 'California, United States',
            'is_mobile': True,
        },
        '345': {
            'number': '+447891123856',
            'country_code': '44',
            'number_formatted': '+44 7891 123856',
            'descr': 'United Kingdom',
            'is_mobile': True,
        },
        '456': {
            'number': '+442071128953',
            'country_code': '44',
            'number_formatted': '+44 20 7112 8953',
            'descr': 'London, United Kingdom',
            'is_mobile': False,
        },
        '567': None,
    } == data


async def test_repeat_uuid(cli, tmpdir, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d73',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': '07891123856'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    assert str(tmpdir.listdir()[0]).endswith('69eb85e8-1504-40aa-94ff-75bb65fd8d73-447891123856.txt')
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 409, await r.text()
    data = await r.json()
    assert {'message': 'Send group with id "69eb85e8-1504-40aa-94ff-75bb65fd8d73" already exists\n'} == data


async def test_invalid_number(cli, tmpdir, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d74',
        'company_code': 'foobar',
        'country_code': 'US',
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [
            {'number': '+447891123856'},  # uk mobile
            {'number': '+44 (0) 207 1128 953'},  # not mobile
            {'number': '1 818 337 3095'},  # US mobile or fix
            {'number': '+12001230101'},  # not possible
        ],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 4
    assert len(tmpdir.listdir()) == 2
    files = {str(f).split('/')[-1] for f in tmpdir.listdir()}
    assert files == {
        '69eb85e8-1504-40aa-94ff-75bb65fd8d74-18183373095.txt',
        '69eb85e8-1504-40aa-94ff-75bb65fd8d74-447891123856.txt',
    }


async def test_exceed_cost_limit(cli, tmpdir, worker):
    d = {
        'company_code': 'cost-test',
        'cost_limit': 0.1,
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': f'0789112385{i}'} for i in range(4)],
    }
    r = await cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 4
    assert {'status': 'enqueued', 'spend': 0.0} == await r.json()
    assert len(tmpdir.listdir()) == 4
    r = await cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 8
    assert {'status': 'enqueued', 'spend': 0.048} == await r.json()
    assert len(tmpdir.listdir()) == 8

    r = await cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    obj = await r.json()
    assert 0.095 < obj['spend'] < 0.097
    assert await worker.run_check() == 12
    assert len(tmpdir.listdir()) == 12

    r = await cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status == 402, await r.text()
    obj = await r.json()
    assert 0.143 < obj['spend'] < 0.145
    assert obj['cost_limit'] == 0.1
    assert len(tmpdir.listdir()) == 12


async def test_send_messagebird(cli, tmpdir, dummy_server, worker):
    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'number': '07801234567'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert [
        'POST /messagebird/lookup/447801234567/hlr > 201',
        'GET /messagebird/lookup/447801234567 > 200',
        'GET /messagebird-pricing?username=mb-username&password=mb-password > 200',
        'POST /messagebird/messages > 201',
    ] == dummy_server.log

    # send again, this time hlr look and pricing requests shouldn't occur
    data = dict(data, uid=str(uuid4()))
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 2
    assert len(dummy_server.log) == 5
    assert dummy_server.log[4] == 'POST /messagebird/messages > 201'


async def test_messagebird_no_hlr(cli, tmpdir, dummy_server, worker):
    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'number': '07888888888'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert [
        'POST /messagebird/lookup/447888888888/hlr > 201',
        'GET /messagebird/lookup/447888888888 > 200',
    ] == dummy_server.log
    dummy_server.log = []


async def test_messagebird_no_network(cli, tmpdir, dummy_server, worker):
    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'number': '07777777777'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert [
        'POST /messagebird/lookup/447777777777/hlr > 201',
        'GET /messagebird/lookup/447777777777 > 200',
    ] == dummy_server.log
    dummy_server.log = []


async def test_messagebird_webhook(cli, db_conn, dummy_server, worker):
    data = {
        'uid': str(uuid4()),
        'company_code': 'webhook-test',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'first_name': 'John', 'last_name': 'Doe', 'user_link': 4321, 'number': '07801234567'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1

    assert 1 == await db_conn.fetchval('select count(*) from messages')
    msg = await db_conn.fetchrow('select * from messages join message_groups j on messages.group_id = j.id')
    assert msg['status'] == 'send'
    assert msg['to_first_name'] == 'John'
    assert msg['to_last_name'] == 'Doe'
    assert msg['to_user_link'] == '4321'
    assert msg['to_address'] == '+44 7801 234567'
    assert msg['from_name'] == 'Morpheus'
    assert msg['body'] == 'this is a message'
    assert msg['cost'] == 0.02
    assert len(msg['tags']) == 1  # just group_id

    url_args = {
        'id': msg['external_id'],
        'reference': 'morpheus',
        'recipient': '447801234567',
        'status': 'delivered',
        'statusDatetime': '2032-06-06T12:00:00',
    }
    r = await cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    assert 1 == await db_conn.fetchval('select count(*) from messages')
    msg = await db_conn.fetchrow('select * from messages')
    assert msg['status'] == 'delivered'


async def test_failed_render(cli, tmpdir, db_conn, worker):
    data = {
        'uid': str(uuid4()),
        'company_code': 'test_failed_render',
        'method': 'sms-test',
        'context': {'foo': 'FOO'},
        'main_template': 'this is a message {{ foo }',
        'recipients': [{'number': '07891123856'}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 0

    assert 1 == await db_conn.fetchval('select count(*) from messages')
    msg = await db_conn.fetchrow('select * from messages')
    assert msg['status'] == 'render_failed'


async def test_link_shortening(cli, tmpdir, db_conn, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d75',
        'company_code': 'sms_test_link_shortening',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '07891123856', 'context': {'foo': 'http://whatever.com/foo/bar'}}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d75-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    print(msg_file)
    assert '\nfrom_name: Morpheus\n' in msg_file
    assert '\nmessage:\nthis is a message click.example.com/l' in msg_file
    token = re.search('message click.example.com/l(.+?)\n', msg_file).groups()[0]
    assert len(token) == 12

    assert 1 == await db_conn.fetchval('select count(*) from links')
    link = await db_conn.fetchrow('select * from links')
    assert link['url'] == 'http://whatever.com/foo/bar'
    assert link['token'] == token

    r = await cli.get(f'/l{token}', allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'http://whatever.com/foo/bar'

    r = await cli.get(f'/l{token}.', allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'http://whatever.com/foo/bar'


async def test_send_multi_part(cli, tmpdir, worker):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d76',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}\n' * 10,
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d76-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file: str = tmpdir.join(f).read()
    print(msg_file)
    assert '\nlength: SmsLength(length=230, parts=2)\n' in msg_file
    assert msg_file.count('this is a message bar') == 10


async def test_send_too_long(cli, tmpdir):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d77',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'x' * 1500,
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = await cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    # no messages sent:
    assert len(tmpdir.listdir()) == 0
