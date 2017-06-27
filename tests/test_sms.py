import uuid


async def test_send_message(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'method': 'sms-test',
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
    assert 'to: 447891123856\n' in msg_file
    assert '\nfrom_name: Morpheus\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file


async def test_validate_number(cli, tmpdir):
    data = {
        'country_code': 'US',
        'numbers': {
            123: 'xxxxx',
            456: '1 818 337 3095',
            789: '+447891123856',
            10: '+44 (0) 207 1128 953',
            11: '+12001230101',  # not possible
        }
    }
    r = await cli.get('/validate/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 200, await r.text()
    data = await r.json()
    # json keys are always strings
    assert {
        '123': None,
        '456': {
            'number': '18183373095',
            'formatted_number': '+1 818-337-3095',
            'descr': 'California, United States',
            'is_mobile': True
        },
        '789': {
            'number': '447891123856',
            'formatted_number': '+44 7891 123856',
            'descr': 'United Kingdom',
            'is_mobile': True
        },
        '10': {
            'number': '442071128953',
            'formatted_number': '+44 20 7112 8953',
            'descr': 'London, United Kingdom',
            'is_mobile': False
        },
        '11': None,
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
