

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
