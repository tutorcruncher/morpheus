import re
from buildpg import V
from buildpg.clauses import Where
from datetime import datetime, timedelta
from foxglove.db.helpers import SyncDb
from urllib.parse import urlencode
from uuid import uuid4

from src.main import settings


def test_send_message(cli, tmpdir, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d71',
        'company_code': 'foobar',
        'method': 'sms-test',
        'from_name': 'foobar send',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d71-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    assert (
        "to: Number(number='+447891123856', country_code='44', "
        "number_formatted='+44 7891 123856', descr=None, is_mobile=True)"
    ) in msg_file
    assert f'\nfrom_name: {settings.tc_registered_originator}\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


def test_send_message_usa(cli, settings, tmpdir, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d72',
        'company_code': 'foobar',
        'country_code': 'US',
        'from_name': 'foobar send',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '+1 818 337 3095', 'context': {'foo': 'bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d72-18183373095.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    assert (
        "to: Number(number='+18183373095', country_code='1', "
        "number_formatted='+1 818-337-3095', descr=None, is_mobile=True)"
    ) in msg_file
    assert f'\nfrom_name: {settings.us_send_number}\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


def test_send_message_canada(cli, settings, tmpdir, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d72',
        'company_code': 'foobar',
        'country_code': 'CA',
        'from_name': 'foobar send',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '+1 818 337 3095', 'context': {'foo': 'bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d72-18183373095.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    assert (
        "to: Number(number='+18183373095', country_code='1', "
        "number_formatted='+1 818-337-3095', descr=None, is_mobile=True)"
    ) in msg_file
    assert f'\nfrom_name: {settings.canada_send_number}\n' in msg_file
    assert '\nmessage:\nthis is a message bar\n' in msg_file
    assert '\nlength: SmsLength(length=21, parts=1)\n' in msg_file


def test_validate_number(cli, tmpdir):
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
    r = cli.get('/validate/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 200, r.text
    data = r.json()
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


def test_repeat_uuid(cli, tmpdir, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d73',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': '07891123856'}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    assert str(tmpdir.listdir()[0]).endswith('69eb85e8-1504-40aa-94ff-75bb65fd8d73-447891123856.txt')
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 409, r.text
    data = r.json()
    assert {'message': 'Send group with id "69eb85e8-1504-40aa-94ff-75bb65fd8d73" already exists\n'} == data


def test_invalid_number(cli, tmpdir, worker, loop):
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
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 4
    assert len(tmpdir.listdir()) == 2
    files = {str(f).split('/')[-1] for f in tmpdir.listdir()}
    assert files == {
        '69eb85e8-1504-40aa-94ff-75bb65fd8d74-18183373095.txt',
        '69eb85e8-1504-40aa-94ff-75bb65fd8d74-447891123856.txt',
    }


def test_exceed_cost_limit(cli, tmpdir, worker, loop, sync_db, send_sms, send_webhook):
    d = {
        'company_code': 'cost-test',
        'cost_limit': 0.1,
        'method': 'sms-test',
        'main_template': 'this is a message',
        'recipients': [{'number': f'0789112385{i}'} for i in range(4)],
    }

    where = Where(V('company_id') == 1)

    r = cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 4
    assert {'status': 'enqueued', 'spend': 0.0} == r.json()
    assert len(tmpdir.listdir()) == 4

    msg_ext_ids = sync_db.fetchval_b('select array_agg(external_id) from messages :where', where=where)
    for ext_id in msg_ext_ids:
        send_webhook(ext_id, 0.03)

    r = cli.post('/send/sms/', json=dict(uid=str(uuid4()), **d), headers={'Authorization': 'testing-key'})
    assert r.status_code == 402, r.text
    obj = r.json()

    assert obj['spend'] == 0.12
    assert obj['cost_limit'] == 0.1


def test_send_messagebird(cli, tmpdir, dummy_server, worker, loop):
    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'number': '07801234567'}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert 'POST /messagebird/messages > 201' in dummy_server.log

    # send again, this time hlr look and pricing requests shouldn't occur
    data = dict(data, uid=str(uuid4()))
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 2
    assert len(dummy_server.log) == 2
    assert dummy_server.log[1] == 'POST /messagebird/messages > 201'


def test_messagebird_webhook_sms_pricing(cli, sync_db: SyncDb, dummy_server, worker, loop):
    data = {
        'uid': str(uuid4()),
        'company_code': 'webhook-test',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'first_name': 'John', 'last_name': 'Doe', 'user_link': 4321, 'number': '07801234567'}],
    }

    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1

    msg = sync_db.fetchrow_b('select * from messages join message_groups g on g.id = messages.id')
    assert msg['status'] == 'send'
    assert msg['to_first_name'] == 'John'
    assert msg['to_last_name'] == 'Doe'
    assert msg['to_user_link'] == '4321'
    assert msg['to_address'] == '+44 7801 234567'
    assert msg['from_name'] == 'Morpheus'
    assert msg['body'] == 'this is a message'
    assert msg['cost'] is None
    assert len(msg['tags']) == 1  # just group_id

    url_args = {
        'id': msg['external_id'],
        'reference': 'morpheus',
        'recipient': '447801234567',
        'status': 'delivered',
        'statusDatetime': '2032-06-06T12:00:00',
        'price[amount]': 0.07,
    }
    r = cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
    assert r.status_code == 200, r.text
    assert worker.test_run() == 2

    msg = sync_db.fetchrow_b('select * from messages')
    assert msg['status'] == 'delivered'
    assert msg['cost'] == 0.07


def test_messagebird_webhook_carrier_failed(cli, sync_db: SyncDb, dummy_server, worker, loop):
    data = {
        'uid': str(uuid4()),
        'company_code': 'webhook-test',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'first_name': 'John', 'last_name': 'Doe', 'user_link': 4321, 'number': '07801234567'}],
    }

    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1

    msg = sync_db.fetchrow_b('select * from messages join message_groups g on g.id = messages.id')
    assert msg['status'] == 'send'
    assert msg['to_first_name'] == 'John'
    assert msg['to_last_name'] == 'Doe'
    assert msg['to_user_link'] == '4321'
    assert msg['to_address'] == '+44 7801 234567'
    assert msg['from_name'] == 'Morpheus'
    assert msg['body'] == 'this is a message'
    assert msg['cost'] is None
    assert len(msg['tags']) == 1  # just group_id

    url_args = {
        'id': msg['external_id'],
        'reference': 'morpheus',
        'recipient': '447801234567',
        'status': 'delivery_failed',
        'statusDatetime': '2032-06-06T12:00:00',
        'statusReason': 'carrier+rejected',
        'statusErrorCode': 104,
    }

    r = cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
    assert r.status_code == 200, r.text
    assert worker.test_run() == 2

    msg = sync_db.fetchrow_b('select * from messages')
    assert msg['status'] == 'delivery_failed'
    assert msg['cost'] is None


def test_messagebird_webhook_other_delivery_failed(cli, sync_db: SyncDb, dummy_server, worker, loop):
    data = {
        'uid': str(uuid4()),
        'company_code': 'webhook-test',
        'method': 'sms-messagebird',
        'main_template': 'this is a message',
        'recipients': [{'first_name': 'John', 'last_name': 'Doe', 'user_link': 4321, 'number': '07801234567'}],
    }

    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1

    msg = sync_db.fetchrow_b('select * from messages join message_groups g on g.id = messages.id')
    assert msg['status'] == 'send'
    assert msg['to_first_name'] == 'John'
    assert msg['to_last_name'] == 'Doe'
    assert msg['to_user_link'] == '4321'
    assert msg['to_address'] == '+44 7801 234567'
    assert msg['from_name'] == 'Morpheus'
    assert msg['body'] == 'this is a message'
    assert msg['cost'] is None
    assert len(msg['tags']) == 1  # just group_id

    url_args = {
        'id': msg['external_id'],
        'reference': 'morpheus',
        'recipient': '447801234567',
        'status': 'delivery_failed',
        'statusDatetime': '2032-06-06T12:00:00',
        'statusReason': 'unknown+subscriber',
        'statusErrorCode': 27,
    }

    r = cli.get(f'/webhook/messagebird/?{urlencode(url_args)}')
    assert r.status_code == 200, r.text
    assert worker.test_run() == 2

    msg = sync_db.fetchrow_b('select * from messages')
    assert msg['status'] == 'delivery_failed'
    assert msg['cost'] is None


def test_failed_render(cli, tmpdir, sync_db: SyncDb, worker, loop):
    data = {
        'uid': str(uuid4()),
        'company_code': 'test_failed_render',
        'method': 'sms-test',
        'context': {'foo': 'FOO'},
        'main_template': 'this is a message {{ foo }',
        'recipients': [{'number': '07891123856'}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 0

    assert sync_db.fetchrow_b('select * from messages')['status'] == 'render_failed'


def test_link_shortening(cli, tmpdir, sync_db: SyncDb, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d75',
        'company_code': 'sms_test_link_shortening',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}',
        'recipients': [{'number': '07891123856', 'context': {'foo': 'http://whatever.com/foo/bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d75-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file = tmpdir.join(f).read()
    assert f'\nfrom_name: {settings.tc_registered_originator}\n' in msg_file
    assert '\nmessage:\nthis is a message click.example.com/l' in msg_file
    token = re.search('message click.example.com/l(.+?)\n', msg_file).groups()[0]
    assert len(token) == 12

    link = sync_db.fetchrow_b('select * from links')
    assert link['url'] == 'http://whatever.com/foo/bar'
    assert link['token'] == token

    r = cli.get(f'/l{token}', allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'http://whatever.com/foo/bar'

    r = cli.get(f'/l{token}.', allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'http://whatever.com/foo/bar'


def test_send_multi_part(cli, tmpdir, worker, loop):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d76',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'this is a message {{ foo }}\n' * 10,
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
    assert len(tmpdir.listdir()) == 1
    f = '69eb85e8-1504-40aa-94ff-75bb65fd8d76-447891123856.txt'
    assert str(tmpdir.listdir()[0]).endswith(f)
    msg_file: str = tmpdir.join(f).read()
    assert '\nlength: SmsLength(length=230, parts=2)\n' in msg_file
    assert msg_file.count('this is a message bar') == 10


def test_send_too_long(cli, tmpdir):
    data = {
        'uid': '69eb85e8-1504-40aa-94ff-75bb65fd8d77',
        'company_code': 'foobar',
        'method': 'sms-test',
        'main_template': 'x' * 1500,
        'recipients': [{'number': '07891123856', 'context': {'foo': 'bar'}}],
    }
    r = cli.post('/send/sms/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    # no messages sent:
    assert len(tmpdir.listdir()) == 0


def test_sms_billing(cli, send_sms, send_webhook, sync_db):
    ext_ids = []
    for i in range(4):
        ext_id = send_sms(uid=str(uuid4()), company_code='billing-test')
        ext_ids.append(ext_id)
    for ext_id in ext_ids:
        send_webhook(ext_id, 0.012)

    start = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%d')
    end = (datetime.utcnow() + timedelta(days=5)).strftime('%Y-%m-%d')
    data = dict(start=start, end=end, company_code='billing-test')
    r = cli.get(
        '/billing/sms-test/billing-test/', json=dict(uid=str(uuid4()), **data), headers={'Authorization': 'testing-key'}
    )
    assert r.status_code == 200, r.text
    assert {'company': 'billing-test', 'start': start, 'end': end, 'spend': 0.048} == r.json()
