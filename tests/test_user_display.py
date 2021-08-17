import hashlib
import hmac
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from foxglove import glove
from operator import itemgetter
from pytest_toolbox.comparison import RegexStr
from urllib.parse import urlencode

from src.models import Company, Event, Message
from src.schema import MessageStatus


def modify_url(url, settings, company='foobar'):
    args = dict(company=company, expires=round(datetime(2032, 1, 1).timestamp()))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    return str(url) + ('&' if '?' in str(url) else '?') + urlencode(args)


def test_user_list(cli, settings, send_email, db):
    expected_msg_ids = []
    for i in range(4):
        uid = str(uuid.uuid4())
        send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    send_email(uid=str(uuid.uuid4()), company_code='different1')
    send_email(uid=str(uuid.uuid4()), company_code='different2')
    r = cli.get(modify_url('/messages/email-test/', settings, 'whoever'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 4
    msg_ids = [h['external_id'] for h in data['items']]
    assert msg_ids == list(reversed(expected_msg_ids))
    first_item = data['items'][0]
    assert first_item == {
        'id': Message.manager.get(db, external_id=expected_msg_ids[3]).id,
        'external_id': expected_msg_ids[3],
        'to_ext_link': None,
        'to_address': '3@t.com',
        'to_dst': '<3@t.com>',
        'to_name': ' ',
        'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'status': 'Sent',
        'method': 'email-test',
        'subject': 'test message',
    }


def test_user_list_sms(cli, settings, send_sms, db):
    send_sms(company_code='testing')

    r = cli.get(modify_url('/messages/sms-test/', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1
    assert len(data['items']) == 1
    assert data['items'][0] == {
        'id': Message.manager.get(db).id,
        'external_id': Message.manager.get(db).external_id,
        'to_ext_link': None,
        'to_address': '+44 7896 541236',
        'to_dst': '<+44 7896 541236>',
        'to_name': ' ',
        'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'status': 'Sent',
        'method': 'sms-test',
        'subject': None,
    }


def test_user_search(cli, settings, send_email):
    msgs = {}
    for i, subject in enumerate(['apple', 'banana', 'cherry', 'durian']):
        uid = str(uuid.uuid4())
        send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}], subject_template=subject)
        msgs[subject] = f'{uid}-{i}tcom'

    send_email(uid=str(uuid.uuid4()), company_code='different1', subject_template='eggplant')

    r = cli.get(modify_url('/messages/email-test/?q=cherry', settings, 'whoever'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1
    item = data['items'][0]
    assert item['external_id'] == msgs['cherry']
    assert item['subject'] == 'cherry'
    r = cli.get(modify_url('/messages/email-test/?q=eggplant', settings, 'whoever'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 0


def test_user_search_space(cli, settings, send_email):
    send_email(
        company_code='testing',
        recipients=[{'address': 'testing@example.com'}],
        subject_template='foobar',
    )
    send_email(
        company_code='testing',
        recipients=[{'address': 'testing@example.com'}],
        subject_template='bar',
    )
    send_email(
        company_code='testing',
        recipients=[{'address': 'testing@example.com'}],
        subject_template='foo bar',
    )

    r = cli.get(modify_url('/messages/email-test/?q=foobar', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1

    r = cli.get(modify_url('/messages/email-test/?q=foo%20bar', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1


def test_pagination(cli, settings, send_email):
    for i in range(110):
        send_email(
            uid=str(uuid.uuid4()),
            company_code='testing',
            recipients=[{'address': f'{i}@t.com'}],
            subject_template='foobar',
        )

    for i in range(20):
        send_email(
            uid=str(uuid.uuid4()),
            company_code='testing',
            recipients=[{'address': f'{i}@t.com'}],
            subject_template='barfoo',
        )

    r = cli.get(modify_url('/messages/email-test/', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    first_item = data['items'][0]
    assert len(data['items']) == 100
    assert data['count'] == 130

    r = cli.get(modify_url('/messages/email-test/', settings, 'testing') + '&offset=100')
    assert r.status_code == 200, r.text
    data = r.json()
    assert first_item not in data['items']
    assert len(data['items']) == 30
    assert data['count'] == 130


def test_user_aggregate(cli, settings, send_email, db, loop, worker):
    for i in range(4):
        send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])
    msg_id = send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])

    data = {'ts': int(2e10), 'event': 'open', '_id': msg_id, 'user_agent': 'testincalls'}
    cli.post('/webhook/test/', json=data)

    send_email(uid=str(uuid.uuid4()), company_code='different')
    loop.run_until_complete(glove.redis.enqueue_job('update_aggregation_view'))
    loop.run_until_complete(worker.run_check())

    assert Message.manager.count(db) == 6
    r = cli.get(modify_url('/messages/email-test/aggregation/', settings, 'user-aggs'))
    assert r.status_code == 200, r.text
    data = r.json()
    histogram = data.pop('histogram')
    assert data == {
        'all_90_day': 5,
        'open_90_day': 1,
        'all_7_day': 5,
        'open_7_day': 1,
        'all_28_day': 5,
        'open_28_day': 1,
    }
    assert sorted(histogram, key=itemgetter('count')) == [
        {'count': 1, 'day': f'{date.today():%Y-%m-%d}', 'status': 'Opened'},
        {'count': 4, 'day': f'{date.today():%Y-%m-%d}', 'status': 'Sent'},
    ]


def test_user_aggregate_no_data(cli, settings, db):
    Company.manager.create(db, code='testing')
    r = cli.get(modify_url('/messages/email-test/aggregation/', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data == {
        'histogram': [],
        'all_90_day': 0,
        'open_90_day': 0,
        'all_7_day': 0,
        'open_7_day': 0,
        'all_28_day': 0,
        'open_28_day': 0,
    }


def test_user_tags(cli, settings, send_email):
    uid1 = str(uuid.uuid4())
    send_email(
        uid=uid1,
        company_code='tagtest',
        tags=['trigger:broadcast', 'broadcast:123'],
        recipients=[
            {'address': '1@t.com', 'tags': ['user:1', 'shoesize:10']},
            {'address': '2@t.com', 'tags': ['user:2', 'shoesize:8']},
        ],
    )
    uid2 = str(uuid.uuid4())
    send_email(
        uid=uid2,
        company_code='tagtest',
        tags=['trigger:other'],
        recipients=[
            {'address': '3@t.com', 'tags': ['user:3', 'shoesize:10']},
            {'address': '4@t.com', 'tags': ['user:4', 'shoesize:8']},
        ],
    )

    send_email(uid=str(uuid.uuid4()), company_code='different1')
    send_email(uid=str(uuid.uuid4()), company_code='different2')

    r = cli.get(modify_url('/messages/email-test/', settings, 'tagtest') + '&tags=broadcast:123')
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 2, json.dumps(data, indent=2)
    assert {h['external_id'] for h in data['items']} == {f'{uid1}-1tcom', f'{uid1}-2tcom'}

    r = cli.get(modify_url('/messages/email-test/', settings, 'tagtest') + '&tags=user:2')
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1, json.dumps(data, indent=2)
    assert data['items'][0]['external_id'] == f'{uid1}-2tcom'

    r = cli.get(modify_url('/messages/email-test/', settings, 'tagtest') + '&tags=trigger:other&tags=shoesize:8')
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1
    assert data['items'][0]['external_id'] == f'{uid2}-4tcom'


def test_message_details(cli, settings, send_email, db, worker, loop):
    msg_ext_id = send_email(company_code='test-details')

    data = {'ts': int(1e10), 'event': 'open', '_id': msg_ext_id, 'user_agent': 'testincalls'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert loop.run_until_complete(worker.run_check()) == 2

    message = Message.manager.get(db, external_id=msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/{message.id}/', settings, 'test-details'))
    assert r.status_code == 200, r.text
    assert r.json() == {
        'id': message.id,
        'external_id': msg_ext_id,
        'to_ext_link': None,
        'to_address': 'foobar@testing.com',
        'to_dst': '<foobar@testing.com>',
        'to_name': ' ',
        'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'subject': 'test message',
        'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'status': 'Opened',
        'method': 'email-test',
        'body': '<body>\nthis is a test\n</body>',
        'events': [
            {
                'status': 'Opened',
                'datetime': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
                'details': (
                    '{\n  "user_agent": "testincalls",\n  "location": null,\n  "bounce_description": null,\n  '
                    '"clicks": null,\n  "diag": null,\n  "reject": null,\n  "opens": null,\n  "resends": null,\n  '
                    '"smtp_events": null,\n  "state": null\n}'
                ),
            }
        ],
        'attachments': [],
    }


def test_message_details_links(cli, settings, send_email, db, worker, loop):
    msg_ext_id = send_email(
        company_code='test-details',
        recipients=[
            {
                'first_name': 'Foo',
                'last_name': 'Bar',
                'user_link': '/whatever/123/',
                'address': 'foobar@testing.com',
                'pdf_attachments': [
                    {'name': 'testing.pdf', 'html': '<h1>testing</h1>', 'id': 123},
                    {'name': 'different.pdf', 'html': '<h1>different</h1>'},
                ],
            }
        ],
    )
    message = Message.manager.get(db, external_id=msg_ext_id)
    data = {'ts': int(2e12), 'event': 'open', '_id': msg_ext_id, 'user_agent': 'testincalls'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert loop.run_until_complete(worker.run_check()) == 2
    r = cli.get(modify_url(f'/messages/email-test/{message.id}/', settings, 'test-details'))
    assert r.status_code == 200, r.text
    assert r.json() == {
        'id': message.id,
        'external_id': msg_ext_id,
        'to_ext_link': '/whatever/123/',
        'to_address': 'foobar@testing.com',
        'to_dst': 'Foo Bar <foobar@testing.com>',
        'to_name': 'Foo Bar',
        'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'subject': 'test message',
        'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'status': 'Opened',
        'body': '<body>\nthis is a test\n</body>',
        'method': 'email-test',
        'events': [
            {
                'status': 'Opened',
                'datetime': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
                'details': (
                    '{\n  "user_agent": "testincalls",\n  "location": null,\n  "bounce_description": null,\n  '
                    '"clicks": null,\n  "diag": null,\n  "reject": null,\n  "opens": null,\n  "resends": null,\n  '
                    '"smtp_events": null,\n  "state": null\n}'
                ),
            }
        ],
        'attachments': [
            ['/attachment-doc/123/', 'testing.pdf'],
            ['#', 'different.pdf'],
        ],
    }


def test_no_event_data(cli, settings, send_email, db):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message = Message.manager.get(db, external_id=msg_ext_id)
    events = [
        Event(
            ts=(datetime(2032, 6, 1) + timedelta(days=i, hours=i * 2)).replace(tzinfo=timezone.utc),
            message_id=message.id,
            status=MessageStatus.send,
        )
        for i in range(3)
    ]
    Event.manager.create_many(db, *events)
    r = cli.get(modify_url(f'/messages/email-test/{message.id}/', settings, 'test-details'))
    assert r.json()['events'] == [
        {'status': 'Sent', 'datetime': '2032-06-01T00:00:00+00:00'},
        {'status': 'Sent', 'datetime': '2032-06-02T02:00:00+00:00'},
        {'status': 'Sent', 'datetime': '2032-06-03T04:00:00+00:00'},
    ]


def test_invalid_message_id(cli, db, settings, send_email):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message = Message.manager.get(db, external_id=msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/{message.id}/', settings, 'not_real_company'))
    assert r.status_code == 404

    r = cli.get(modify_url(f'/messages/email-test/999{message.id}/', settings, 'test-details'))
    assert r.status_code == 404


def test_many_events(cli, settings, send_email, db):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message = Message.manager.get(db, external_id=msg_ext_id)
    events = [
        Event(
            ts=(datetime(2032, 6, 1) + timedelta(days=i, hours=i * 2)).replace(tzinfo=timezone.utc),
            message_id=message.id,
            status=MessageStatus.send,
            extra=json.dumps({'foo': 'bar', 'v': i}),
        )
        for i in range(55)
    ]
    Event.manager.create_many(db, *events)

    r = cli.get(modify_url(f'/messages/email-test/{message.id}/', settings, 'test-details'))
    assert r.status_code == 200, r.text
    events = r.json()['events']
    assert len(events) == 51
    assert Event.manager.count(db) == 55
    assert events[-1]['status'] == '5 more'


def test_user_sms_list(cli, settings, send_sms, db):
    ext_id = send_sms(company_code='snapcrap')

    send_sms(uid=str(uuid.uuid4()), company_code='flip')
    r = cli.get(modify_url('/messages/sms-test/', settings, 'snapcrap'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data == {
        'items': [
            {
                'id': 1,
                'external_id': ext_id,
                'to_ext_link': None,
                'to_address': '+44 7896 541236',
                'to_dst': '<+44 7896 541236>',
                'to_name': ' ',
                'subject': None,
                'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
                'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
                'status': 'Sent',
                'method': 'sms-test',
            },
        ],
        'count': 1,
        'spend': 0.012,
    }


def test_valid_signature(cli, settings, db):
    Company.manager.create(db, code='whatever')
    args = dict(company='whatever', expires=round(datetime(2032, 1, 1).timestamp()))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 200, r.text


def test_invalid_signature(cli, settings):
    args = dict(company='whatever', expires=round(datetime(2032, 1, 1).timestamp()))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest() + 'xxx'
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 403, r.text
    assert {'message': 'Invalid token'} == r.json()


def test_invalid_expiry(cli, settings):
    args = dict(company='whatever', expires='xxx')
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 422, r.text
    assert {
        "detail": [
            {"loc": ["query", "expires"], "msg": "invalid datetime format", "type": "value_error.datetime"},
            {"loc": ["query", "expires"], "msg": "invalid datetime format", "type": "value_error.datetime"},
        ]
    } == r.json()


def test_sig_expired(cli, settings):
    args = dict(company='whatever', expires=round(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 403, r.text
    assert {'message': 'Token expired'} == r.json()
