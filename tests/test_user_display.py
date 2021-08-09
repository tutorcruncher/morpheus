import hashlib
import hmac
import json
import re
import uuid
from arq.utils import to_unix_ms
from buildpg import MultipleValues, Values
from datetime import date, datetime, timedelta, timezone
from operator import itemgetter
from pytest_toolbox.comparison import RegexStr
from urllib.parse import urlencode

from src.models import Message
from src.schema import MessageStatus
from src.worker import update_aggregation_view


def modify_url(url, settings, company='foobar'):
    args = dict(company=company, expires=to_unix_ms(datetime(2032, 1, 1)))
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
        'status': 'send',
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
        'status': 'send',
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
    uid = str(uuid.uuid4())
    send_email(
        uid=uid, company_code='testing', recipients=[{'address': 'testing@example.com'}], subject_template='foobar'
    )

    r = cli.get(modify_url('/messages/email-test/?q=foobar', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1

    r = cli.get(modify_url('/messages/email-test/?q=foo%20bar', settings, 'testing'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 0


def test_user_list_lots_query_test(cli, settings, send_email):
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

    r = cli.get(modify_url('/messages/email-test/messages.html', settings, 'testing'))
    assert r.status_code == 200, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    text = r.text

    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results == 130
    assert '1 - 100' not in text
    assert f'101 - {min(results, 150)}' in text
    assert 'href="?from=100"' in text

    url = modify_url('/messages/email-test/messages.html', settings, 'testing')
    r = cli.get(url + '&q=foobar&from=100')
    assert r.status_code == 200, r.text
    text = r.text
    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results == 10
    assert '1 - 100' in text
    assert f'101 - {min(results, 150)}' not in text
    assert 'href="?q=foobar&amp;from=0"' in text


def test_user_aggregate(cli, settings, send_email, db):
    for i in range(4):
        send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])
    msg_id = send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])

    data = {'ts': int(2e10), 'event': 'open', '_id': msg_id, 'user_agent': 'testincalls'}
    cli.post('/webhook/test/', json=data)

    send_email(uid=str(uuid.uuid4()), company_code='different')

    update_aggregation_view({'pg': db})

    r = cli.get(modify_url('/messages/email-test/aggregation.json', settings, 'user-aggs'))
    assert r.status_code == 200, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
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
        {'count': 1, 'day': f'{date.today():%Y-%m-%d}', 'status': 'open'},
        {'count': 4, 'day': f'{date.today():%Y-%m-%d}', 'status': 'send'},
    ]

    r = cli.get(modify_url('/messages/email-test/aggregation.json', settings, '__all__'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert sum(v['count'] for v in data['histogram']) == 6


def test_user_aggregate_no_data(cli, settings, db):
    db.execute('insert into companies (code) values ($1)', 'testing')
    r = cli.get(modify_url('/messages/email-test/aggregation.json', settings, 'testing'))
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

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'broadcast:123')])
    r = cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 2, json.dumps(data, indent=2)
    assert {h['external_id'] for h in data['items']} == {f'{uid1}-1tcom', f'{uid1}-2tcom'}

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'user:2')])
    r = cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1, json.dumps(data, indent=2)
    assert data['items'][0]['external_id'] == f'{uid1}-2tcom'

    query = [('tags', 'trigger:other'), ('tags', 'shoesize:8')]
    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query(query)
    r = cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1
    assert data['items'][0]['external_id'] == f'{uid2}-4tcom'


def test_message_details(cli, settings, send_email, db, worker):
    msg_ext_id = send_email(company_code='test-details')

    data = {'ts': int(1e10), 'event': 'open', '_id': msg_ext_id, 'user_agent': 'testincalls'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert worker.run_check() == 2

    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/message/{message_id}.html', settings, 'test-details'))
    text = r.text
    assert r.status_code == 200, text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    spaceless = re.sub('\n +', '\n', text)
    assert '<label>Subject:</label>\n<span>test message</span>' in spaceless
    assert '<label>To:</label>\n<span>&lt;foobar@testing.com&gt;</span>' in spaceless

    assert 'Open &bull;' in text
    assert '"user_agent": "testincalls",' in text
    assert text.count('<span class="datetime">') == 3


def test_message_details_link(cli, settings, send_email, db, worker):
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

    data = {'ts': int(2e12), 'event': 'open', '_id': msg_ext_id, 'user_agent': 'testincalls'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert worker.run_check() == 2

    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    url = modify_url(f'/messages/email-test/message/{message_id}.html', settings, 'test-details')
    r = cli.get(url)
    assert r.status_code == 200, r.text
    text = r.text
    assert '<span><a href="/whatever/123/">Foo Bar &lt;foobar@testing.com&gt;</a></span>' in text
    assert '<a href="/attachment-doc/123/">testing.pdf</a>' in text
    assert '<a href="#">different.pdf</a>' in text
    d = re.search('Open &bull; .+', text).group()
    assert 'Open &bull; <span class="datetime">2033-05-18T03:33:20+00</span>' == d, text
    assert 'extra values not shown' not in text

    r = cli.get(url + '&' + urlencode({'dttz': 'Europe/London'}))
    assert r.status_code == 200, r.text
    text = r.text
    d = re.search('Open &bull; .+', text).group()
    assert 'Open &bull; <span class="datetime">2033-05-18T04:33:20+01</span>' == d, text

    r = cli.get(url + '&' + urlencode({'dttz': 'snap'}))
    assert r.status_code == 400, r.text
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    assert {'message': 'unknown timezone: "snap"'} == r.json()


def test_no_event_data(cli, settings, send_email, db):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    db.execute_b(
        'insert into events (:values__names) values :values',
        values=MultipleValues(
            *[
                Values(
                    ts=(datetime(2032, 6, 1) + timedelta(days=i, hours=i * 2)).replace(tzinfo=timezone.utc),
                    message_id=message_id,
                    status=MessageStatus.send,
                )
                for i in range(3)
            ]
        ),
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/message/{message_id}.html', settings, 'test-details'))
    assert '<div class="events" id="morpheus-accordion">\n' in r.text


def test_single_item_events(cli, settings, send_email, db):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    db.execute_b(
        'insert into events (:values__names) values :values',
        values=MultipleValues(
            *[
                Values(
                    ts=(datetime(2032, 6, 1) + timedelta(days=i, hours=i * 2)).replace(tzinfo=timezone.utc),
                    message_id=message_id,
                    status=MessageStatus.send,
                )
                for i in range(3)
            ]
        ),
    )

    url = modify_url(f'/messages/email-test/?message_id={message_id}', settings, 'test-details')
    r = cli.get(url)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['events'] == [
        {'status': 'send', 'ts': '2032-06-01T00:00:00+00', 'extra': None},
        {'status': 'send', 'ts': '2032-06-02T02:00:00+00', 'extra': None},
        {'status': 'send', 'ts': '2032-06-03T04:00:00+00', 'extra': None},
    ]


def test_invalid_message_id(cli, settings):
    url = modify_url('/messages/email-test/?message_id=foobar', settings, 'test-details')
    r = cli.get(url)
    assert r.status_code == 400, r.text
    data = r.json()
    assert data == {'message': "invalid get argument 'message_id': 'foobar'"}


def test_many_events(cli, settings, send_email, db):
    msg_ext_id = send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    db.execute_b(
        'insert into events (:values__names) values :values',
        values=MultipleValues(
            *[
                Values(
                    ts=(datetime(2032, 6, 1) + timedelta(days=i)).replace(tzinfo=timezone.utc),
                    message_id=message_id,
                    status=MessageStatus.send,
                    extra=json.dumps({'foo': 'bar', 'v': i}),
                )
                for i in range(55)
            ]
        ),
    )

    url = modify_url(f'/messages/email-test/message/{message_id}.html', settings, 'test-details')
    r = cli.get(url)
    assert r.status_code == 200, r.text
    text = r.text
    assert text.count('#morpheus-accordion') == 51
    assert 'Send &bull; <span class="datetime">2032-06-16T00:00:00+00</span>\n' in text, text
    assert '5 more &bull; ...' in text


def test_message_details_missing(cli, settings):
    r = cli.get(modify_url('/messages/email-test/message/123.html', settings, 'test-details'))
    assert r.status_code == 404, r.text
    assert {'message': 'message not found'} == r.json()


def test_message_preview(cli, settings, send_email, db):
    msg_ext_id = send_email(company_code='preview')
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status_code == 200, r.text
    assert '<body>\nthis is a test\n</body>' == r.text


def test_message_preview_disable_links(cli, send_email, settings, db):
    msg_ext_id = send_email(
        company_code='preview',
        context={
            'message__render': (
                'Hi, <a href="https://lp.example.com/">\n<span class="class">Look at '
                'this link that needs removed</span></a>'
            ),
            'unsubscribe_link': 'http://example.org/unsub',
        },
        recipients=[{'address': '1@example.org'}],
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status_code == 200, r.text
    msg = r.text
    assert '<p>Hi, <a href="#"><br>\n<span class="class">Look at this link that needs removed</span></a></p>' in msg


def test_message_preview_disable_links_md(send_email, settings, cli, db):
    msg_ext_id = send_email(
        company_code='preview',
        main_template='testing {{{ foobar }}}',
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'foobar__md': '[hello](www.example.org/hello)',
        },
    )
    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = cli.get(modify_url(f'/messages/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status_code == 200, r.text
    assert 'testing <p><a href="#">hello</a></p>\n' == r.text


def test_user_sms(cli, settings, send_sms, db):
    send_sms(company_code='snapcrap')

    send_sms(uid=str(uuid.uuid4()), company_code='flip')
    r = cli.get(modify_url('/messages/sms-test/', settings, 'snapcrap'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 1
    item = data['items'][0]
    assert item['method'] == 'sms-test'
    assert item['company_id'] == db.fetchval('select id from companies where code=$1', 'snapcrap')
    assert item['status'] == 'send'
    assert item['from_name'] == 'FooBar'
    assert item['cost'] == 0.012
    assert 'events' not in item
    assert json.loads(item['extra']) == {'length': 21, 'parts': 1}
    assert data['spend'] == 0.012

    r = cli.get(modify_url('/messages/sms-test/', settings, '__all__'))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data['count'] == 2


def test_user_sms_preview(cli, settings, send_sms, db):
    msg_ext_id = send_sms(company_code='smspreview', main_template='this is a test {{ variable }} ' * 10)

    message_id = db.fetchval('select id from messages where external_id=$1', msg_ext_id)
    send_sms(uid=str(uuid.uuid4()), company_code='flip')
    r = cli.get(modify_url(f'/messages/sms-test/{message_id}/preview/', settings, 'smspreview'))
    text = r.text
    assert r.status_code == 200, text
    assert '<span class="metadata">Length:</span>220' in text
    assert '<span class="metadata">Multipart:</span>2 parts' in text


def test_user_list_lots(cli, settings, send_email):
    for i in range(110):
        send_email(uid=str(uuid.uuid4()), company_code='list-lots', recipients=[{'address': f'{i}@t.com'}])

    r = cli.get(modify_url('/messages/email-test/messages.html', settings, '__all__'))
    assert r.status_code == 200, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    text = r.text
    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results >= 110
    assert '1 - 100' not in text
    assert f'101 - {min(results, 150)}' in text

    url = modify_url('/messages/email-test/messages.html', settings, '__all__')
    r = cli.get(url + '&from=100')
    assert r.status_code == 200, r.text
    text = r.text
    assert '1 - 100' in text
    assert f'101 - {min(results, 150)}' not in text


def test_valid_signature(cli, settings, db):
    db.execute('insert into companies (code) values ($1)', 'whatever')
    args = dict(company='whatever', expires=to_unix_ms(datetime(2032, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 200, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'


def test_invalid_signature(cli, settings):
    args = dict(company='whatever', expires=to_unix_ms(datetime(2032, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest() + 'xxx'
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 403, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {'message': 'Invalid token'} == r.json()


def test_invalid_expiry(cli, settings):
    args = dict(company='whatever', expires='xxx')
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 400, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {
        'message': 'Invalid Data',
        'details': [{'loc': ['expires'], 'msg': 'invalid datetime format', 'type': 'value_error.datetime'}],
    } == r.json()


def test_sig_expired(cli, settings):
    args = dict(company='whatever', expires=to_unix_ms(datetime(2000, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = cli.get('/messages/email-test/?' + urlencode(args))
    assert r.status_code == 403, r.text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {'message': 'token expired'} == r.json()
