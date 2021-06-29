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

from morpheus.app.schema import MessageStatus
from morpheus.app.worker import update_aggregation_view


def modify_url(url, settings, company='foobar'):
    args = dict(company=company, expires=to_unix_ms(datetime(2032, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    return str(url) + ('&' if '?' in str(url) else '?') + urlencode(args)


async def test_user_list(cli, settings, send_email, db_conn):
    expected_msg_ids = []
    for i in range(4):
        uid = str(uuid.uuid4())
        await send_email(uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}])
        expected_msg_ids.append(f'{uid}-{i}tcom')

    await send_email(uid=str(uuid.uuid4()), company_code='different1')
    await send_email(uid=str(uuid.uuid4()), company_code='different2')
    r = await cli.get(modify_url('/user/email-test/messages.json', settings, 'whoever'))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    data = await r.json()
    # debug(data)
    assert data['count'] == 4
    msg_ids = [h['external_id'] for h in data['items']]
    assert msg_ids == list(reversed(expected_msg_ids))
    first_item = data['items'][0]
    assert first_item == {
        'id': await db_conn.fetchval('select id from messages where external_id=$1', expected_msg_ids[3]),
        'external_id': expected_msg_ids[3],
        'send_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'update_ts': RegexStr(r'\d{4}-\d{2}-\d{2}.*'),
        'status': 'send',
        'to_first_name': None,
        'to_last_name': None,
        'to_user_link': None,
        'to_address': '3@t.com',
        'company_id': await db_conn.fetchval('select id from companies where code=$1', 'whoever'),
        'method': 'email-test',
        'subject': 'test message',
        'tags': [expected_msg_ids[3][:-6]],
        'from_name': 'Sender Name',
        'cost': None,
        'extra': None,
    }

    r = await cli.get(modify_url('/user/email-test/messages.json', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 6

    r = await cli.get(modify_url('/user/email-test/messages.html', settings, '__all__'))
    assert r.status == 200, await r.text()
    text = await r.text()
    assert '<caption>Results: <b>6</b></caption>' in text
    assert text.count('.com</a>') == 6


async def test_user_list_sms(cli, settings, send_sms, db_conn):
    await send_sms(company_code='testing')

    r = await cli.get(modify_url('/user/sms-test/messages.json', settings, 'testing'))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    data = await r.json()
    assert data['count'] == 1
    assert len(data['items']) == 1
    assert data['items'][0]['body'] == 'this is a test apples'


async def test_user_search(cli, settings, send_email):
    msgs = {}
    for i, subject in enumerate(['apple', 'banana', 'cherry', 'durian']):
        uid = str(uuid.uuid4())
        await send_email(
            uid=uid, company_code='whoever', recipients=[{'address': f'{i}@t.com'}], subject_template=subject
        )
        msgs[subject] = f'{uid}-{i}tcom'

    await send_email(uid=str(uuid.uuid4()), company_code='different1', subject_template='eggplant')

    r = await cli.get(modify_url('/user/email-test/messages.json?q=cherry', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 1
    item = data['items'][0]
    # debug(item)
    assert item['external_id'] == msgs['cherry']
    assert item['subject'] == 'cherry'
    r = await cli.get(modify_url('/user/email-test/messages.json?q=eggplant', settings, 'whoever'))
    assert r.status == 200, await r.text()
    data = await r.json()
    # debug(data)
    assert data['count'] == 0


async def test_user_search_space(cli, settings, send_email):
    uid = str(uuid.uuid4())
    await send_email(
        uid=uid, company_code='testing', recipients=[{'address': 'testing@example.com'}], subject_template='foobar'
    )

    r = await cli.get(modify_url('/user/email-test/messages.json?q=foobar', settings, 'testing'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 1

    r = await cli.get(modify_url('/user/email-test/messages.json?q=foo%20bar', settings, 'testing'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 0


async def test_user_list_lots_query_test(cli, settings, send_email):
    for i in range(110):
        await send_email(
            uid=str(uuid.uuid4()),
            company_code='testing',
            recipients=[{'address': f'{i}@t.com'}],
            subject_template='foobar',
        )

    for i in range(20):
        await send_email(
            uid=str(uuid.uuid4()),
            company_code='testing',
            recipients=[{'address': f'{i}@t.com'}],
            subject_template='barfoo',
        )

    r = await cli.get(modify_url('/user/email-test/messages.html', settings, 'testing'))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    text = await r.text()

    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results == 130
    assert '1 - 100' not in text
    assert f'101 - {min(results, 150)}' in text
    assert 'href="?from=100"' in text

    url = modify_url('/user/email-test/messages.html', settings, 'testing')
    r = await cli.get(url + '&q=foobar&from=100')
    assert r.status == 200, await r.text()
    text = await r.text()
    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results == 10
    assert '1 - 100' in text
    assert f'101 - {min(results, 150)}' not in text
    assert 'href="?q=foobar&amp;from=0"' in text


async def test_user_aggregate(cli, settings, send_email, db_conn):
    for i in range(4):
        await send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])
    msg_id = await send_email(uid=str(uuid.uuid4()), company_code='user-aggs', recipients=[{'address': f'{i}@t.com'}])

    data = {'ts': int(2e10), 'event': 'open', '_id': msg_id, 'user_agent': 'testincalls'}
    await cli.post('/webhook/test/', json=data)

    await send_email(uid=str(uuid.uuid4()), company_code='different')

    await update_aggregation_view({'pg': db_conn})

    r = await cli.get(modify_url('/user/email-test/aggregation.json', settings, 'user-aggs'))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    data = await r.json()
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

    r = await cli.get(modify_url('/user/email-test/aggregation.json', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert sum(v['count'] for v in data['histogram']) == 6


async def test_user_aggregate_no_data(cli, settings, db_conn):
    await db_conn.execute('insert into companies (code) values ($1)', 'testing')
    r = await cli.get(modify_url('/user/email-test/aggregation.json', settings, 'testing'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data == {
        'histogram': [],
        'all_90_day': 0,
        'open_90_day': 0,
        'all_7_day': 0,
        'open_7_day': 0,
        'all_28_day': 0,
        'open_28_day': 0,
    }


async def test_user_tags(cli, settings, send_email):
    uid1 = str(uuid.uuid4())
    await send_email(
        uid=uid1,
        company_code='tagtest',
        tags=['trigger:broadcast', 'broadcast:123'],
        recipients=[
            {'address': '1@t.com', 'tags': ['user:1', 'shoesize:10']},
            {'address': '2@t.com', 'tags': ['user:2', 'shoesize:8']},
        ],
    )
    uid2 = str(uuid.uuid4())
    await send_email(
        uid=uid2,
        company_code='tagtest',
        tags=['trigger:other'],
        recipients=[
            {'address': '3@t.com', 'tags': ['user:3', 'shoesize:10']},
            {'address': '4@t.com', 'tags': ['user:4', 'shoesize:8']},
        ],
    )

    await send_email(uid=str(uuid.uuid4()), company_code='different1')
    await send_email(uid=str(uuid.uuid4()), company_code='different2')

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'broadcast:123')])
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 2, json.dumps(data, indent=2)
    assert {h['external_id'] for h in data['items']} == {f'{uid1}-1tcom', f'{uid1}-2tcom'}

    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query([('tags', 'user:2')])
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 1, json.dumps(data, indent=2)
    assert data['items'][0]['external_id'] == f'{uid1}-2tcom'

    query = [('tags', 'trigger:other'), ('tags', 'shoesize:8')]
    url = cli.server.app.router['user-messages'].url_for(method='email-test').with_query(query)
    r = await cli.get(modify_url(url, settings, 'tagtest'))
    assert r.status == 200, await r.text()
    data = await r.json()
    # debug(data)
    assert data['count'] == 1
    assert data['items'][0]['external_id'] == f'{uid2}-4tcom'


async def test_message_details(cli, settings, send_email, db_conn, worker):
    msg_ext_id = await send_email(company_code='test-details')

    data = {'ts': int(1e10), 'event': 'open', '_id': msg_ext_id, 'user_agent': 'testincalls'}
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(modify_url(f'/user/email-test/message/{message_id}.html', settings, 'test-details'))
    text = await r.text()
    assert r.status == 200, text
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    spaceless = re.sub('\n +', '\n', text)
    assert '<label>Subject:</label>\n<span>test message</span>' in spaceless
    assert '<label>To:</label>\n<span>&lt;foobar@testing.com&gt;</span>' in spaceless

    assert 'Open &bull;' in text
    assert '"user_agent": "testincalls",' in text
    assert text.count('<span class="datetime">') == 3


async def test_message_details_link(cli, settings, send_email, db_conn, worker):
    msg_ext_id = await send_email(
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
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    url = modify_url(f'/user/email-test/message/{message_id}.html', settings, 'test-details')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    text = await r.text()
    assert '<span><a href="/whatever/123/">Foo Bar &lt;foobar@testing.com&gt;</a></span>' in text
    assert '<a href="/attachment-doc/123/">testing.pdf</a>' in text
    assert '<a href="#">different.pdf</a>' in text
    d = re.search('Open &bull; .+', text).group()
    assert 'Open &bull; <span class="datetime">2033-05-18T03:33:20+00</span>' == d, text
    assert 'extra values not shown' not in text

    r = await cli.get(url + '&' + urlencode({'dttz': 'Europe/London'}))
    assert r.status == 200, await r.text()
    text = await r.text()
    d = re.search('Open &bull; .+', text).group()
    assert 'Open &bull; <span class="datetime">2033-05-18T04:33:20+01</span>' == d, text

    r = await cli.get(url + '&' + urlencode({'dttz': 'snap'}))
    assert r.status == 400, await r.text()
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    assert {'message': 'unknown timezone: "snap"'} == await r.json()


async def test_no_event_data(cli, settings, send_email, db_conn):
    msg_ext_id = await send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    await db_conn.execute_b(
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
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(modify_url(f'/user/email-test/message/{message_id}.html', settings, 'test-details'))
    assert '<div class="events" id="morpheus-accordion">\n' in await r.text()


async def test_single_item_events(cli, settings, send_email, db_conn):
    msg_ext_id = await send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    await db_conn.execute_b(
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

    url = modify_url(f'/user/email-test/messages.json?message_id={message_id}', settings, 'test-details')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['events'] == [
        {'status': 'send', 'ts': '2032-06-01T00:00:00+00', 'extra': None},
        {'status': 'send', 'ts': '2032-06-02T02:00:00+00', 'extra': None},
        {'status': 'send', 'ts': '2032-06-03T04:00:00+00', 'extra': None},
    ]


async def test_invalid_message_id(cli, settings):
    url = modify_url('/user/email-test/messages.json?message_id=foobar', settings, 'test-details')
    r = await cli.get(url)
    assert r.status == 400, await r.text()
    data = await r.json()
    assert data == {'message': "invalid get argument 'message_id': 'foobar'"}


async def test_many_events(cli, settings, send_email, db_conn):
    msg_ext_id = await send_email(
        company_code='test-details', recipients=[{'first_name': 'Foo', 'address': 'foobar@testing.com'}]
    )
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    await db_conn.execute_b(
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

    url = modify_url(f'/user/email-test/message/{message_id}.html', settings, 'test-details')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    text = await r.text()
    assert text.count('#morpheus-accordion') == 51
    assert 'Send &bull; <span class="datetime">2032-06-16T00:00:00+00</span>\n' in text, text
    assert '5 more &bull; ...' in text


async def test_message_details_missing(cli, settings):
    r = await cli.get(modify_url('/user/email-test/message/123.html', settings, 'test-details'))
    assert r.status == 404, await r.text()
    assert {'message': 'message not found'} == await r.json()


async def test_message_preview(cli, settings, send_email, db_conn):
    msg_ext_id = await send_email(company_code='preview')
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(modify_url(f'/user/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status == 200, await r.text()
    assert '<body>\nthis is a test\n</body>' == await r.text()


async def test_message_preview_disable_links(cli, send_email, settings, db_conn):
    msg_ext_id = await send_email(
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
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(modify_url(f'/user/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status == 200, await r.text()
    msg = await r.text()
    assert '<p>Hi, <a href="#"><br>\n<span class="class">Look at this link that needs removed</span></a></p>' in msg


async def test_message_preview_disable_links_md(send_email, settings, cli, db_conn):
    msg_ext_id = await send_email(
        company_code='preview',
        main_template='testing {{{ foobar }}}',
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'foobar__md': '[hello](www.example.org/hello)',
        },
    )
    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(modify_url(f'/user/email-test/{message_id}/preview/', settings, 'preview'))
    assert r.status == 200, await r.text()
    assert 'testing <p><a href="#">hello</a></p>\n' == await r.text()


async def test_user_sms(cli, settings, send_sms, db_conn):
    await send_sms(company_code='snapcrap')

    await send_sms(uid=str(uuid.uuid4()), company_code='flip')
    r = await cli.get(modify_url('/user/sms-test/messages.json', settings, 'snapcrap'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 1
    item = data['items'][0]
    assert item['method'] == 'sms-test'
    assert item['company_id'] == await db_conn.fetchval('select id from companies where code=$1', 'snapcrap')
    assert item['status'] == 'send'
    assert item['from_name'] == 'FooBar'
    assert item['cost'] == 0.012
    assert 'events' not in item
    assert json.loads(item['extra']) == {'length': 21, 'parts': 1}
    assert data['spend'] == 0.012

    r = await cli.get(modify_url('/user/sms-test/messages.json', settings, '__all__'))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['count'] == 2

    r = await cli.get(modify_url('/user/sms-test/messages.html', settings, 'snapcrap'))
    assert r.status == 200, await r.text()
    text = await r.text()
    assert '<caption>Total spend this month: <b>£0.012</b><span id="extra-spend-info"></span></caption>' in text, text


async def test_user_sms_preview(cli, settings, send_sms, db_conn):
    msg_ext_id = await send_sms(company_code='smspreview', main_template='this is a test {{ variable }} ' * 10)

    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    await send_sms(uid=str(uuid.uuid4()), company_code='flip')
    r = await cli.get(modify_url(f'/user/sms-test/{message_id}/preview/', settings, 'smspreview'))
    text = await r.text()
    assert r.status == 200, text
    assert '<span class="metadata">Length:</span>220' in text
    assert '<span class="metadata">Multipart:</span>2 parts' in text


async def test_user_list_lots(cli, settings, send_email):
    for i in range(110):
        await send_email(uid=str(uuid.uuid4()), company_code='list-lots', recipients=[{'address': f'{i}@t.com'}])

    r = await cli.get(modify_url('/user/email-test/messages.html', settings, '__all__'))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    text = await r.text()
    m = re.search(r'<caption>Results: <b>(\d+)</b></caption>', text)
    results = int(m.groups()[0])
    assert results >= 110
    assert '1 - 100' not in text
    assert f'101 - {min(results, 150)}' in text

    url = modify_url('/user/email-test/messages.html', settings, '__all__')
    r = await cli.get(url + '&from=100')
    assert r.status == 200, await r.text()
    text = await r.text()
    assert '1 - 100' in text
    assert f'101 - {min(results, 150)}' not in text


async def test_valid_signature(cli, settings, db_conn):
    await db_conn.execute('insert into companies (code) values ($1)', 'whatever')
    args = dict(company='whatever', expires=to_unix_ms(datetime(2032, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = await cli.get('/user/email-test/messages.json?' + urlencode(args))
    assert r.status == 200, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'


async def test_invalid_signature(cli, settings):
    args = dict(company='whatever', expires=to_unix_ms(datetime(2032, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest() + 'xxx'
    r = await cli.get('/user/email-test/messages.json?' + urlencode(args))
    assert r.status == 403, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {'message': 'Invalid token'} == await r.json()


async def test_invalid_expiry(cli, settings):
    args = dict(company='whatever', expires='xxx')
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = await cli.get('/user/email-test/messages.json?' + urlencode(args))
    assert r.status == 400, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {
        'message': 'Invalid Data',
        'details': [{'loc': ['expires'], 'msg': 'invalid datetime format', 'type': 'value_error.datetime'}],
    } == await r.json()


async def test_sig_expired(cli, settings):
    args = dict(company='whatever', expires=to_unix_ms(datetime(2000, 1, 1)))
    body = '{company}:{expires}'.format(**args).encode()
    args['signature'] = hmac.new(settings.user_auth_key, body, hashlib.sha256).hexdigest()
    r = await cli.get('/user/email-test/messages.json?' + urlencode(args))
    assert r.status == 403, await r.text()
    assert r.headers['Access-Control-Allow-Origin'] == '*'
    assert {'message': 'token expired'} == await r.json()
