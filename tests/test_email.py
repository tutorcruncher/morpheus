import base64
import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from arq import Retry
from pytest_toolbox.comparison import AnyInt, RegexStr

from morpheus.app.ext import ApiError
from morpheus.app.main import create_app, get_mandrill_webhook_key
from morpheus.app.models import EmailRecipientModel
from morpheus.app.worker import email_retrying, send_email as worker_send_email


async def test_send_email(cli, worker, tmpdir):
    uuid = str(uuid4())
    data = {
        'uid': uuid,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {'message__render': '# hello\n\nThis is a **{{ b }}**.\n', 'a': 'Apple', 'b': f'Banana'},
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': f'bar',
                'user_link': '/user/profile/42/',
                'address': 'foobar@example.org',
                'tags': ['foobar'],
            }
        ],
    }
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 1
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(uuid + '-foobarexampleorg.txt').read()
    assert '\nsubject: test email Apple\n' in msg_file
    assert '\n<p>This is a <strong>Banana</strong>.</p>\n' in msg_file
    data = json.loads(re.search(r'data: ({.*?})\ncontent:', msg_file, re.S).groups()[0])
    assert data['from_email'] == 's@muelcolvin.com'
    assert data['to_address'] == 'foobar@example.org'
    assert data['to_user_link'] == '/user/profile/42/'
    assert data['attachments'] == []
    assert set(data['tags']) == {uuid, 'foobar'}


async def test_webhook(cli, send_email, db_conn, worker):
    uuid = str(uuid4())
    message_id = await send_email(uid=uuid)

    message = await db_conn.fetchrow('select * from messages where external_id=$1', message_id)
    assert message['status'] == 'send'
    first_update_ts = message['update_ts']

    events = await db_conn.fetchval('select count(*) from events')
    assert events == 0

    data = {'ts': int(2e9), 'event': 'open', '_id': message_id, 'foobar': ['hello', 'world']}
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    message = await db_conn.fetchrow('select * from messages where external_id=$1', message_id)
    assert message['status'] == 'open'
    assert message['update_ts'] > first_update_ts
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    events = [dict(e) for e in events]
    assert len(events) == 1
    assert events == [
        {
            'id': AnyInt(),
            'message_id': message['id'],
            'status': 'open',
            'ts': datetime(2033, 5, 18, 3, 33, 20, tzinfo=timezone.utc),
            'extra': RegexStr('{.*}'),
        }
    ]
    extra = json.loads(events[0]['extra'])
    assert extra['diag'] is None
    assert extra['opens'] is None


async def test_webhook_old(cli, send_email, db_conn, worker):
    msg_id = await send_email()
    message = await db_conn.fetchrow('select * from messages where external_id=$1', msg_id)
    assert message['status'] == 'send'
    first_update_ts = message['update_ts']
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    assert len(events) == 0
    data = {'ts': int(1.4e9), 'event': 'open', '_id': msg_id}
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    message = await db_conn.fetchrow('select * from messages where external_id=$1', msg_id)
    assert message['status'] == 'send'
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    assert len(events) == 1
    assert message['update_ts'] == first_update_ts


async def test_webhook_repeat(cli, send_email, db_conn, worker):
    msg_id = await send_email()
    message = await db_conn.fetchrow('select * from messages where external_id=$1', msg_id)
    assert message['status'] == 'send'
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    assert len(events) == 0
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id}
    for _ in range(3):
        r = await cli.post('/webhook/test/', json=data)
        assert r.status == 200, await r.text()
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id, 'user_agent': 'xx'}
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 5

    message = await db_conn.fetchrow('select * from messages where external_id=$1', msg_id)
    assert message['status'] == 'open'
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    assert len(events) == 2


async def test_webhook_missing(cli, send_email, db_conn):
    msg_id = await send_email()

    data = {'ts': int(1e10), 'event': 'open', '_id': 'missing', 'foobar': ['hello', 'world']}
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    message = await db_conn.fetchrow('select * from messages where external_id=$1', msg_id)
    assert message['status'] == 'send'
    events = await db_conn.fetch('select * from events where message_id=$1', message['id'])
    assert len(events) == 0


async def test_mandrill_send(send_email, db_conn, dummy_server):
    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobaratestingcom')
    assert m is None
    await send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@testing.com'}])

    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobaratestingcom')
    assert m is not None
    assert m['to_address'] == 'foobar_a@testing.com'
    assert dummy_server.app['log'] == ['POST /mandrill/messages/send.json > 200']


async def test_send_mandrill_with_other_attachment(send_email, db_conn):
    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobarctestingcom')
    assert m is None
    await send_email(
        method='email-mandrill',
        recipients=[
            {
                'address': 'foobar_c@testing.com',
                'attachments': [
                    {'name': 'calendar.ics', 'content': 'Look this is some test data', 'mime_type': 'text/calendar'}
                ],
            }
        ],
    )
    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobarctestingcom')
    assert m['to_address'] == 'foobar_c@testing.com'
    assert set(m['attachments']) == {'::calendar.ics'}


async def test_example_email_address(send_email, db_conn):
    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobaraexamplecom')
    assert m is None
    await send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@example.com'}])

    m = await db_conn.fetchrow('select * from messages where external_id=$1', 'mandrill-foobaraexamplecom')
    assert m['to_address'] == 'foobar_a@example.com'
    assert m['status'] == 'send'


async def test_mandrill_webhook(cli, send_email, db_conn, worker):
    await send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    assert 1 == await db_conn.fetchval('select count(*) from messages')

    events = await db_conn.fetch('select * from events')
    assert len(events) == 0

    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'mandrill-testingexampleorg', 'foobar': ['hello', 'world']}]
    data = {'mandrill_events': json.dumps(messages)}

    sig = base64.b64encode(
        hmac.new(
            b'testing',
            msg=(
                b'https://None/webhook/mandrill/mandrill_events[{"ts": 1969660800, '
                b'"event": "open", "_id": "mandrill-testingexampleorg", "foobar": ["hello", "world"]}]'
            ),
            digestmod=hashlib.sha1,
        ).digest()
    )
    r = await cli.post('/webhook/mandrill/', data=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status == 200, await r.text()
    assert await worker.run_check() == 2

    events = await db_conn.fetch('select * from events')
    assert len(events) == 1

    assert events[0]['ts'] == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)
    assert events[0]['status'] == 'open'


async def test_mandrill_webhook_invalid(cli, send_email, db_conn):
    await send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'e587306</div></body><meta name=', 'foobar': ['x']}]
    data = {'mandrill_events': json.dumps(messages)}

    sig = base64.b64encode(
        hmac.new(
            b'testing',
            msg=(
                b'https://None/webhook/mandrill/mandrill_events[{"ts": 1969660800, '
                b'"event": "open", "_id": "e587306</div></body><meta name=", "foobar": ["x"]}]'
            ),
            digestmod=hashlib.sha1,
        ).digest()
    )
    r = await cli.post('/webhook/mandrill/', data=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status == 200, await r.text()

    events = await db_conn.fetch('select * from events')
    assert len(events) == 0


async def test_mandrill_send_bad_template(cli, send_email, db_conn):
    assert 0 == await db_conn.fetchval('select count(*) from messages')
    await send_email(
        method='email-mandrill', main_template='{{ foo } test message', recipients=[{'address': 'foobar_b@testing.com'}]
    )

    assert 1 == await db_conn.fetchval('select count(*) from messages')
    assert 'render_failed' == await db_conn.fetchval('select status from messages')


async def test_send_email_headers(cli, tmpdir, worker):
    uid = str(uuid4())
    data = {
        'uid': uid,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {'message__render': 'test email {{ a }} {{ b}} {{ c }}.\n', 'a': 'Apple', 'b': f'Banana'},
        'headers': {'Reply-To': 'another@whoever.com', 'List-Unsubscribe': '<http://example.org/unsub>'},
        'recipients': [
            {'first_name': 'foo', 'last_name': f'bar', 'address': f'foobar@example.org', 'context': {'c': 'Carrot'}},
            {
                'address': f'2@example.org',
                'context': {'b': 'Banker'},
                'headers': {'List-Unsubscribe': '<http://example.org/different>'},
            },
        ],
    }
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert await worker.run_check() == 2

    assert len(tmpdir.listdir()) == 2
    msg_file = tmpdir.join(f'{uid}-foobarexampleorg.txt').read()
    assert '<p>test email Apple Banana Carrot.</p>\n' in msg_file
    assert '"to_address": "foobar@example.org",\n' in msg_file
    assert '"Reply-To": "another@whoever.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/unsub>"\n' in msg_file

    msg_file = tmpdir.join(f'{uid}-2exampleorg.txt').read()
    assert '<p>test email Apple Banker .</p>\n' in msg_file
    assert '"to_address": "2@example.org",\n' in msg_file
    assert '"Reply-To": "another@whoever.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/different>"\n' in msg_file


async def test_send_unsub_context(send_email, tmpdir):
    uid = str(uuid4())
    await send_email(
        uid=uid,
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'unsubscribe_link': 'http://example.org/unsub',
        },
        recipients=[
            {'address': f'1@example.org'},
            {
                'address': f'2@example.org',
                'context': {'unsubscribe_link': 'http://example.org/context'},
                'headers': {'List-Unsubscribe': '<http://example.org/different>'},
            },
        ],
    )
    assert len(tmpdir.listdir()) == 2
    msg_file = tmpdir.join(f'{uid}-1exampleorg.txt').read()
    # print(msg_file)
    assert '"to_address": "1@example.org",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/unsub>"\n' in msg_file
    assert '<p>test email http://example.org/unsub.</p>\n' in msg_file

    msg_file = tmpdir.join(f'{uid}-2exampleorg.txt').read()
    print(msg_file)
    assert '"to_address": "2@example.org",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/different>"\n' in msg_file
    assert '<p>test email http://example.org/context.</p>\n' in msg_file


async def test_markdown_context(send_email, tmpdir):
    message_id = await send_email(
        main_template='testing {{{ foobar }}}',
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'foobar__md': '[hello](www.example.org/hello)',
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert 'content:\ntesting <p><a href="www.example.org/hello">hello</a></p>\n' in msg_file


async def test_partials(send_email, tmpdir):
    message_id = await send_email(
        main_template=('message: |{{{ message }}}|\n' 'foo: {{ foo }}\n' 'partial: {{> test_p }}'),
        context={'message__render': '{{foo}} {{> test_p }}', 'foo': 'FOO', 'bar': 'BAR'},
        mustache_partials={'test_p': 'foo ({{ foo }}) bar **{{ bar }}**'},
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert (
        """
content:
message: |<p>FOO foo (FOO) bar <strong>BAR</strong></p>
|
foo: FOO
partial: foo (FOO) bar **BAR**
"""
        in msg_file
    )


async def test_macros(send_email, tmpdir):
    message_id = await send_email(
        main_template='macro result: foobar(hello | {{ foo }})',
        context={'foo': 'FOO', 'bar': 'BAR'},
        macros={'foobar(a | b)': '___{{ a }} {{b}}___'},
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert 'content:\nmacro result: ___hello FOO___\n' in msg_file


async def test_macros_more(send_email, tmpdir):
    message_id = await send_email(
        main_template=(
            'foo:foo()\n'
            'foo wrong:foo(1 | 2)\n'
            'bar:bar()\n'
            'spam1:spam(x | y )\n'
            'spam2:spam(with bracket )  | {{ bar}} )\n'
            'spam3:spam({{ foo }} | {{ bar}} )\n'
            'spam wrong:spam(1 | {{ bar}} | x)\n'
            'button:centered_button(Reset password now | {{ password_reset_link }})\n'
        ),
        context={
            'foo': 'FOO',
            'bar': 'BAR',
            'password_reset_link': '/testagency/password/reset/t-4mx-2968ca2f34bc512e70e6/',
        },
        macros={
            'foo()': '___is foo___',
            'bar': '___is bar___',
            'spam(apple | pear)': '___spam {{apple}} {{pear}}___',
            'centered_button(text | link)': """
      <div class="button">
        <a href="{{ link }}"><span>{{ text }}</span></a>
      </div>\n""",
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert (
        """
content:
foo:___is foo___
foo wrong:foo(1 | 2)
bar:bar()
spam1:___spam x y___
spam2:spam(with bracket )  | BAR )
spam3:___spam FOO BAR___
spam wrong:spam(1 | BAR | x)
button:
      <div class="button">
        <a href="/testagency/password/reset/t-4mx-2968ca2f34bc512e70e6/"><span>Reset password now</span></a>
      </div>
"""
        in msg_file
    )


async def test_macro_in_message(send_email, tmpdir):
    message_id = await send_email(
        context={
            'pay_link': '/pay/now/123/',
            'first_name': 'John',
            'message__render': ('# hello {{ first_name }}\n' 'centered_button(Pay now | {{ pay_link }})\n'),
        },
        macros={
            'centered_button(text | link)': (
                '<div class="button">\n' '  <a href="{{ link }}"><span>{{ text }}</span></a>\n' '</div>\n'
            )
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert (
        """
content:
<body>
<h1>hello John</h1>

<div class="button">
  <a href="/pay/now/123/"><span>Pay now</span></a>
</div>

</body>
"""
        in msg_file
    )


async def test_send_md_options(send_email, tmpdir):
    message_id = await send_email(context={'message__render': 'we are_testing_emphasis **bold**\nnewline'})
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '<p>we are_testing_emphasis <strong>bold</strong><br>\nnewline</p>' in msg_file


async def test_standard_sass(cli, tmpdir, worker):
    data = dict(
        uid=str(uuid4()),
        company_code='foobar',
        from_address='Sender Name <sender@example.org>',
        method='email-test',
        subject_template='test message',
        context={'message': 'this is a test'},
        recipients=[{'address': 'foobar@testing.com'}],
    )
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201
    assert await worker.run_check() == 1
    message_id = data['uid'] + '-foobartestingcom'

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '<style>#body{-webkit-font-smoothing' in msg_file


async def test_custom_sass(send_email, tmpdir):
    message_id = await send_email(
        main_template='{{{ css }}}',
        context={'css__sass': ('.foo {\n' '  .bar {\n' '    color: black;\n' '    width: (60px / 6);\n' '  }\n' '}')},
    )

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '.foo .bar{color:black;width:10px}' in msg_file
    assert '#body{-webkit-font-smoothing' not in msg_file


async def test_invalid_mustache_subject(send_email, tmpdir, db_conn):
    message_id = await send_email(
        subject_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_subject'
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\nsubject: {{ foo } test message\n' in msg_file

    messages = await db_conn.fetch(
        'select * from messages m join message_groups j on m.group_id=j.id where j.company=$1',
        'test_invalid_mustache_subject',
    )
    assert len(messages) == 1
    m = messages[0]
    # debug(dict(m))
    assert m['status'] == 'send'
    assert m['subject'] == '{{ foo } test message'
    assert m['body'] == '<body>\n\n</body>'


async def test_invalid_mustache_body(send_email, db_conn):
    await send_email(
        main_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_body'
    )

    messages = await db_conn.fetch(
        'select * from messages m join message_groups j on m.group_id=j.id where j.company=$1',
        'test_invalid_mustache_body',
    )
    assert len(messages) == 1
    m = messages[0]
    # debug(dict(m))
    assert m['status'] == 'render_failed'
    assert m['subject'] is None
    assert m['body'] == 'Error rendering email: unclosed tag at line 1'


async def test_send_with_pdf(send_email, tmpdir, db_conn):
    message_id = await send_email(
        recipients=[
            {
                'address': 'foobar@testing.com',
                'pdf_attachments': [
                    {'name': 'testing.pdf', 'html': '<h1>testing</h1>', 'id': 123},
                    {'name': 'different.pdf', 'html': '<h1>different</h1>'},
                ],
            }
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '<h1>testing</h1>"' in msg_file

    attachments = await db_conn.fetchval('select attachments from messages where external_id=$1', message_id)
    assert set(attachments) == {'123::testing.pdf', '::different.pdf'}


async def test_send_with_other_attachment(send_email, tmpdir, db_conn):
    message_id = await send_email(
        recipients=[
            {
                'address': 'foobar@testing.com',
                'attachments': [
                    {'name': 'calendar.ics', 'content': 'Look this is some test data', 'mime_type': 'text/calendar'}
                ],
            }
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert 'Look this is some test data' in msg_file
    attachments = await db_conn.fetchval('select attachments from messages where external_id=$1', message_id)
    assert set(attachments) == {'::calendar.ics'}


async def test_pdf_not_unicode(send_email, tmpdir, cli):
    message_id = await send_email(
        recipients=[
            {'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': '<h1>binary</h1>'}]}
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '"testing.pdf:binary-"' in msg_file


async def test_pdf_empty(send_email, tmpdir):
    message_id = await send_email(
        recipients=[{'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': ''}]}]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '\n  "attachments": []\n' in msg_file


async def test_mandrill_send_client_error(db_conn, worker_ctx, call_send_emails):
    group_id, m = await call_send_emails(subject_template='__slow__')

    assert 0 == await db_conn.fetchval('select count(*) from messages')
    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 5_000

    assert 0 == await db_conn.fetchval('select count(*) from messages')


async def test_mandrill_send_many_errors(db_conn, worker_ctx, call_send_emails):
    group_id, m = await call_send_emails()

    assert 0 == await db_conn.fetchval('select count(*) from messages')
    worker_ctx['job_try'] = 10

    await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)

    assert 1 == await db_conn.fetchval('select count(*) from messages')

    m = await db_conn.fetchrow('select * from messages')
    assert m['status'] == 'send_request_failed'
    assert m['body'] == 'upstream error'


async def test_mandrill_send_502(db_conn, call_send_emails, worker_ctx):
    group_id, m = await call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 5_000

    assert 0 == await db_conn.fetchval('select count(*) from messages')


async def test_mandrill_send_502_last(db_conn, call_send_emails, worker_ctx):
    group_id, m = await call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = len(email_retrying)

    with pytest.raises(Retry) as exc_info:
        await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 43_200_000

    assert 0 == await db_conn.fetchval('select count(*) from messages')


async def test_mandrill_send_500_nginx(db_conn, call_send_emails, worker_ctx):
    group_id, m = await call_send_emails(subject_template='__500_nginx__')

    worker_ctx['job_try'] = 2

    with pytest.raises(Retry) as exc_info:
        await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 10_000

    assert 0 == await db_conn.fetchval('select count(*) from messages')


async def test_mandrill_send_500_not_nginx(db_conn, call_send_emails, worker_ctx):
    group_id, m = await call_send_emails(subject_template='__500__')

    worker_ctx['job_try'] = 1

    with pytest.raises(ApiError) as exc_info:
        await worker_send_email(worker_ctx, group_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.status == 500

    assert 0 == await db_conn.fetchval('select count(*) from messages')


async def send_with_link(send_email, tmpdir):
    mid = await send_email(
        main_template='<a href="{{ the_link }}">foobar</a> test message',
        context={'the_link': 'https://www.foobar.com'},
        company_code='test_link_shortening',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<a href="https://click.example.com/l(.+?)\?u=(.+?)">foobar</a> test message', msg_file)
    assert m, msg_file
    token, enc_url = m.groups()
    assert len(token) == 30
    assert base64.urlsafe_b64decode(enc_url).decode() == 'https://www.foobar.com'
    return token


async def test_link_shortening(send_email, tmpdir, cli, db_conn, worker):
    token = await send_with_link(send_email, tmpdir)

    assert 1 == await db_conn.fetchval('select count(*) from messages')
    m = await db_conn.fetchrow('select * from messages')
    assert m['status'] == 'send'

    assert 1 == await db_conn.fetchval('select count(*) from links')
    link = await db_conn.fetchrow('select * from links')
    assert dict(link) == {'id': AnyInt(), 'message_id': m['id'], 'token': token, 'url': 'https://www.foobar.com'}

    r = await cli.get(
        '/l' + token,
        allow_redirects=False,
        headers={
            'X-Forwarded-For': '54.170.228.0, 141.101.88.55',
            'X-Request-Start': '1969660800',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/59.0.3071.115 Safari/537.36',
        },
    )
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'https://www.foobar.com'
    assert await worker.run_check() == 2

    m_status = await db_conn.fetchval('select status from messages where id=$1', m['id'])
    assert m_status == 'click'
    assert 1 == await db_conn.fetchval('select count(*) from events')
    event = await db_conn.fetchrow('select * from events')
    assert event['status'] == 'click'
    assert event['ts'] == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)
    extra = json.loads(event['extra'])
    assert extra == {
        'ip': '54.170.228.0',
        'target': 'https://www.foobar.com',
        'user_agent': (
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/59.0.3071.115 Safari/537.36'
        ),
        'user_agent_display': 'Chrome 59 on Linux',
    }


async def test_link_shortening_wrong_url(send_email, tmpdir, cli):
    token = await send_with_link(send_email, tmpdir)
    # check we use the right url with a valid token but a different url arg
    r = await cli.get('/l' + token + '?u=' + base64.urlsafe_b64encode(b'different').decode(), allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'https://www.foobar.com'


async def test_link_shortening_wrong_url_missing(send_email, tmpdir, cli):
    token = await send_with_link(send_email, tmpdir)
    r = await cli.get('/lx' + token + '?u=' + base64.urlsafe_b64encode(b'different').decode(), allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'different'


async def test_link_shortening_repeat(send_email, tmpdir, cli, db_conn, worker):
    token = await send_with_link(send_email, tmpdir)
    r = await cli.get('/l' + token, allow_redirects=False)
    assert r.status == 307, await r.text()
    assert await worker.run_check() == 2
    assert r.headers['location'] == 'https://www.foobar.com'
    assert 1 == await db_conn.fetchval('select count(*) from events')

    r = await cli.get('/l' + token, allow_redirects=False)
    assert r.status == 307, await r.text()
    assert r.headers['location'] == 'https://www.foobar.com'
    assert 1 == await db_conn.fetchval('select count(*) from events')


async def test_link_shortening_in_render(send_email, tmpdir, db_conn):
    mid = await send_email(
        context={'message__render': 'test email {{ xyz }}\n', 'xyz': 'http://example.com/foobar'},
        company_code='test_link_shortening_in_render',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<p>test email https://click.example.com/l(.+?)\?u=(.+?)</p>', msg_file)
    assert m, msg_file
    token, enc_url = m.groups()

    assert 1 == await db_conn.fetchval('select count(*) from links')
    link = await db_conn.fetchrow('select * from links')
    assert link['url'] == 'http://example.com/foobar'
    assert link['token'] == token
    assert base64.urlsafe_b64decode(enc_url).decode() == 'http://example.com/foobar'


async def test_link_shortening_keep_long_link(send_email, tmpdir, cli):
    mid = await send_email(
        context={'message__render': 'test email {{ xyz_original }}\n', 'xyz': 'http://example.org/foobar'},
        company_code='test_link_shortening_in_render',
    )
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<p>test email http://example.org/foobar</p>', msg_file)
    assert m, msg_file


async def test_link_shortening_not_image(send_email, tmpdir, cli):
    mid = await send_email(
        context={
            'message__render': '{{ foo }} {{ bar}}',
            'foo': 'http://example.com/foobar',
            'bar': 'http://whatever.com/img.jpg',
        },
        company_code='test_link_shortening_in_render',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{mid}.txt').read()
    assert re.search(r'<p>https://click.example.com/l(\S+) http://whatever\.com/img\.jpg</p>', msg_file), msg_file


async def test_mandrill_key_not_setup(settings):
    app = create_app(settings)
    try:
        assert app['webhook_auth_key'] is None
        await get_mandrill_webhook_key(app)
        assert app['webhook_auth_key'] is None
    finally:
        await app['mandrill'].close()
        await app['morpheus_api'].close()


async def test_mandrill_key_existing(settings):
    settings.host_name = 'example.com'
    app = create_app(settings)
    try:
        assert app['webhook_auth_key'] is None
        await get_mandrill_webhook_key(app)
        assert app['webhook_auth_key'] == b'existing-auth-key'
    finally:
        await app['mandrill'].close()
        await app['morpheus_api'].close()


async def test_mandrill_key_new(settings):
    settings.host_name = 'different.com'
    app = create_app(settings)
    app['server_up_wait'] = 0
    try:
        assert app['webhook_auth_key'] is None
        await get_mandrill_webhook_key(app)
        assert app['webhook_auth_key'] == b'new-auth-key'
    finally:
        await app['mandrill'].close()
        await app['morpheus_api'].close()


async def test_mandrill_key_fail(settings):
    settings.host_name = 'fail.com'
    app = create_app(settings)
    app['server_up_wait'] = 0
    try:
        assert app['webhook_auth_key'] is None
        with pytest.raises(ApiError):
            await get_mandrill_webhook_key(app)
        assert app['webhook_auth_key'] is None
    finally:
        await app['mandrill'].close()
        await app['morpheus_api'].close()


async def test_not_json(cli, tmpdir):
    r = await cli.post('/send/email/', data='xxx', headers={'Authorization': 'testing-key'})
    assert r.status == 400, await r.text()
    assert {'message': 'Error decoding JSON'} == await r.json()


async def test_invalid_json(cli, tmpdir):
    data = {
        'uid': 'xxx',
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {},
        'recipients': [{'address': 'foobar_a@testing.com'}],
    }
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 400, await r.text()
    assert {
        'message': 'Invalid Data',
        'details': [{'loc': ['uid'], 'msg': 'value is not a valid uuid', 'type': 'type_error.uuid'}],
    } == await r.json()
