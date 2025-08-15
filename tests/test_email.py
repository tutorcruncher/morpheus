import base64
import hashlib
import hmac
import json
import logging
import pytest
import re
from arq import Retry
from buildpg import V
from datetime import datetime, timedelta, timezone
from foxglove.db.helpers import SyncDb
from pathlib import Path
from pytest_toolbox.comparison import AnyInt, RegexStr
from starlette.testclient import TestClient
from unittest.mock import Mock, patch
from uuid import uuid4

from src.schemas.messages import EmailRecipientModel, EmailSendModel, MessageStatus
from src.spam.services import OpenAISpamEmailService, SpamCacheService, SpamCheckResult
from src.views.email import get_spam_checker
from src.worker import delete_old_emails, email_retrying, send_email as worker_send_email

THIS_DIR = Path(__file__).parent.resolve()


def test_send_email(cli: TestClient, worker, tmpdir, loop):
    uuid = str(uuid4())
    data = {
        'uid': uuid,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {'message__render': '# hello\n\nThis is a **{{ b }}**.\n', 'a': 'Apple', 'b': 'Banana'},
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': 'bar',
                'user_link': '/user/profile/42/',
                'address': 'foobar@example.org',
                'tags': ['foobar'],
            }
        ],
    }
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 1
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


def test_webhook(cli: TestClient, send_email, sync_db: SyncDb, worker, loop):
    uuid = str(uuid4())
    message_id = send_email(uid=uuid)

    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == message_id)
    assert message['status'] == 'send'
    first_update_ts = message['update_ts']

    events = sync_db.fetchval('select count(*) from events')
    assert events == 0

    data = {'ts': int(2e9), 'event': 'open', '_id': message_id, 'foobar': ['hello', 'world']}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert worker.test_run() == 2

    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == message_id)
    assert message['status'] == 'open'
    assert message['update_ts'] > first_update_ts
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 1
    event = sync_db.fetchrow_b('select * from events where :where', where=V('message_id') == message['id'])
    assert event['ts'] == datetime(2033, 5, 18, 3, 33, 20, tzinfo=timezone.utc)
    assert event['extra'] == RegexStr('{.*}')
    extra = json.loads(event['extra'])
    assert extra['diag'] is None
    assert extra['opens'] is None


def test_webhook_old(cli: TestClient, send_email, sync_db: SyncDb, worker, loop):
    msg_id = send_email()
    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == msg_id)
    assert message['status'] == 'send'
    first_update_ts = message['update_ts']
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 0
    data = {'ts': int(1.4e9), 'event': 'open', '_id': msg_id}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert worker.test_run() == 2

    assert message['status'] == 'send'
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 1
    assert message['update_ts'] == first_update_ts


def test_webhook_repeat(cli: TestClient, send_email, sync_db: SyncDb, worker, loop):
    msg_id = send_email()
    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == msg_id)
    assert message['status'] == 'send'
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 0
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id}
    for _ in range(3):
        r = cli.post('/webhook/test/', json=data)
        assert r.status_code == 200, r.text
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id, 'user_agent': 'xx'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert worker.test_run() == 5

    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == msg_id)
    assert message['status'] == 'open'
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 2


def test_webhook_missing(cli: TestClient, send_email, sync_db: SyncDb):
    msg_id = send_email()

    data = {'ts': int(1e10), 'event': 'open', '_id': 'missing', 'foobar': ['hello', 'world']}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    message = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == msg_id)
    assert message['status'] == 'send'
    assert sync_db.fetchval_b('select count(*) from events where :where', where=V('message_id') == message['id']) == 0


def test_mandrill_send(send_email, sync_db: SyncDb, dummy_server):
    assert sync_db.fetchval('select count(*) from messages') == 0
    send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@testing.com'}])

    m = sync_db.fetchrow_b(
        'select * from messages where :where', where=V('external_id') == 'mandrill-foobaratestingcom'
    )
    assert m['to_address'] == 'foobar_a@testing.com'
    assert dummy_server.app['log'] == ['POST /mandrill/messages/send.json > 200']


def test_send_mandrill_with_other_attachments(send_email, sync_db: SyncDb, dummy_server):
    with open(THIS_DIR / 'attachments/testing.pdf', 'rb') as f:
        content = f.read()
    sent_content = base64.b64encode(content).decode()
    assert sync_db.fetchval('select count(*) from messages') == 0
    send_email(
        method='email-mandrill',
        recipients=[
            {
                'address': 'foobar_c@testing.com',
                'attachments': [
                    {'name': 'calendar.ics', 'content': 'Look this is some test data', 'mime_type': 'text/calendar'},
                    {'name': 'testing.pdf', 'content': sent_content, 'mime_type': 'application/pdf'},
                ],
            }
        ],
    )
    m = sync_db.fetchrow_b(
        'select * from messages where :where', where=V('external_id') == 'mandrill-foobarctestingcom'
    )
    assert m['to_address'] == 'foobar_c@testing.com'
    assert set(m['attachments']) == {'::calendar.ics', '::testing.pdf'}


def test_example_email_address(send_email, sync_db: SyncDb, dummy_server):
    assert sync_db.fetchval('select count(*) from messages') == 0
    send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@example.com'}])

    m = sync_db.fetchrow_b(
        'select * from messages where :where', where=V('external_id') == 'mandrill-foobaraexamplecom'
    )
    assert m['to_address'] == 'foobar_a@example.com'
    assert m['status'] == 'send'


def test_mandrill_webhook(cli: TestClient, send_email, sync_db: SyncDb, worker, loop, dummy_server, settings):
    send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    assert sync_db.fetchval('select count(*) from messages') == 1

    assert sync_db.fetchval('select count(*) from events') == 0

    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'mandrill-testingexampleorg', 'foobar': ['hello', 'world']}]

    msg = f'https://localhost/webhook/mandrill/mandrill_events{json.dumps(messages)}'
    sig = base64.b64encode(
        hmac.new(settings.mandrill_webhook_key.encode(), msg=msg.encode(), digestmod=hashlib.sha1).digest()
    )
    r = cli.post(
        '/webhook/mandrill/',
        data={'mandrill_events': json.dumps(messages)},
        headers={'X-Mandrill-Signature': sig.decode()},
    )
    assert r.status_code == 200, r.json()
    assert worker.test_run() == 2

    assert sync_db.fetchval('select count(*) from events') == 1

    events = sync_db.fetch('select * from events')
    assert events[0]['ts'] == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)
    assert events[0]['status'] == 'open'


def test_mandrill_webhook_invalid(cli: TestClient, send_email, sync_db: SyncDb, dummy_server, settings):
    send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'e587306</div></body><meta name=', 'foobar': ['x']}]
    data = {'mandrill_events': messages}

    msg = (
        'https://localhost/webhook/mandrill/mandrill_events[{"ts": 1969660800, "event": "open", '
        '"_id": "e587306</div></body><meta name=", "foobar": ["x"]}]'
    )
    sig = base64.b64encode(
        hmac.new(settings.mandrill_webhook_key.encode(), msg=msg.encode(), digestmod=hashlib.sha1).digest()
    )
    r = cli.post('/webhook/mandrill/', data=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status_code == 400, r.text

    assert sync_db.fetchval('select count(*) from events') == 0


def test_mandrill_send_bad_template(cli: TestClient, send_email, sync_db: SyncDb, dummy_server):
    assert sync_db.fetchval('select count(*) from messages') == 0
    send_email(
        method='email-mandrill', main_template='{{ foo } test message', recipients=[{'address': 'foobar_b@testing.com'}]
    )
    message = sync_db.fetchrow_b('select * from messages')
    assert message['status'] == 'render_failed'


def test_send_email_headers(cli: TestClient, tmpdir, worker, loop, dummy_server):
    uid = str(uuid4())
    data = {
        'uid': uid,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {'message__render': 'test email {{ a }} {{ b}} {{ c }}.\n', 'a': 'Apple', 'b': 'Banana'},
        'headers': {'Reply-To': 'another@whoever.com', 'List-Unsubscribe': '<http://example.org/unsub>'},
        'recipients': [
            {'first_name': 'foo', 'last_name': 'bar', 'address': 'foobar@example.org', 'context': {'c': 'Carrot'}},
            {
                'address': '2@example.org',
                'context': {'b': 'Banker'},
                'headers': {'List-Unsubscribe': '<http://example.org/different>'},
            },
        ],
    }
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == 2

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


def test_send_unsub_context(send_email, tmpdir):
    uid = str(uuid4())
    send_email(
        uid=uid,
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'unsubscribe_link': 'http://example.org/unsub',
        },
        recipients=[
            {'address': '1@example.org'},
            {
                'address': '2@example.org',
                'context': {'unsubscribe_link': 'http://example.org/context'},
                'headers': {'List-Unsubscribe': '<http://example.org/different>'},
            },
        ],
    )
    assert len(tmpdir.listdir()) == 2
    msg_file = tmpdir.join(f'{uid}-1exampleorg.txt').read()
    assert '"to_address": "1@example.org",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/unsub>"\n' in msg_file
    assert '<p>test email http://example.org/unsub.</p>\n' in msg_file

    msg_file = tmpdir.join(f'{uid}-2exampleorg.txt').read()
    assert '"to_address": "2@example.org",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.org/different>"\n' in msg_file
    assert '<p>test email http://example.org/context.</p>\n' in msg_file


def test_markdown_context(send_email, tmpdir):
    message_id = send_email(
        main_template='testing {{{ foobar }}}',
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'foobar__md': '[hello](www.example.org/hello)',
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert 'content:\ntesting <p><a href="www.example.org/hello">hello</a></p>\n' in msg_file


def test_partials(send_email, tmpdir):
    message_id = send_email(
        main_template='message: |{{{ message }}}|\n' 'foo: {{ foo }}\n' 'partial: {{> test_p }}',
        context={'message__render': '{{foo}} {{> test_p }}', 'foo': 'FOO', 'bar': 'BAR'},
        mustache_partials={'test_p': 'foo ({{ foo }}) bar **{{ bar }}**'},
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
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


def test_macros(send_email, tmpdir):
    message_id = send_email(
        main_template='macro result: foobar(hello | {{ foo }})',
        context={'foo': 'FOO', 'bar': 'BAR'},
        macros={'foobar(a | b)': '___{{ a }} {{b}}___'},
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert 'content:\nmacro result: ___hello FOO___\n' in msg_file


def test_macros_more(send_email, tmpdir):
    message_id = send_email(
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


def test_macro_in_message(send_email, tmpdir):
    message_id = send_email(
        context={
            'pay_link': '/pay/now/123/',
            'first_name': 'John',
            'message__render': '# hello {{ first_name }}\n' 'centered_button(Pay now | {{ pay_link }})\n',
        },
        macros={
            'centered_button(text | link)': (
                '<div class="button">\n' '  <a href="{{ link }}"><span>{{ text }}</span></a>\n' '</div>\n'
            )
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
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


def test_send_md_options(send_email, tmpdir):
    message_id = send_email(context={'message__render': 'we are_testing_emphasis **bold**\nnewline'})
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '<p>we are_testing_emphasis <strong>bold</strong><br>\nnewline</p>' in msg_file


def test_standard_sass(cli: TestClient, tmpdir, worker, loop):
    data = dict(
        uid=str(uuid4()),
        company_code='foobar',
        from_address='Sender Name <sender@example.org>',
        method='email-test',
        subject_template='test message',
        context={'message': 'this is a test'},
        recipients=[{'address': 'foobar@testing.com'}],
    )
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201
    assert worker.test_run() == 1
    message_id = data['uid'] + '-foobartestingcom'

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '<style>#body{-webkit-font-smoothing' in msg_file


def test_custom_sass(send_email, tmpdir):
    message_id = send_email(
        main_template='{{{ css }}}',
        context={'css__sass': '.foo {\n  .bar {\n    color: black;\n    width: (60px / 6);\n  }\n' '}'},
    )

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '.foo .bar{color:black;width:10px}' in msg_file
    assert '#body{-webkit-font-smoothing' not in msg_file


def test_invalid_mustache_subject(send_email, tmpdir, sync_db: SyncDb):
    message_id = send_email(
        subject_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_subject'
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\nsubject: {{ foo } test message\n' in msg_file

    message = sync_db.fetchrow_b('select * from messages')
    assert message['status'] == 'send'
    assert message['subject'] == '{{ foo } test message'
    assert message['body'] == '<body>\n\n</body>'


def test_invalid_mustache_body(send_email, sync_db: SyncDb):
    send_email(main_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_body')

    m = sync_db.fetchrow_b('select * from messages')
    assert m['status'] == 'render_failed'
    assert m['subject'] is None
    assert m['body'] == 'Error rendering email: unclosed tag at line 1'


# def test_send_with_pdf(send_email, tmpdir, sync_db: SyncDb):
#     message_id = send_email(
#         recipients=[
#             {
#                 'address': 'foobar@testing.com',
#                 'pdf_attachments': [
#                     {'name': 'testing.pdf', 'html': '<h1>testing</h1>', 'id': 123},
#                     {'name': 'different.pdf', 'html': '<h1>different</h1>'},
#                 ],
#             }
#         ]
#     )
#     assert len(tmpdir.listdir()) == 1
#     msg_file = tmpdir.join(f'{message_id}.txt').read()
#     assert 'testing.pdf' in msg_file
#
#     attachments = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == message_id)[
#         'attachments'
#     ]
#     assert set(attachments) == {'123::testing.pdf', '::different.pdf'}


def test_send_with_other_attachment(send_email, tmpdir, sync_db: SyncDb):
    message_id = send_email(
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
    attachments = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == message_id)[
        'attachments'
    ]
    assert set(attachments) == {'::calendar.ics'}


def test_send_with_other_attachment_pdf(send_email, tmpdir, sync_db: SyncDb):
    msg = 'Look this is some test data'
    encoded_content = base64.b64encode(b'Look this is some test data').decode()
    message_id = send_email(
        recipients=[
            {
                'address': 'foobar@testing.com',
                'attachments': [
                    {'name': 'test_pdf.pdf', 'content': msg, 'mime_type': 'application/pdf'},
                    {'name': 'test_pdf_encoded.pdf', 'content': encoded_content, 'mime_type': 'application/pdf'},
                ],
            }
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert f'test_pdf.pdf:{msg}' in msg_file
    assert f'test_pdf_encoded.pdf:{msg}' in msg_file
    attachments = sync_db.fetchrow_b('select * from messages where :where', where=V('external_id') == message_id)[
        'attachments'
    ]
    assert set(attachments) == {'::test_pdf.pdf', '::test_pdf_encoded.pdf'}


# def test_pdf_not_unicode(send_email, tmpdir, cli):
#     message_id = send_email(
#         recipients=[
#             {'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': '<h1>binary</h1>'}]}
#         ]
#     )
#     assert len(tmpdir.listdir()) == 1
#     msg_file = tmpdir.join(f'{message_id}.txt').read()
#     assert 'testing.pdf' in msg_file


def test_pdf_empty(send_email, tmpdir, dummy_server):
    message_id = send_email(
        recipients=[{'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': ''}]}]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\n  "attachments": []\n' in msg_file


def test_mandrill_send_client_error(sync_db: SyncDb, worker_ctx, call_send_emails, loop):
    group_id, c_id, m = call_send_emails(subject_template='__slow__')

    assert sync_db.fetchval('select count(*) from messages') == 0
    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        loop.run_until_complete(
            worker_send_email(
                worker_ctx,
                group_id,
                c_id,
                EmailRecipientModel(address='testing@recipient.com'),
                m,
                SpamCheckResult(spam=False, reason=''),
            )
        )
    assert exc_info.value.defer_score == 5_000

    assert sync_db.fetchval('select count(*) from messages') == 0


def test_mandrill_send_many_errors(sync_db: SyncDb, worker_ctx, call_send_emails, loop):
    group_id, c_id, m = call_send_emails()

    assert sync_db.fetchval('select count(*) from messages') == 0
    worker_ctx['job_try'] = 10

    loop.run_until_complete(
        worker_send_email(
            worker_ctx,
            group_id,
            c_id,
            EmailRecipientModel(address='testing@recipient.com'),
            m,
            SpamCheckResult(spam=False, reason=''),
        )
    )

    m = sync_db.fetchrow_b('select * from messages')
    assert m['status'] == 'send_request_failed'
    assert m['body'] == 'upstream error'


def test_mandrill_send_502(sync_db: SyncDb, call_send_emails, loop, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        loop.run_until_complete(
            worker_send_email(
                worker_ctx,
                group_id,
                c_id,
                EmailRecipientModel(address='testing@recipient.com'),
                m,
                SpamCheckResult(spam=False, reason=''),
            )
        )
    assert exc_info.value.defer_score == 5_000

    assert sync_db.fetchval('select count(*) from messages') == 0


def test_mandrill_send_502_last(sync_db: SyncDb, call_send_emails, loop, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = len(email_retrying)

    with pytest.raises(Retry) as exc_info:
        loop.run_until_complete(
            worker_send_email(
                worker_ctx,
                group_id,
                c_id,
                EmailRecipientModel(address='testing@recipient.com'),
                m,
                SpamCheckResult(spam=False, reason=''),
            )
        )
    assert exc_info.value.defer_score == 43_200_000

    assert sync_db.fetchval('select count(*) from messages') == 0


def test_mandrill_send_500_nginx(sync_db: SyncDb, call_send_emails, loop, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__500_nginx__')

    worker_ctx['job_try'] = 2

    with pytest.raises(Retry) as exc_info:
        loop.run_until_complete(
            worker_send_email(
                worker_ctx,
                group_id,
                c_id,
                EmailRecipientModel(address='testing@recipient.com'),
                m,
                SpamCheckResult(spam=False, reason=''),
            )
        )
    assert exc_info.value.defer_score == 10_000
    assert sync_db.fetchval('select count(*) from messages') == 0


def send_with_link(send_email, tmpdir):
    mid = send_email(
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


def test_link_shortening(send_email, tmpdir, cli: TestClient, sync_db: SyncDb, worker, loop):
    token = send_with_link(send_email, tmpdir)

    m = sync_db.fetchrow_b('select * from messages')
    assert m['status'] == 'send'

    link = sync_db.fetchrow_b('select * from links')
    assert link['id'] == AnyInt()
    assert link['message_id'] == m['id']
    assert link['token'] == token
    assert link['url'] == 'https://www.foobar.com'

    r = cli.get(
        '/l' + token,
        allow_redirects=False,
        headers={
            'X-Forwarded-For': '54.170.228.0, 141.101.88.55',
            'X-Request-Start': '1969660800',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/59.0.3071.115 Safari/537.36',
        },
    )
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'https://www.foobar.com'
    assert worker.test_run() == 2

    m = sync_db.fetchrow_b('select * from messages where :where', where=V('id') == m['id'])
    assert m['status'] == 'click'
    event = sync_db.fetchrow_b('select * from events')
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


def test_link_shortening_wrong_url(send_email, tmpdir, cli, dummy_server):
    token = send_with_link(send_email, tmpdir)
    # check we use the right url with a valid token but a different url arg
    r = cli.get('/l' + token + '?u=' + base64.urlsafe_b64encode(b'different').decode(), allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'https://www.foobar.com'


def test_link_shortening_wrong_url_missing(send_email, tmpdir, cli, dummy_server):
    token = send_with_link(send_email, tmpdir)
    r = cli.get('/lx' + token + '?u=' + base64.urlsafe_b64encode(b'different').decode(), allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'different'


def test_link_shortening_repeat(send_email, tmpdir, cli: TestClient, sync_db: SyncDb, worker, loop, dummy_server):
    token = send_with_link(send_email, tmpdir)
    r = cli.get('/l' + token, allow_redirects=False)
    assert r.status_code == 307, r.text
    assert worker.test_run() == 2
    assert r.headers['location'] == 'https://www.foobar.com'
    assert sync_db.fetchval('select count(*) from events') == 1

    r = cli.get('/l' + token, allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'https://www.foobar.com'
    assert sync_db.fetchval('select count(*) from events') == 1


def test_link_shortening_in_render(send_email, tmpdir, sync_db: SyncDb):
    mid = send_email(
        context={'message__render': 'test email {{ xyz }}\n', 'xyz': 'http://example.com/foobar'},
        company_code='test_link_shortening_in_render',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<p>test email https://click.example.com/l(.+?)\?u=(.+?)</p>', msg_file)
    assert m, msg_file
    token, enc_url = m.groups()

    link = sync_db.fetchrow_b('select * from links')
    assert link['url'] == 'http://example.com/foobar'
    assert link['token'] == token
    assert base64.urlsafe_b64decode(enc_url).decode() == 'http://example.com/foobar'


def test_link_shortening_keep_long_link(send_email, tmpdir, cli):
    mid = send_email(
        context={'message__render': 'test email {{ xyz_original }}\n', 'xyz': 'http://example.org/foobar'},
        company_code='test_link_shortening_in_render',
    )
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<p>test email http://example.org/foobar</p>', msg_file)
    assert m, msg_file


def test_link_shortening_not_image(send_email, tmpdir, cli):
    mid = send_email(
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


def test_not_json(cli: TestClient, tmpdir):
    r = cli.post('/send/email/', data='xxx', headers={'Authorization': 'testing-key'})
    assert r.status_code == 422, r.text


def test_invalid_json(cli: TestClient, tmpdir):
    data = {
        'uid': 'xxx',
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {},
        'recipients': [{'address': 'foobar_a@testing.com'}],
    }
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 422
    assert {
        'detail': [{'loc': ['body', 'uid'], 'msg': 'value is not a valid uuid', 'type': 'type_error.uuid'}]
    } == r.json()


def test_delete_old_messages(cli: TestClient, send_email, sync_db: SyncDb, worker, loop):
    for i in range(3):
        send_email()

    m = sync_db.fetch('select * from messages')[0]
    sync_db.execute(
        'update message_groups set created_ts = $1 where id = $2', datetime.today() - timedelta(days=366), m['group_id']
    )
    m = sync_db.fetch('select * from messages')[1]
    sync_db.execute(
        'update message_groups set created_ts = $1 where id = $2', datetime.today() - timedelta(days=200), m['group_id']
    )
    m = sync_db.fetch('select * from messages')[2]
    sync_db.execute(
        'update message_groups set created_ts = $1 where id = $2', datetime.today() + timedelta(days=1), m['group_id']
    )

    assert sync_db.fetchval('select count(*) from messages') == 3
    loop.run_until_complete(delete_old_emails({'pg': sync_db}))
    assert sync_db.fetchval('select count(*) from messages') == 2


@pytest.mark.spam
def test_send_spam_email(cli: TestClient, sync_db: SyncDb, worker):
    # Prepare the spammy message
    spammy_message = 'Buy now! This is not a drill! Click here for free money!'
    context = {'main_message__render': spammy_message}

    # Send the first email
    uuid1 = str(uuid4())
    recipients = []
    for i in range(21):
        recipients.append(
            {
                'first_name': f'First Name User {i}',
                'last_name': f'Last Name User {i}',
                'address': f'user{i}@example.org',
                'tags': ['test'],
            }
        )

    data1 = {
        'uid': uuid1,
        'company_code': 'foobar',
        'from_address': 'Spammer <spam@example.com>',
        'method': 'email-test',
        'subject_template': 'Spam offer',
        'main_template': '{{{ main_message }}}',
        'context': context,
        'recipients': recipients,
    }
    r1 = cli.post('/send/email/', json=data1, headers={'Authorization': 'testing-key'})
    assert r1.status_code == 201, r1.text
    assert worker.test_run() == len(recipients)

    # get the group form the message_groups table
    message_group = sync_db.fetchrow_b('select * from message_groups where :where', where=V('uuid') == uuid1)
    assert str(message_group['uuid']) == uuid1
    assert message_group['company_id'] == 1
    assert message_group['message_method'] == 'email-test'
    assert message_group['from_email'] == 'spam@example.com'
    assert message_group['from_name'] == 'Spammer'

    message = sync_db.fetchrow_b('select * from messages where :where', where=V('group_id') == message_group['id'])
    assert message['spam_status']
    assert message['spam_reason'] == 'This is spam for testing purposes'
    assert message['status'] == MessageStatus.send
    assert spammy_message in message['body']


@pytest.mark.spam
def test_send_multiple_spam_emails(cli: TestClient, sync_db: SyncDb, worker):
    # Prepare the spammy message
    spammy_message = 'Buy now! This is not a drill! Click here for free money!'
    context = {'main_message__render': spammy_message}

    # Send the first spam email
    uuid1 = str(uuid4())
    recipients = []
    for i in range(21):
        recipients.append(
            {
                'first_name': f'First Name User {i}',
                'last_name': f'Last Name User {i}',
                'address': f'user{i}@example.org',
                'tags': ['test'],
            }
        )
    data1 = {
        'uid': uuid1,
        'company_code': 'foobar',
        'from_address': 'Spammer <spam@example.com>',
        'method': 'email-test',
        'subject_template': 'Spam offer',
        'main_template': '{{{ main_message }}}',
        'context': context,  # same spammy content
        'recipients': recipients,
    }
    r1 = cli.post('/send/email/', json=data1, headers={'Authorization': 'testing-key'})
    assert r1.status_code == 201, r1.text

    # Send the second spam email with the same content
    uuid2 = str(uuid4())
    data2 = {
        'uid': uuid2,
        'company_code': 'foobar',
        'from_address': 'Spammer <spam@example.com>',
        'method': 'email-test',
        'subject_template': 'Spam offer',
        'main_template': '{{{ main_message }}}',
        'context': context,  # same spammy content
        'recipients': recipients,
    }
    r2 = cli.post('/send/email/', json=data2, headers={'Authorization': 'testing-key'})
    assert r2.status_code == 201, r2.text
    assert worker.test_run() == len(recipients) * 2

    # Check both emails are logged in the database and have status 'send'
    for uid in (uuid1, uuid2):
        group = sync_db.fetchrow_b('select * from message_groups where :where', where=V('uuid') == uid)
        assert str(group['uuid']) == uid
        message = sync_db.fetchrow_b('select * from messages where :where', where=V('group_id') == group['id'])
        assert message['spam_status']
        assert message['spam_reason'] == 'This is spam for testing purposes'
        assert message['status'] == MessageStatus.send
        assert spammy_message in message['body']


@pytest.mark.spam
def test_spam_check_only_for_more_than_20_recipients(cli, monkeypatch):
    called = {}

    async def fake_is_spam_email(self, email_info, company_name):
        called['called'] = True
        return SpamCheckResult(spam=False, reason='')

    monkeypatch.setattr(OpenAISpamEmailService, 'is_spam_email', fake_is_spam_email)

    # Case 1: 20 recipients (should NOT call spam check)
    called.clear()
    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'from_address': 'Tester <tester@example.com>',
        'method': 'email-test',
        'subject_template': 'Test',
        'context': {'message': 'test'},
        'recipients': [{'address': f'{i}@example.com'} for i in range(20)],
    }
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert not called.get('called', False)

    # Case 2: 21 recipients (should call spam check)
    called.clear()
    data['uid'] = str(uuid4())
    data['recipients'] = [{'address': f'{i}@example.com'} for i in range(21)]
    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert called.get('called', False)


@pytest.mark.spam
def test_non_spam_emails_are_cached(cli, monkeypatch):
    """Test that non-spam emails are cached and reused on subsequent identical requests."""
    call_count = {'count': 0}

    async def fake_is_spam_email(self, email_info, company_name):
        call_count['count'] += 1
        return SpamCheckResult(spam=False, reason='This is a legitimate email')

    monkeypatch.setattr(OpenAISpamEmailService, 'is_spam_email', fake_is_spam_email)

    context = {'main_message__render': 'Welcome to our tutoring service! Your lesson is scheduled.'}

    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'from_address': 'Tutor Agency <admin@tutoragency.com>',
        'method': 'email-test',
        'subject_template': 'Welcome to our service',
        'main_template': '{{{ main_message }}}',
        'context': context,
        'recipients': [{'address': f'student{i}@example.com'} for i in range(21)],
    }

    # First request should call spam check
    r1 = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r1.status_code == 201, r1.text
    assert call_count['count'] == 1  # Spam check called once

    # Second request with identical content should use cache
    data['uid'] = str(uuid4())  # Different UID but same content
    r2 = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r2.status_code == 201, r2.text
    assert call_count['count'] == 1  # Spam check NOT called again (cached result used)


def test_get_spam_checker():
    """Test that get_spam_checker creates and returns the correct EmailSpamChecker instance."""

    mock_cache_service = Mock()
    mock_spam_service = Mock()
    mock_checker = Mock()
    mock_openai_client = Mock()

    # Patch the service constructors to return our mocks
    with patch('src.views.email.SpamCacheService', return_value=mock_cache_service) as mock_cache_class, patch(
        'src.views.email.OpenAISpamEmailService', return_value=mock_spam_service
    ) as mock_spam_class, patch(
        'src.views.email.EmailSpamChecker', return_value=mock_checker
    ) as mock_checker_class, patch(
        'src.views.email.glove'
    ) as mock_glove, patch(
        'src.views.email.get_openai_client', return_value=mock_openai_client
    ) as mock_get_client:
        mock_glove.redis = Mock()

        result = get_spam_checker()

        mock_cache_class.assert_called_once_with(mock_glove.redis)
        mock_get_client.assert_called_once()
        mock_spam_class.assert_called_once_with(mock_openai_client)

        mock_checker_class.assert_called_once_with(mock_spam_service, mock_cache_service)

        assert result == mock_checker


def test_get_cache_key_with_emojis_and_special_chars():
    """
    Test that SpamCacheService.get_cache_key correctly generates cache keys
    for messages containing various character types.

    Verifies that the cache key generation handles:
    - Emoji characters (e.g. 👋, 🎉)
    - Unicode special characters and accents (e.g. é, ç)
    - Asian language characters (Chinese, Japanese, Korean)
    - Mixed content with multiple character types
    - Empty messages
    - HTML entities
    - Various line ending formats

    This ensures the caching system works reliably across all possible message content.
    """

    # Create a mock redis client
    mock_redis = Mock()
    cache_service = SpamCacheService(mock_redis)

    # Test cases with various special characters and emojis
    test_cases = [
        {
            'name': 'basic_emojis',
            'message': 'Hello! 👋 Welcome to our service! 🎉',
            'company_code': 'test_company',
            'expected_prefix': 'spam_content:',
        },
        {
            'name': 'unicode_special_chars',
            'message': 'Café résumé naïve façade',
            'company_code': 'accent_company',
            'expected_prefix': 'spam_content:',
        },
        {
            'name': 'asian_characters',
            'message': '你好世界！こんにちは世界！안녕하세요 세계!',
            'company_code': 'asian_company',
            'expected_prefix': 'spam_content:',
        },
        {
            'name': 'mixed_content',
            'message': '🎓 Education + 📚 Learning = 💡 Success! 你好!',
            'company_code': 'mixed_company',
            'expected_prefix': 'spam_content:',
        },
        {'name': 'empty_message', 'message': '', 'company_code': 'empty_company', 'expected_prefix': 'spam_content:'},
        {
            'name': 'html_entities',
            'message': '&lt;script&gt;alert("Hello")&lt;/script&gt;',
            'company_code': 'html_company',
            'expected_prefix': 'spam_content:',
        },
        {
            'name': 'newlines_and_tabs',
            'message': 'Line 1\nLine 2\tTabbed content\r\nWindows line',
            'company_code': 'format_company',
            'expected_prefix': 'spam_content:',
        },
    ]

    for test_case in test_cases:
        # Create EmailSendModel with the test message
        email_model = EmailSendModel(
            uid=str(uuid4()),
            company_code=test_case['company_code'],
            from_address='Test User <test@example.com>',
            method='email-test',
            subject_template='Test Subject',
            context={'main_message__render': test_case['message']},
            recipients=[],
        )

        # Get the cache key
        cache_key = cache_service.get_cache_key(email_model)

        # Verify the key format
        assert cache_key.startswith(test_case['expected_prefix'])
        assert cache_key.endswith(f":{test_case['company_code']}")

        # Verify it contains a hash (64 hex characters)
        parts = cache_key.split(':')
        assert len(parts) == 3
        assert len(parts[1]) == 64  # SHA256 hash is 64 hex characters
        assert all(c in '0123456789abcdef' for c in parts[1])  # Valid hex

        # Verify that different messages produce different hashes
        if test_case['name'] != 'empty_message':
            # Create another model with slightly different message
            email_model2 = EmailSendModel(
                uid=str(uuid4()),
                company_code=test_case['company_code'],
                from_address='Test User <test@example.com>',
                method='email-test',
                subject_template='Test Subject',
                context={'main_message__render': test_case['message'] + 'extra'},
                recipients=[],
            )
            cache_key2 = cache_service.get_cache_key(email_model2)
            assert (
                cache_key != cache_key2
            ), f"Different messages should produce different cache keys for {test_case['name']}"

        # Verify that same message with different company code produces different keys
        email_model3 = EmailSendModel(
            uid=str(uuid4()),
            company_code=test_case['company_code'] + '_different',
            from_address='Test User <test@example.com>',
            method='email-test',
            subject_template='Test Subject',
            context={'main_message__render': test_case['message']},
            recipients=[],
        )
        cache_key3 = cache_service.get_cache_key(email_model3)
        assert (
            cache_key != cache_key3
        ), f"Different company codes should produce different cache keys for {test_case['name']}"


@pytest.mark.spam
def test_spam_logging_includes_body(cli: TestClient, sync_db: SyncDb, worker, caplog):
    caplog.set_level(logging.ERROR, logger='spam.email_checker')

    recipients = []
    for i in range(21):
        recipients.append(
            {
                'first_name': f'User{i}',
                'last_name': f'Last{i}',
                'address': f'user{i}@example.org',
                'tags': ['test'],
            }
        )

    data = {
        'uid': str(uuid4()),
        'company_code': 'foobar',
        'from_address': 'Spammer <spam@example.com>',
        'method': 'email-test',
        'subject_template': 'Urgent: {{ company_name }} Alert!',
        'main_template': '{{{ main_message }}}',
        'context': {
            'main_message__render': 'Hi {{ recipient_first_name }},\n\nDont miss out on <b>FREE MONEY</b>! '
            'Click [here]({{ login_link }}) now!\n\nRegards,\n{{ company_name }}',
            'company_name': 'TestCorp',
            'login_link': 'https://spam.example.com/click',
        },
        'recipients': recipients,
    }

    r = cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status_code == 201, r.text
    assert worker.test_run() == len(recipients)

    records = [r for r in caplog.records if r.name == 'spam.email_checker' and r.levelno == logging.ERROR]
    assert len(records) == 1
    body = getattr(records[0], 'email_main_body')
    assert (
        body == 'Hi {{ recipient_first_name }}, Dont miss out on FREE MONEY! '
        'Click [here]({{ login_link }}) now! Regards, {{ company_name }}'
    )
