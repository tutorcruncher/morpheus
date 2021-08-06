import base64
import hashlib
import hmac
import json
import logging
import pytest
import re
from arq import Retry
from datetime import datetime, timezone
from pathlib import Path
from pytest_toolbox.comparison import AnyInt, RegexStr
from uuid import uuid4

from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from src.ext import ApiError
from src.models import Message, Event, Link
from src.schema import EmailRecipientModel
from src.worker import email_retrying, send_email as worker_send_email

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
    assert loop.run_until_complete(worker.run_check()) == 1
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


def test_webhook(cli: TestClient, send_email, db: Session, worker, loop):
    uuid = str(uuid4())
    message_id = send_email(uid=uuid)

    message = Message.manager.get(db, external_id=message_id)
    assert message.status == 'send'
    first_update_ts = message.update_ts

    events = Event.manager.count(db)
    assert events == 0

    data = {'ts': int(2e9), 'event': 'open', '_id': message_id, 'foobar': ['hello', 'world']}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert loop.run_until_complete(worker.run_check()) == 2

    db.refresh(message)
    assert message.status == 'open'
    assert message.update_ts > first_update_ts
    events = [e.to_dict() for e in Event.manager.filter(db, message_id=message.id)]
    assert len(events) == 1
    e = events[0]
    assert e['ts'] == datetime(2033, 5, 18, 3, 33, 20, tzinfo=timezone.utc)
    assert e['extra'] == RegexStr('{.*}')
    assert events == [
        {
            'id': AnyInt(),
            'message_id': message.id,
            'status': 'open',
            'ts': datetime(2033, 5, 18, 3, 33, 20, tzinfo=timezone.utc),
            'extra': RegexStr('{.*}'),
        }
    ]
    extra = json.loads(events[0]['extra'])
    assert extra['diag'] is None
    assert extra['opens'] is None


def test_webhook_old(cli: TestClient, send_email, db: Session, worker, loop):
    msg_id = send_email()
    message = Message.manager.get(db, external_id=msg_id)
    assert message.status == 'send'
    first_update_ts = message.update_ts
    events = Event.manager.filter(db, message_id=message.id)
    assert len(events) == 0
    data = {'ts': int(1.4e9), 'event': 'open', '_id': msg_id}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert loop.run_until_complete(worker.run_check()) == 2

    db.refresh(message)
    assert message.status == 'send'
    events = Event.manager.filter(db, message_id=message.id)
    assert len(events) == 1
    assert message.update_ts == first_update_ts


def test_webhook_repeat(cli: TestClient, send_email, db: Session, worker, loop):
    msg_id = send_email()
    message = Message.manager.get(db, external_id=msg_id)
    assert message.status == 'send'
    events = Event.manager.filter(db, message_id=message.id)
    assert len(events) == 0
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id}
    for _ in range(3):
        r = cli.post('/webhook/test/', json=data)
        assert r.status_code == 200, r.text
    data = {'ts': '2032-06-06T12:10', 'event': 'open', '_id': msg_id, 'user_agent': 'xx'}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    assert loop.run_until_complete(worker.run_check()) == 5

    db.refresh(message)
    assert message.status == 'open'
    events = Event.manager.filter(db, message_id=message.id)
    assert len(events) == 2


def test_webhook_missing(cli: TestClient, send_email, db: Session):
    msg_id = send_email()

    data = {'ts': int(1e10), 'event': 'open', '_id': 'missing', 'foobar': ['hello', 'world']}
    r = cli.post('/webhook/test/', json=data)
    assert r.status_code == 200, r.text
    message = Message.manager.get(db, external_id=msg_id)
    assert message.status == 'send'
    events = Event.manager.filter(db, message_id=message.id)
    assert len(events) == 0


def test_mandrill_send(send_email, db: Session, dummy_server):
    assert Message.manager.count(db) == 0
    send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@testing.com'}])

    m = Message.manager.get(db, external_id='mandrill-foobaratestingcom')
    assert m.to_address == 'foobar_a@testing.com'
    assert dummy_server.app['log'] == ['POST /mandrill/messages/send.json > 200']


def test_send_mandrill_with_other_attachments(send_email, db: Session, dummy_server):
    with open(THIS_DIR / 'attachments/testing.pdf', 'rb') as f:
        content = f.read()
    sent_content = base64.b64encode(content).decode()
    assert Message.manager.count(db) == 0
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
    m = Message.manager.get(db, external_id='mandrill-foobarctestingcom')
    assert m.to_address == 'foobar_c@testing.com'
    assert set(m.attachments) == {'::calendar.ics', '::testing.pdf'}


def test_example_email_address(send_email, db: Session, dummy_server):
    assert Message.manager.count(db) == 0
    send_email(method='email-mandrill', recipients=[{'address': 'foobar_a@example.com'}])

    m = Message.manager.get(db, external_id='mandrill-foobaraexamplecom')
    assert m.to_address == 'foobar_a@example.com'
    assert m.status == 'send'


def test_mandrill_webhook(cli: TestClient, send_email, db: Session, worker, loop, dummy_server, settings):
    send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    assert Message.manager.count(db) == 1

    assert Event.manager.count(db) == 0

    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'mandrill-testingexampleorg', 'foobar': ['hello', 'world']}]
    data = {'events': messages}

    sig = base64.b64encode(
        hmac.new(
            settings.mandrill_webhook_key.encode(),
            msg=(
                b'https://localhost/webhook/mandrill/mandrill_events[{"ts": 1969660800, '
                b'"event": "open", "_id": "mandrill-testingexampleorg", "foobar": ["hello", "world"]}]'
            ),
            digestmod=hashlib.sha1,
        ).digest()
    )
    r = cli.post('/webhook/mandrill/', json=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status_code == 200, r.json()
    assert loop.run_until_complete(worker.run_check()) == 2

    assert Event.manager.count(db) == 1

    events = Event.manager.all(db)
    assert events[0].ts == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)
    assert events[0].status == 'open'


def test_mandrill_webhook_invalid(cli: TestClient, send_email, db: Session, dummy_server, settings):
    send_email(method='email-mandrill', recipients=[{'address': 'testing@example.org'}])
    messages = [{'ts': 1969660800, 'event': 'open', '_id': 'e587306</div></body><meta name=', 'foobar': ['x']}]
    data = {'events': messages}

    sig = base64.b64encode(
        hmac.new(
            settings.mandrill_webhook_key.encode(),
            msg=(
                b'https://localhost/webhook/mandrill/mandrill_events[{"ts": 1969660800, '
                b'"event": "open", "_id": "e587306</div></body><meta name=", "foobar": ["x"]}]'
            ),
            digestmod=hashlib.sha1,
        ).digest()
    )
    r = cli.post('/webhook/mandrill/', json=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status_code == 200, r.text

    events = Event.manager.all(db)
    assert len(events) == 0


def test_mandrill_send_bad_template(cli: TestClient, send_email, db: Session, dummy_server):
    assert Message.manager.count(db) == 0
    send_email(
        method='email-mandrill', main_template='{{ foo } test message', recipients=[{'address': 'foobar_b@testing.com'}]
    )
    message = Message.manager.get(db)
    assert message.status == 'render_failed'


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
    assert loop.run_until_complete(worker.run_check()) == 2

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
        main_template=('message: |{{{ message }}}|\n' 'foo: {{ foo }}\n' 'partial: {{> test_p }}'),
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
    assert loop.run_until_complete(worker.run_check()) == 1
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


def test_invalid_mustache_subject(send_email, tmpdir, db: Session):
    message_id = send_email(
        subject_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_subject'
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\nsubject: {{ foo } test message\n' in msg_file

    messages = Message.manager.get(db)
    assert messages.status == 'send'
    assert messages.subject == '{{ foo } test message'
    assert messages.body == '<body>\n\n</body>'


def test_invalid_mustache_body(send_email, db: Session):
    send_email(main_template='{{ foo } test message', context={'foo': 'FOO'}, company_code='test_invalid_mustache_body')

    m = Message.manager.get(db)
    assert m.status == 'render_failed'
    assert m.subject is None
    assert m.body == 'Error rendering email: unclosed tag at line 1'


def test_send_with_pdf(send_email, tmpdir, db: Session):
    message_id = send_email(
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
    debug(msg_file)
    assert '<h1>testing</h1>"' in msg_file

    attachments = Message.manager.get(db, external_id=message_id).attachments
    assert set(attachments) == {'123::testing.pdf', '::different.pdf'}


def test_send_with_other_attachment(send_email, tmpdir, db: Session):
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
    attachments = Message.manager.get(db, external_id=message_id).attachments
    assert set(attachments) == {'::calendar.ics'}


def test_send_with_other_attachment_pdf(send_email, tmpdir, db: Session):
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
    attachments = Message.manager.get(db, external_id=message_id).attachments
    assert set(attachments) == {'::test_pdf.pdf', '::test_pdf_encoded.pdf'}


def test_pdf_not_unicode(send_email, tmpdir, cli):
    message_id = send_email(
        recipients=[
            {'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': '<h1>binary</h1>'}]}
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '"testing.pdf:binary-"' in msg_file


def test_pdf_empty(send_email, tmpdir, dummy_server):
    message_id = send_email(
        recipients=[{'address': 'foobar@testing.com', 'pdf_attachments': [{'name': 'testing.pdf', 'html': ''}]}]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\n  "attachments": []\n' in msg_file


def test_mandrill_send_client_error(db, worker_ctx, call_send_emails):
    group_id, c_id, m = call_send_emails(subject_template='__slow__')

    assert Message.manager.count(db) == 0
    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 5_000

    assert Message.manager.count(db) == 0


def test_mandrill_send_many_errors(db, worker_ctx, call_send_emails):
    group_id, c_id, m = call_send_emails()

    assert Message.manager.count(db) == 0
    worker_ctx['job_try'] = 10

    worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)

    m = Message.manager.get()
    assert m.status == 'send_request_failed'
    assert m.body == 'upstream error'


def test_mandrill_send_502(db, call_send_emails, worker_ctx, caplog):
    caplog.set_level(logging.INFO, logger='morpheus')
    group_id, c_id, m = call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = 1

    with pytest.raises(Retry) as exc_info:
        worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 5_000

    assert Message.manager.count(db) == 0
    assert 'Mandrill unexpected response POST /messages/send.json -> 502' in caplog.text
    assert 'temporary mandrill error' in caplog.text


def test_mandrill_send_502_last(db, call_send_emails, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__502__')

    worker_ctx['job_try'] = len(email_retrying)

    with pytest.raises(Retry) as exc_info:
        worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 43_200_000

    assert Message.manager.count(db) == 0


def test_mandrill_send_500_nginx(db, call_send_emails, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__500_nginx__')

    worker_ctx['job_try'] = 2

    with pytest.raises(Retry) as exc_info:
        worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.defer_score == 10_000

    assert Message.manager.count(db) == 0


def test_mandrill_send_500_not_nginx(db, call_send_emails, worker_ctx):
    group_id, c_id, m = call_send_emails(subject_template='__500__')

    worker_ctx['job_try'] = 1

    with pytest.raises(ApiError) as exc_info:
        worker_send_email(worker_ctx, group_id, c_id, EmailRecipientModel(address='testing@recipient.com'), m)
    assert exc_info.value.status == 500

    assert Message.manager.count(db) == 0


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


def test_link_shortening(send_email, tmpdir, cli: TestClient, db: Session, worker, loop):
    token = send_with_link(send_email, tmpdir)

    m = Message.manager.get(db)
    assert m.status == 'send'

    link = Link.objects.get(db)
    assert dict(link) == {'id': AnyInt(), 'message_id': m['id'], 'token': token, 'url': 'https://www.foobar.com'}

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
    assert loop.run_until_complete(worker.run_check()) == 2

    m_status = db.refresh(m).status
    assert m_status == 'click'
    event = Event.manager.get(db)
    assert event.status == 'click'
    assert event.ts == datetime(2032, 6, 1, 0, 0, tzinfo=timezone.utc)
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


def test_link_shortening_repeat(send_email, tmpdir, cli: TestClient, db: Session, worker, loop, dummy_server):
    token = send_with_link(send_email, tmpdir)
    r = cli.get('/l' + token, allow_redirects=False)
    assert r.status_code == 307, r.text
    assert loop.run_until_complete(worker.run_check()) == 2
    assert r.headers['location'] == 'https://www.foobar.com'
    assert Event.manager.count(db) == 1

    r = cli.get('/l' + token, allow_redirects=False)
    assert r.status_code == 307, r.text
    assert r.headers['location'] == 'https://www.foobar.com'
    assert Event.manager.count(db) == 1


def test_link_shortening_in_render(send_email, tmpdir, db: Session):
    mid = send_email(
        context={'message__render': 'test email {{ xyz }}\n', 'xyz': 'http://example.com/foobar'},
        company_code='test_link_shortening_in_render',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{mid}.txt').read()
    m = re.search(r'<p>test email https://click.example.com/l(.+?)\?u=(.+?)</p>', msg_file)
    assert m, msg_file
    token, enc_url = m.groups()

    link = Link.manager.get(db)
    assert link.url == 'http://example.com/foobar'
    assert link.token == token
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
