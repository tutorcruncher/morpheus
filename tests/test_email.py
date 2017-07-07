import base64
import hashlib
import hmac
import json
import re
import uuid


async def test_send_email(cli, tmpdir):
    data = {
        'uid': 'x' * 20,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {
            'message__render': '# hello\n\nThis is a **{{ b }}**.\n',
            'a': 'Apple',
            'b': f'Banana',
        },
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': f'bar',
                'user_link': '/user/profile/42/',
                'address': 'foobar@example.com',
                'tags': ['foobar'],
            }
        ]
    }
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join('xxxxxxxxxxxxxxxxxxxx-foobarexamplecom.txt').read()
    print(msg_file)
    assert '\nsubject: test email Apple\n' in msg_file
    assert '\n<p>This is a <strong>Banana</strong>.</p>\n' in msg_file
    data = json.loads(re.search('data: ({.*?})\ncontent:', msg_file, re.S).groups()[0])
    assert data['from_email'] == 's@muelcolvin.com'
    assert data['to_address'] == 'foobar@example.com'
    assert data['to_user_link'] == '/user/profile/42/'
    assert data['attachments'] == []
    assert set(data['tags']) == {'xxxxxxxxxxxxxxxxxxxx', 'foobar'}


async def test_webhook(cli, send_email):
    message_id = await send_email(uid='x' * 20)
    r = await cli.server.app['es'].get('messages/email-test/xxxxxxxxxxxxxxxxxxxx-foobartestingcom')
    data = await r.json()
    assert data['_source']['status'] == 'send'
    first_update_ts = data['_source']['update_ts']
    assert data['_source']['send_ts'] == first_update_ts
    assert len(data['_source']['events']) == 0
    data = {
        'ts': int(2e9),
        'event': 'open',
        '_id': message_id,
        'foobar': ['hello', 'world']
    }
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    r = await cli.server.app['es'].get('messages/email-test/xxxxxxxxxxxxxxxxxxxx-foobartestingcom')
    data = await r.json()
    assert data['_source']['status'] == 'open'
    assert len(data['_source']['events']) == 1
    assert data['_source']['update_ts'] > first_update_ts


async def test_webhook_old(cli, send_email):
    msg_id = await send_email()
    r = await cli.server.app['es'].get(f'messages/email-test/{msg_id}')
    data = await r.json()
    assert data['_source']['status'] == 'send'
    first_update_ts = data['_source']['update_ts']
    assert data['_source']['send_ts'] == first_update_ts
    assert len(data['_source']['events']) == 0
    data = {
        'ts': int(1.4e9),
        'event': 'open',
        '_id': msg_id,
    }
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()

    r = await cli.server.app['es'].get(f'messages/email-test/{msg_id}')
    data = await r.json()
    assert data['_source']['status'] == 'send'
    assert len(data['_source']['events']) == 1
    assert data['_source']['update_ts'] == first_update_ts


async def test_webhook_missing(cli, send_email):
    msg_id = await send_email()

    data = {
        'ts': int(1e10),
        'event': 'open',
        '_id': 'missing',
        'foobar': ['hello', 'world']
    }
    r = await cli.post('/webhook/test/', json=data)
    assert r.status == 200, await r.text()
    r = await cli.server.app['es'].get(f'messages/email-test/{msg_id}')
    data = await r.json()
    assert data['_source']['status'] == 'send'
    assert len(data['_source']['events']) == 0


async def test_mandrill_send(cli, send_email):
    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobaratestingcom', allowed_statuses='*')
    assert r.status == 404, await r.text()
    await send_email(
        method='email-mandrill',
        recipients=[{'address': 'foobar_a@testing.com'}]
    )

    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobaratestingcom', allowed_statuses='*')
    assert r.status == 200, await r.text()
    data = await r.json()
    assert data['_source']['to_address'] == 'foobar_a@testing.com'


async def test_mandrill_webhook(cli):
    await cli.server.app['es'].post(
        f'messages/email-mandrill/test-webhook',
        company='foobar',
        send_ts=123,
        update_ts=123,
        status='send',
        to_address='testing@example.com',
        events=[]
    )
    r = await cli.server.app['es'].get('messages/email-mandrill/test-webhook')
    assert r.status == 200
    data = await r.json()
    assert len(data['_source']['events']) == 0
    messages = [{'ts': int(1e10), 'event': 'open', '_id': 'test-webhook', 'foobar': ['hello', 'world']}]
    data = {'mandrill_events': json.dumps(messages)}

    sig = base64.b64encode(
        hmac.new(
            b'testing',
            msg=(b'https://None/webhook/mandrill/mandrill_events[{"ts": 10000000000, '
                 b'"event": "open", "_id": "test-webhook", "foobar": ["hello", "world"]}]'),
            digestmod=hashlib.sha1
        ).digest()
    )
    r = await cli.post('/webhook/mandrill/', data=data, headers={'X-Mandrill-Signature': sig.decode()})
    assert r.status == 200, await r.text()
    r = await cli.server.app['es'].get('messages/email-mandrill/test-webhook')
    assert r.status == 200
    data = await r.json()
    assert len(data['_source']['events']) == 1
    assert data['_source']['update_ts'] == 1e13
    assert data['_source']['status'] == 'open'


async def test_mandrill_send_bad_template(cli, send_email):
    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobarbtestingcom', allowed_statuses='*')
    assert r.status == 404, await r.text()
    await send_email(
        method='email-mandrill',
        main_template='{{ foo } test message',
        recipients=[{'address': 'foobar_b@testing.com'}]
    )

    r = await cli.server.app['es'].get('messages/email-mandrill/mandrill-foobarbtestingcom', allowed_statuses='*')
    assert r.status == 404, await r.text()


async def test_send_email_headers(cli, tmpdir):
    uid = str(uuid.uuid4())
    data = {
        'uid': uid,
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {
            'message__render': 'test email {{ a }} {{ b}} {{ c }}.\n',
            'a': 'Apple',
            'b': f'Banana',
        },
        'headers': {
            'Reply-To': 'another@whoever.com',
            'List-Unsubscribe': '<http://example.com/unsub>'
        },
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': f'bar',
                'address': f'foobar@example.com',
                'context': {
                    'c': 'Carrot',
                },
            },
            {
                'address': f'2@example.com',
                'context': {
                    'b': 'Banker',
                },
                'headers': {
                    'List-Unsubscribe': '<http://example.com/different>'
                },
            }
        ]
    }
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 2
    msg_file = tmpdir.join(f'{uid}-foobarexamplecom.txt').read()
    # print(msg_file)
    assert '<p>test email Apple Banana Carrot.</p>\n' in msg_file
    assert '"to_address": "foobar@example.com",\n' in msg_file
    assert '"Reply-To": "another@whoever.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.com/unsub>"\n' in msg_file

    msg_file = tmpdir.join(f'{uid}-2examplecom.txt').read()
    print(msg_file)
    assert '<p>test email Apple Banker .</p>\n' in msg_file
    assert '"to_address": "2@example.com",\n' in msg_file
    assert '"Reply-To": "another@whoever.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.com/different>"\n' in msg_file


async def test_send_unsub_context(send_email, tmpdir):
    uid = str(uuid.uuid4())
    await send_email(
        uid=uid,
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'unsubscribe_link': 'http://example.com/unsub'
        },
        recipients=[
            {'address': f'1@example.com'},
            {
                'address': f'2@example.com',
                'context': {
                    'unsubscribe_link': 'http://example.com/context'
                },
                'headers': {
                    'List-Unsubscribe': '<http://example.com/different>'
                },
            }
        ]
    )
    assert len(tmpdir.listdir()) == 2
    msg_file = tmpdir.join(f'{uid}-1examplecom.txt').read()
    # print(msg_file)
    assert '"to_address": "1@example.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.com/unsub>"\n' in msg_file
    assert '<p>test email http://example.com/unsub.</p>\n' in msg_file

    msg_file = tmpdir.join(f'{uid}-2examplecom.txt').read()
    print(msg_file)
    assert '"to_address": "2@example.com",\n' in msg_file
    assert '"List-Unsubscribe": "<http://example.com/different>"\n' in msg_file
    assert '<p>test email http://example.com/context.</p>\n' in msg_file


async def test_markdown_context(send_email, tmpdir):
    message_id = await send_email(
        main_template='testing {{{ foobar }}}',
        context={
            'message__render': 'test email {{ unsubscribe_link }}.\n',
            'foobar__md': '[hello](www.example.com/hello)'
        },
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert 'content:\ntesting <p><a href="www.example.com/hello">hello</a></p>\n' in msg_file


async def test_partials(send_email, tmpdir):
    message_id = await send_email(
        main_template=('message: |{{{ message }}}|\n'
                       'foo: {{ foo }}\n'
                       'partial: {{> test_p }}'),
        context={
            'message__render': '{{foo}} {{> test_p }}',
            'foo': 'FOO',
            'bar': 'BAR',
        },
        mustache_partials={
            'test_p': 'foo ({{ foo }}) bar **{{ bar }}**',
        }
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert """
content:
message: |<p>FOO foo (FOO) bar <strong>BAR</strong></p>
|
foo: FOO
partial: foo (FOO) bar **BAR**
""" in msg_file


async def test_macros(send_email, tmpdir):
    message_id = await send_email(
        main_template='macro result: foobar(hello | {{ foo }})',
        context={
            'foo': 'FOO',
            'bar': 'BAR',
        },
        macros={
            'foobar(a | b)': '___{{ a }} {{b}}___'
        }
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
      </div>\n"""
        }
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert """
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
""" in msg_file


async def test_macro_in_message(send_email, tmpdir):
    message_id = await send_email(
        context={
            'pay_link': '/pay/now/123/',
            'first_name': 'John',
            'message__render': (
                '# hello {{ first_name }}\n'
                'centered_button(Pay now | {{ pay_link }})\n'
            )
        },
        macros={
            'centered_button(text | link)': (
                '<div class="button">\n'
                '  <a href="{{ link }}"><span>{{ text }}</span></a>\n'
                '</div>\n'
            )
        }
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert """
content:
<body>
<h1>hello John</h1>

<div class="button">
  <a href="/pay/now/123/"><span>Pay now</span></a>
</div>

</body>
""" in msg_file


async def test_send_md_options(send_email, tmpdir):
    message_id = await send_email(context={'message__render': 'we are_testing_emphasis **bold**\nnewline'})
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '<p>we are_testing_emphasis <strong>bold</strong><br>\nnewline</p>' in msg_file


async def test_standard_sass(cli, tmpdir):
    data = dict(
        uid=str(uuid.uuid4()),
        company_code='foobar',
        from_address='Sender Name <sender@example.com>',
        method='email-test',
        subject_template='test message',
        context={'message': 'this is a test'},
        recipients=[{'address': 'foobar@testing.com'}]
    )
    r = await cli.post('/send/email/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201
    message_id = data['uid'] + '-foobartestingcom'

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '<style>#body{-webkit-font-smoothing' in msg_file


async def test_custom_sass(send_email, tmpdir):
    message_id = await send_email(
        main_template='{{{ css }}}',
        context={
            'css__sass': (
                '.foo {\n'
                '  .bar {\n'
                '    color: black;\n'
                '    width: (60px / 6);\n'
                '  }\n'
                '}'
            )
        }
    )

    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '.foo .bar{color:black;width:10px}' in msg_file
    assert '#body{-webkit-font-smoothing' not in msg_file


async def test_invalid_mustache_subject(send_email, tmpdir, cli):
    message_id = await send_email(
        subject_template='{{ foo } test message',
        context={'foo': 'FOO'},
        company_code='test_invalid_mustache_subject',
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    assert '\nsubject: {{ foo } test message\n' in msg_file

    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.server.app['es'].get('messages/email-test/_search?q=company:test_invalid_mustache_subject')
    response_data = await r.json()
    # import json
    # print(json.dumps(response_data, indent=2))
    assert response_data['hits']['total'] == 1
    source = response_data['hits']['hits'][0]['_source']
    assert source['status'] == 'send'
    assert source['subject'] == '{{ foo } test message'
    assert source['body'] == '<body>\n\n</body>'


async def test_invalid_mustache_body(send_email, tmpdir, cli):
    await send_email(
        main_template='{{ foo } test message',
        context={'foo': 'FOO'},
        company_code='test_invalid_mustache_body',
    )

    await cli.server.app['es'].get('messages/_refresh')
    r = await cli.server.app['es'].get('messages/email-test/_search?q=company:test_invalid_mustache_body')
    response_data = await r.json()
    import json
    print(json.dumps(response_data, indent=2))
    assert response_data['hits']['total'] == 1
    source = response_data['hits']['hits'][0]['_source']
    assert source['status'] == 'render_failed'
    assert 'subject' not in source
    # https://github.com/noahmorrison/chevron/pull/22
    assert source['body'].startswith('Error rendering email: unclosed tag at line')


async def test_send_with_pdf(send_email, tmpdir, cli):
    message_id = await send_email(
        recipients=[
            {
                'address': 'foobar@testing.com',
                'pdf_attachments': [
                    {
                        'name': 'testing.pdf',
                        'html': '<h1>testing</h1>',
                        'id': 123,
                    },
                    {
                        'name': 'different.pdf',
                        'html': '<h1>different</h1>',
                    }
                ]
            }
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '<h1>testing</h1>"' in msg_file
    r = await cli.server.app['es'].get(f'messages/email-test/{message_id}')
    data = await r.json()
    assert set(data['_source']['attachments']) == {'123::testing.pdf', '::different.pdf'}


async def test_pdf_empty(send_email, tmpdir):
    message_id = await send_email(
        recipients=[
            {
                'address': 'foobar@testing.com',
                'pdf_attachments': [
                    {
                        'name': 'testing.pdf',
                        'html': '',
                    }
                ]
            }
        ]
    )
    assert len(tmpdir.listdir()) == 1
    msg_file = tmpdir.join(f'{message_id}.txt').read()
    print(msg_file)
    assert '\n  "attachments": []\n' in msg_file
