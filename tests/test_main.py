import string


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert 'Morpheus, for more information see' in await r.text()


async def test_index_head(cli):
    r = await cli.head('/')
    assert r.status == 200
    assert '' == await r.text()


async def test_robots(cli):
    r = await cli.get('/robots.txt')
    assert r.status == 200
    assert 'User-agent: *' in await r.text()


async def test_favicon(cli):
    r = await cli.get('/favicon.ico', allow_redirects=False)
    assert r.status == 301
    assert r.headers['Location'] == 'https://secure.tutorcruncher.com/favicon.ico'


async def test_send_message(cli, tmpdir):
    data = {
        'uid': '123' * 7,
        'markdown_template': '# hello\n\nThis is a **{{ b }}**.\n',
        'company_code': 'foobar',
        'from_address': 'Samuel <s@muelcolvin.com>',
        'method': 'email-test',
        'subject_template': 'test email {{ a }}',
        'context': {
            'a': 'Apple',
            'b': f'Banana',
        },
        'recipients': [
            {
                'first_name': 'foo',
                'last_name': f'bar',
                'address': f'foobar@example.com',
            }
        ]
    }
    r = await cli.post('/send/', json=data, headers={'Authorization': 'testing-key'})
    assert r.status == 201, await r.text()
    assert len(tmpdir.listdir()) == 1
    f = tmpdir.listdir()[0]
    msg_file = f.read()
    assert '\nsubject: test email Apple\n' in msg_file
    assert '\n<p>This is a <strong>Banana</strong>.</p>\n' in msg_file
    assert '"from_email": "s@muelcolvin.com",\n' in msg_file
    assert '"to_email": "foobar@example.com",\n' in msg_file
