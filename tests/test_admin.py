import base64
import re
import uuid


def gen_headers():
    token = base64.b64encode(b'whoever:testing').decode()
    return {'Authorization': f'Basic {token}'}


async def test_aggregates(cli, send_email):
    await cli.server.app['es'].create_indices(True)
    for i in range(4):
        await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'{i}@t.com'}])

    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get('/admin/?method=email-test', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    assert '<h3>Total: 4</h3>' in text
    assert text.count('<td>4</td>') == 1
    assert text.count('<td>0</td>') > 5  # to allow statuses to change


async def test_list(cli, send_email):
    # make sure at least two messages are sent
    await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'xx@t.com'}])
    await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'xy@t.com'}])

    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get('/admin/list/?method=email-test', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    # print(text)
    m = re.search('<h3>Total: (\d+)</h3>', text)
    assert m, text
    send_count = int(m.groups()[0])
    assert send_count > 1
    assert '<td>xx@t.com</td>' in text


async def test_details(cli, send_email):
    message_id = await send_email()

    await cli.server.app['es'].get('messages/_refresh')

    r = await cli.get(f'/admin/get/email-test/{message_id}/', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    # print(text)
    assert f'<h2>Message {message_id}</h2>' in text
