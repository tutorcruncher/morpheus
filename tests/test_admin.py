import base64
import re
import uuid
from datetime import datetime, timezone


def gen_headers():
    token = base64.b64encode(b'whoever:testing').decode()
    return {'Authorization': f'Basic {token}'}


async def test_admin_root(cli):
    r = await cli.get('/admin/', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    assert (
        '  <ul>\n'
        '    <li><a href="/">index</a></li>\n'
        '    <li><a href="/admin/?method=email-mandrill">admin - aggregated</a></li>\n'
        '    <li><a href="/admin/list/?method=email-mandrill">admin - list</a></li>\n'
        '  </ul>\n'
    ) in text


async def test_aggregates(cli, send_email):
    for i in range(4):
        await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': f'{i}@t.com'}])

    r = await cli.get('/admin/?method=email-test', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    assert '<h3>Total: 4</h3>' in text
    assert text.count('<td>4</td>') == 1
    assert text.count('<td>0</td>') > 5  # to allow statuses to change


async def test_list(cli, send_email, db_conn):
    # make sure at least two messages are sent
    await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': 'xx@t.com'}])
    await send_email(uid=str(uuid.uuid4()), company_code='whoever', recipients=[{'address': 'xy@t.com'}])

    await db_conn.execute('update messages set update_ts=$1, send_ts=$1', datetime(2032, 6, 1, tzinfo=timezone.utc))

    r = await cli.get('/admin/list/?method=email-test', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    m = re.search('<h3>Total: (\d+)</h3>', text)
    assert m, text
    send_count = int(m.groups()[0])
    assert send_count > 1
    msg_id = await db_conn.fetchval('select id from messages where to_address=$1', 'xx@t.com')
    assert f'<td><a href="/admin/get/email-test/{msg_id}/" class="short">xx@t.com</a></td>\n' in text
    assert '<td>Tue 2032-06-01 00:00 UTC</td>' in text
    r = await cli.get('/admin/list/', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    assert '<h3>Total: 0</h3>' in text


async def test_details(cli, send_email, db_conn):
    msg_ext_id = await send_email()

    message_id = await db_conn.fetchval('select id from messages where external_id=$1', msg_ext_id)
    r = await cli.get(f'/admin/get/email-test/{message_id}/', headers=gen_headers())
    text = await r.text()
    assert r.status == 200, text
    # print(text)
    assert f'<h2>Message {message_id}</h2>' in text
