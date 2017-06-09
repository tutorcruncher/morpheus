#!/usr/bin/env python3.6
import base64
import json
import os
import re
import uuid
from datetime import datetime
from functools import partial
from time import time

import click
import msgpack
import requests
from arq.utils import from_unix_ms, to_unix_ms
from cryptography.fernet import Fernet
from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexers.data import JsonLexer
from pygments.lexers.html import HtmlLexer
from requests.auth import HTTPBasicAuth

hostname = os.getenv('APP_HOST_NAME', 'morpheus.example.com')
root_url = f'https://{hostname}'
root_status_url = root_url.replace('morpheus.', 'morpheus-status.')


def sizeof_fmt(num):
    for unit in ('', 'K', 'M'):
        if abs(num) < 1024.0:
            return '{:3.1f}{}'.format(num, unit)
        num /= 1024.0
    return '{:3.1f}G'.format(num)


def get_data(r):
    try:
        return r.json()
    except ValueError:
        raise RuntimeError(f'response not valid json:\n{r.text}')


formatter = Terminal256Formatter(style='vim')


def print_data(data, fmt='json'):
    if fmt == 'html':
        lexer = HtmlLexer()
    else:
        lexer = JsonLexer()
    if not isinstance(data, str):
        data = json.dumps(data, indent=2)
    print(highlight(data, lexer, formatter))


def print_response(r, *, include=None, exclude=set()):
    data = {
        k: v for k, v in get_data(r).items()
        if k not in exclude and (not include or k in include)
    }
    print_data(data)


def user_auth_headers(user_fernet_key, company):
    key = base64.urlsafe_b64encode(user_fernet_key.encode())
    f = Fernet(key)
    session_data = {
        'company': company,
        'user_id': 123,
        'expires': to_unix_ms(datetime(2020, 1, 1))[0]
    }
    auth_key = f.encrypt(msgpack.packb(session_data, encoding='utf8'))
    return {'Authorization': auth_key}


def style(s, pad=0, limit=1000, **kwargs):
    return click.style(str(s).ljust(pad)[:limit], **kwargs)


green = partial(style, fg='green')
blue = partial(style, fg='blue')
magenta = partial(style, fg='magenta')
yellow = partial(style, fg='yellow')
dim = partial(style, fg='white', dim=True)


def print_messages(data, print_heading=True, show_count=1000, p_from=0):
    if print_heading:
        heading = yellow(
            f'{"ID":6} {"message id":32} {"company":15} {"to":25} {"status":12} {"sent at":20} {"update at":20} '
            f'subject   | Total messages: {data["hits"]["total"]}'
        )
        print(heading)
    messages = data['hits']['hits']
    for message in messages:
        p_from += 1
        if p_from > show_count:
            return None
        source = message['_source']
        sent_ts = from_unix_ms(source['send_ts']).strftime('%a %Y-%m-%d %H:%M')
        update_ts = from_unix_ms(source['update_ts']).strftime('%a %Y-%m-%d %H:%M')

        score = message["_score"]
        if score is None:
            score = f'{p_from:6}'
        else:
            score = f'{score:6.3f}'
        print(f'{score} '
              f'{blue(message["_id"], 32)} '
              f'{magenta(source["company"], 15)} '
              f'{green(source["to_email"], 25, 25)} '
              f'{magenta(source["status"], 12)} '
              f'{green(sent_ts, 20)} '
              f'{yellow(update_ts, 20)} '
              f'{source["subject"]:.40}')
    return p_from


@click.group()
@click.pass_context
def cli(ctx):
    """
    Run morpheus CLI.
    """
    pass


@cli.command()
@click.option('--username', envvar='BASIC_USERNAME')
@click.option('--password', envvar='BASIC_PASSWORD')
@click.option('--user-fernet-key', envvar='APP_USER_FERNET_KEY')
@click.option('--company', default='__all__')
@click.option('--send-method', default='email-mandrill')
def status(username, password, user_fernet_key, company, send_method):
    r = requests.get(root_url)
    assert r.status_code == 200, (r.status_code, r.text)
    print(*re.search('^ *(COMMIT: .+)', r.text, re.M).groups())
    print(*re.search('^ *(RELEASE DATE: .+)', r.text, re.M).groups())

    auth = HTTPBasicAuth(username, password)
    r = requests.get(f'{root_status_url}/api/2/all', auth=auth)
    if r.status_code == 401:
        print(f'authentication with username={username}, password={password} failed')
        exit(1)

    assert r.status_code == 200, (r.status_code, r.text)
    data = get_data(r)
    print('CPU:      {cpu[total]:0.2f}%'.format(**data))
    print('Memory:   {mem[percent]:0.2f}% {v}'.format(v=sizeof_fmt(data['mem']['used']), **data))
    print('Uptime:   {uptime}'.format(**data))
    print('Docker Containers:')
    for c in data['docker']['containers']:
        print('  {name:20} {Status:15} mem: {v:6} CPU: {cpu[total]:0.2f}%'.format(
            v=sizeof_fmt(c['memory']['usage']), **c))
    print('File System:')
    for c in data['fs']:
        print('  {device_name:10} {mnt_point:20} {fs_type:6} used: {v:6} {percent:0.2f}%'.format(
            v=sizeof_fmt(c['used']), **c))

    r = requests.get(
        f'{root_url}/user/{send_method}/aggregation/',
        headers=user_auth_headers(user_fernet_key, company),
    )
    assert r.status_code == 200, (r.status_code, r.text)
    data = get_data(r)
    # print_data(data)
    data = data['aggregations']['_']
    print('Total emails send: {doc_count}'.format(**data))
    for period in data['_']['buckets']:
        opens = {}
        for k, v in period.items():
            if isinstance(v, dict):
                opens[k] = v.get('doc_count')
        opens = ' '.join(f'{k}={str(v):5}' for k, v in sorted(opens.items()))
        dt = datetime.strptime(period['key_as_string'][:10], '%Y-%m-%d')
        print('{dt:%a %Y-%m-%d}   {opens}'.format(dt=dt, opens=opens))


@cli.command()
@click.option('--user-fernet-key', envvar='APP_USER_FERNET_KEY')
@click.option('--company', default='__all__')
@click.option('--send-method', default='email-mandrill')
@click.option('--count', 'show_count', default=50, type=int)
def list(user_fernet_key, company, send_method, show_count):
    p_from = 0
    for i in range(100):
        r = requests.get(
            f'{root_url}/user/{send_method}/?from={p_from}&size=50',
            headers=user_auth_headers(user_fernet_key, company),
        )
        assert r.status_code == 200, (r.status_code, r.text)
        data = get_data(r)
        # print_data(data)
        if not data['hits']['hits']:
            return
        p_from = print_messages(data, i == 0, show_count, p_from)
        if not p_from:
            return


@cli.command()
@click.argument('message_id')
@click.option('--user-fernet-key', envvar='APP_USER_FERNET_KEY')
@click.option('--company', default='__all__')
@click.option('--send-method', default='email-mandrill')
def get(message_id, user_fernet_key, company, send_method):
    r = requests.get(
        f'{root_url}/user/{send_method}/?message_id={message_id}',
        headers=user_auth_headers(user_fernet_key, company),
    )
    assert r.status_code == 200, (r.status_code, r.text)
    data = get_data(r)
    print_data(data)


@cli.command()
@click.argument('search-query')
@click.option('--user-fernet-key', envvar='APP_USER_FERNET_KEY')
@click.option('--company', default='__all__')
@click.option('--send-method', default='email-mandrill')
def search(search_query, user_fernet_key, company, send_method):
    r = requests.get(
        f'{root_url}/user/{send_method}/?q={search_query}',
        headers=user_auth_headers(user_fernet_key, company),
    )
    assert r.status_code == 200, (r.status_code, r.text)
    data = get_data(r)
    # print_data(data['hits']['hits'][:5])
    print_messages(data)


@cli.command()
@click.argument('recipient_email')
@click.option('--recipient-first-name', default='John {}')
@click.option('--recipient-last-name', default='Doe')
@click.option('--subject', default='Morpheus test {{ time }}')
@click.option('--body', type=click.File('r'), required=None)
@click.option('--from', 'efrom', default='Morpheus Testing <testing@example.com>', envvar='SEND_FROM')
@click.option('--attachment', type=click.File('r'), required=None)
@click.option('--auth-key', envvar='APP_AUTH_KEY')
@click.option('--company', default='testing')
@click.option('--recipient-count', default=1)
@click.option('--send-method', default='email-mandrill')
def send(recipient_email,
         recipient_first_name,
         recipient_last_name,
         subject,
         body,
         efrom,
         attachment,
         auth_key,
         company,
         recipient_count,
         send_method):
    uid = str(uuid.uuid4())
    if body:
        body = body.read()
    else:
        body = """\
# Testing Morpheus

This is a **test** at {{ time }}.
"""

    if attachment:
        attachments = [
            {
                'name': 'Invoice INV-123',
                'html': attachment.read(),
            }
        ]
    else:
        attachments = []

    data = {
        'uid': uid,
        'markdown_template': body,
        'company_code': company,
        'from_address': efrom,
        'method': send_method,
        'subject_template': subject,
        'context': {
            'time': datetime.now().strftime('%a %Y-%m-%d %H:%M')
        },
        'recipients': [
            {
                'first_name': recipient_first_name.format(i),
                'last_name': recipient_last_name.format(i),
                'address': recipient_email.format(i),
                'pdf_attachments': attachments
            }
            for i in range(recipient_count)
        ]
    }
    print_data(data)
    start = time()
    r = requests.post(
        f'{root_url}/send/',
        data=json.dumps(data),
        headers={'Authorization': auth_key}
    )
    assert r.status_code == 201, (r.status_code, r.text)
    print(f'time taken: {time() - start:0.3f}')
    if recipient_count == 1:
        print(f'email sent:\n{r.text}')
    else:
        print(f'{recipient_count} emails sent:\n{r.text}')


if __name__ == '__main__':
    cli()


