"""Sync httpx.MockTransport routes for Mandrill / Messagebird stand-in."""
import json
import re
import time

import httpx


class DummyState:
    def __init__(self) -> None:
        self.mandrill_subaccounts: dict = {}
        self.log: list = []


def _json(data, status: int = 200) -> httpx.Response:
    return httpx.Response(status_code=status, json=data)


def _text(text: str, status: int = 200) -> httpx.Response:
    return httpx.Response(status_code=status, text=text)


def make_handler(state: DummyState):
    def handler(request: httpx.Request) -> httpx.Response:
        state.log.append(request)
        path = request.url.path
        method = request.method
        body = request.content
        try:
            data = json.loads(body) if body else {}
        except (ValueError, TypeError):
            data = {}

        # Mandrill ---------------------------------------
        if path == '/mandrill/messages/send.json' and method == 'POST':
            message = data.get('message') or {}
            subject = message.get('subject')
            if subject == '__slow__':
                raise httpx.ReadTimeout('simulated timeout', request=request)
            elif subject == '__502__':
                return _text('', 502)
            elif subject == '__500_nginx__':
                return _text('<hr><center>nginx/1.12.2</center>', 500)
            elif subject == '__500__':
                return _text('foobar', 500)

            if data.get('key') != 'good-mandrill-testing-key':
                return _json({'auth': 'failed'}, 403)
            to_email = message['to'][0]['email']
            return _json(
                [
                    {
                        'email': to_email,
                        '_id': re.sub(r'[^a-zA-Z0-9\-]', '', f'mandrill-{to_email}'),
                        'status': 'queued',
                    }
                ]
            )

        if path == '/mandrill/subaccounts/add.json' and method == 'POST':
            if data.get('key') != 'good-mandrill-testing-key':
                return _json({'auth': 'failed'}, 403)
            sa_id = data['id']
            if sa_id == 'broken':
                return _json({'error': 'snap something unknown went wrong'}, 500)
            elif sa_id in state.mandrill_subaccounts:
                return _json({'message': f'A subaccount with id {sa_id} already exists'}, 500)
            state.mandrill_subaccounts[sa_id] = data
            return _json({'message': "subaccount created (this isn't the same response as mandrill)"})

        if path == '/mandrill/subaccounts/delete.json' and method == 'POST':
            if data.get('key') != 'good-mandrill-testing-key':
                return _json({'auth': 'failed'}, 403)
            sa_id = data['id']
            if sa_id == 'broken1' or sa_id not in state.mandrill_subaccounts:
                return _json({'error': 'snap something unknown went wrong'}, 500)
            elif 'name' not in state.mandrill_subaccounts[sa_id]:
                return _json(
                    {'message': f"No subaccount exists with the id '{sa_id}'", 'name': 'Unknown_Subaccount'},
                    500,
                )
            state.mandrill_subaccounts[sa_id] = data
            return _json({'message': "subaccount deleted (this isn't the same response as mandrill)"})

        if path == '/mandrill/subaccounts/info.json' and method == 'GET':
            # GET requests come through as POST in mandrill ApiSession; data is JSON-bodied
            if data.get('key') != 'good-mandrill-testing-key':
                return _json({'auth': 'failed'}, 403)
            sa_id = data.get('id')
            sa_info = state.mandrill_subaccounts.get(sa_id)
            if sa_info:
                return _json({'subaccount_info': sa_info, 'sent_total': 200 if sa_id == 'lots-sent' else 42})
            return _json({}, 200)

        if path == '/mandrill/webhooks/list.json' and method == 'GET':
            return _json(
                [
                    {
                        'url': 'https://example.com/webhook/mandrill/',
                        'auth_key': 'existing-auth-key',
                        'description': 'testing existing key',
                    }
                ]
            )

        if path == '/mandrill/webhooks/add.json' and method == 'POST':
            if 'fail' in (data.get('url') or ''):
                return _text('', 400)
            return _json({'auth_key': 'new-auth-key', 'description': 'testing new key'})

        # Messagebird ------------------------------------
        if path == '/messagebird/messages' and method == 'POST':
            assert request.headers.get('Authorization') == 'AccessKey good-messagebird-testing-key'
            return _json(
                {'id': '6a23b2037595620ca8459a3b00026003', 'recipients': {'totalCount': len(data['recipients'])}},
                201,
            )

        return _text(f'no dummy route for {method} {path}', 404)

    return handler
