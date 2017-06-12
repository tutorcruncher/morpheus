import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
from datetime import datetime
from html import escape

import chevron
import msgpack
from aiohttp.web import HTTPBadRequest, HTTPConflict, HTTPForbidden, Response
from arq.utils import from_unix_ms
from pydantic.datetime_parse import parse_datetime
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer

from .models import MandrillSingleWebhook, MandrillWebhook, MessageStatus, SendMethod, SendModel
from .utils import THIS_DIR, AdminView, ApiError, ServiceView, UserView, View

logger = logging.getLogger('morpheus.web')


async def index(request):
    template = (THIS_DIR / 'extra/index.html').read_text()
    settings = request.app['settings']
    ctx = {k: escape(v) for k, v in settings.values(include=('commit', 'release_date')).items()}
    return Response(text=chevron.render(template, data=ctx), content_type='text/html')


class SendView(ServiceView):
    async def call(self, request):
        m: SendModel = await self.request_data(SendModel)
        async with await self.sender.get_redis_conn() as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.uid}" already exists\n')
            recipients_key = f'recipients:{m.uid}'
            data = m.values(exclude={'recipients', 'from_address'})
            data.update(
                from_email=m.from_address.email,
                from_name=m.from_address.name,
            )
            pipe = redis.pipeline()
            pipe.lpush(recipients_key, *[msgpack.packb(r.values(), use_bin_type=True) for r in m.recipients])
            pipe.expire(group_key, 86400)
            pipe.expire(recipients_key, 86400)
            await pipe.execute()
            await self.sender.send(recipients_key, **data)
        return Response(text='201 job enqueued\n', status=201)


MSG_FIELDS = (
    'bounce_description',
    'clicks',
    'diag',
    'reject',
    'opens',
    'resends',
    'smtp_events',
    'state',
)


class GeneralWebhookView(View):
    es_type = None

    async def update_message_status(self, m: MandrillSingleWebhook):
            update_uri = f'messages/{self.es_type}/{m.message_id}/_update'
            try:
                await self.app['es'].post(update_uri, doc={'update_ts': m.ts, 'status': m.event})
            except ApiError as e:
                if e.status == 404:
                    # we still return 200 here to avoid mandrill repeatedly trying to send the event
                    logger.warning('no message found for %s, ts: %s, status: %s', m.message_id, m.ts, m.event,
                                   extra={'data': m.values()})
                    return
                else:
                    raise
            logger.info('updating message %s, ts: %s, status: %s', m.message_id, m.ts, m.event)
            await self.app['es'].post(
                update_uri,
                script={
                    'lang': 'painless',
                    'inline': 'ctx._source.events.add(params.event)',
                    'params': {
                        'event': {
                            'ts': m.ts,
                            'status': m.event,
                            'extra': {
                                'user_agent': m.user_agent,
                                'location': m.location,
                                **{f: m.msg.get(f) for f in MSG_FIELDS},
                            },
                        }
                    }
                }
            )


class TestWebhookView(GeneralWebhookView):
    """
    Simple view to update messages faux-sent with email-test
    """
    es_type = 'email-test'

    async def call(self, request):
        m: MandrillSingleWebhook = await self.request_data(MandrillSingleWebhook)
        await self.update_message_status(m)
        return Response(text='message status updated\n')


class MandrillWebhookView(GeneralWebhookView):
    """
    Update messages sent with mandrill
    """
    es_type = 'email-mandrill'

    async def call(self, request):
        try:
            event_data = (await request.post())['mandrill_events']
        except KeyError:
            raise HTTPBadRequest(text='"mandrill_events" not found in post data')

        sig_generated = base64.b64encode(
            hmac.new(
                self.app['webhook_auth_key'],
                msg=(self.app['mandrill_webhook_url'] + 'mandrill_events' + event_data).encode(),
                digestmod=hashlib.sha1
            ).digest()
        )
        sig_given = request.headers.get('X-Mandrill-Signature', '<missing>').encode()
        if not hmac.compare_digest(sig_generated, sig_given):
            raise HTTPForbidden(text='invalid signature')
        try:
            events = json.loads(event_data)
        except ValueError as e:
            raise HTTPBadRequest(text=f'invalid json data: {e}')

        coros = [self.update_message_status(m) for m in MandrillWebhook(events=events).events]
        await asyncio.gather(*coros)
        return Response(text='message status updated\n')


class UserMessageView(UserView):
    """
    List or search of messages for an authenticated user
    """
    async def call(self, request):
        es_query = {
            'query': {
                'bool': {
                    'filter': [
                        {'match_all': {}}
                        if self.session.company == '__all__' else
                        {'term': {'company': self.session.company}},
                    ]
                }
            },
            'from': self.get_arg_int('from', 0),
            'size': self.get_arg_int('size', 10),
        }
        message_id = request.GET.get('message_id')
        query = request.GET.get('q')
        if message_id:
            es_query['query']['bool']['filter'].append(
                {'term': {'_id': message_id}},
            )
        elif query:
            es_query['query']['bool']['should'] = [
                {'simple_query_string': {
                    'query': query,
                    'fields': ['to_*^3', 'subject^2', '_all'],
                    'lenient': True,
                }}
            ]
            es_query['min_score'] = 2
        else:
            es_query['sort'] = [
                {'send_ts': 'desc'},
            ]

        r = await self.app['es'].get(
            'messages/{[method]}/_search?filter_path=hits'.format(request.match_info), **es_query
        )
        return Response(body=await r.text(), content_type='application/json')


class UserAggregationView(UserView):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    def filter(self, interval='day'):
        return {
            'aggs': {
                '_': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'match_all': {}} if self.session.company == '__all__' else
                                {'term': {'company': self.session.company}},
                            ]
                        }
                    },
                    # TODO allow more filtering here, filter to last X days.
                    'aggs': {
                        '_': {
                            'date_histogram': {
                                'field': 'send_ts',
                                'interval': interval
                            },
                            'aggs': {
                                status: {
                                    'filter': {
                                        'term': {
                                            'status': status
                                        }
                                    }
                                } for status in MessageStatus
                            },
                        }
                    }
                }
            }
        }

    async def call(self, request):
        r = await self.app['es'].get(
            'messages/{[method]}/_search?size=0&filter_path=aggregations'.format(request.match_info),
            **self.filter()
        )
        return Response(body=await r.text(), content_type='application/json')


class UserTaggedMessageView(UserView):
    # TODO this should get  built in to UserMessageView
    async def call(self, request):
        r = await self.app['es'].get(
            'messages/{[method]}/_search?filter_path=hits'.format(request.match_info),
            query={
                'bool': {
                    'filter': [
                        {'match_all': {}}
                        if self.session.company == '__all__' else
                        {'term': {'company': self.session.company}},
                    ] + [
                        {'term': {'tags': t}} for t in request.GET.get('q')
                    ]
                }
            },
            sort={'update_ts': {'order': 'desc'}}
        )
        return Response(body=await r.text(), content_type='application/json')


class AdminAggregatedView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill)
        url = self.app.router['user-aggregation'].url_for(method=method)

        r = await morpheus_api.get(url)
        data = await r.json()
        data = data['aggregations']['_']
        headings = ['date'] + sorted(k for k, v in data['_']['buckets'][0].items() if isinstance(v, dict))
        table_body = []
        for period in reversed(data['_']['buckets']):
            row = [datetime.strptime(period['key_as_string'][:10], '%Y-%m-%d').strftime('%a %Y-%m-%d')]
            for h in headings[1:]:
                row.append(period[h].get('doc_count') or '0')
            table_body.append(row)

        return dict(
            total=data['doc_count'],
            table_headings=headings,
            table_body=table_body,
            sub_heading=f'Aggregated {method} data'
        )


class AdminListView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill)
        query = dict(size=100)
        search = self.request.query.get('search')
        search and query.update(q=search)
        url = self.app.router['user-messages'].url_for(method=method).with_query(query)

        r = await morpheus_api.get(url)
        data = await r.json()

        headings = ['Score', 'message id', 'company', 'to', 'status', 'sent at', 'updated at', 'subject']
        table_body = []
        for i, message in enumerate(data['hits']['hits']):
            score, source = message['_score'], message['_source']
            table_body.append([
                str(i) if score is None else f'{score:6.3f}',
                f'<a href="/admin/get/{message["_id"]}/" class="short">{message["_id"]}</a>',
                source['company'],
                source['to_email'],
                source['status'],
                from_unix_ms(source['send_ts']).strftime('%a %Y-%m-%d %H:%M'),
                from_unix_ms(source['update_ts']).strftime('%a %Y-%m-%d %H:%M'),
                source['subject'],
            ])
        return dict(
            total=data['hits']['total'],
            table_headings=headings,
            table_body=table_body,
            sub_heading=f'List {method} messages',
            search=search,
        )


class AdminGetView(AdminView):
    @staticmethod
    def replace_data(m):
        dt = parse_datetime(m.group())
        # WARNING: this means the output is not valid json, but is more readable
        return f'{m.group()} ({dt:%a %Y-%m-%d %H:%M})'

    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill)
        message_id = self.request.match_info['id']
        url = self.app.router['user-messages'].url_for(method=method).with_query({'message_id': message_id})

        r = await morpheus_api.get(url)
        data = await r.json()
        data = json.dumps(data, indent=2)
        data = re.sub('14\d{8,11}', self.replace_data, data)
        return dict(
            sub_heading=f'Message {message_id}',
            extra=highlight(data, JsonLexer(), HtmlFormatter(style='vim'))
        )
