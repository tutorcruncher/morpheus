import asyncio
import base64
import hashlib
import hmac
import json
import logging
from pathlib import Path

import msgpack
from aiohttp.web import HTTPBadRequest, HTTPConflict, HTTPForbidden, Response

from .models import MandrillSingleWebhook, MandrillWebhook, MessageStatus, SendModel
from .utils import ApiError, ServiceView, UserView, View

THIS_DIR = Path(__file__).parent.resolve()
logger = logging.getLogger('morpheus.web')


async def index(request):
    return Response(text=request.app['index_html'], content_type='text/html')

STYLES = (THIS_DIR / 'extra/styles.css').read_bytes()
FAVICON = (THIS_DIR / 'extra/favicon.ico').read_bytes()
ROBOTS = """\
User-agent: *
Allow: /$
Disallow: /
"""


async def styles_css(request):
    return Response(body=STYLES, content_type='text/css')


async def robots_txt(request):
    return Response(text=ROBOTS, content_type='text/plain')


async def favicon(request):
    return Response(body=FAVICON, content_type='image/vnd.microsoft.icon')


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
