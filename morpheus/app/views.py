import asyncio
import json
import logging
from pathlib import Path

import msgpack
from aiohttp.web import HTTPBadRequest, HTTPConflict, HTTPMovedPermanently, Response

from .models import MandrillSingleWebhook, MandrillWebhook, MessageStatus, SendModel
from .utils import ServiceView, UserView, View, ApiError

THIS_DIR = Path(__file__).parent.resolve()
logger = logging.getLogger('morpheus.web')


async def index(request):
    return Response(text=request.app['index_html'], content_type='text/html')

STYLES = (THIS_DIR / 'extra/styles.css').read_text()


async def styles_css(request):
    return Response(text=STYLES, content_type='text/css')


ROBOTS = """\
User-agent: *
Allow: /$
Disallow: /
"""


async def robots_txt(request):
    return Response(text=ROBOTS, content_type='text/plain')


async def favicon(request):
    raise HTTPMovedPermanently('https://secure.tutorcruncher.com/favicon.ico')


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
            events = (await request.post())['mandrill_events']
        except KeyError:
            raise HTTPBadRequest(text='"mandrill_events" not found in post data')

        try:
            events = json.loads(events)
        except ValueError as e:
            raise HTTPBadRequest(text=f'invalid json data: {e}')

        coros = [self.update_message_status(m) for m in MandrillWebhook(events=events).events]
        await asyncio.gather(*coros)
        return Response(text='message status updated\n')


class UserMessageView(UserView):
    async def call(self, request):
        es_query = {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'company': self.session.company}},
                    ]
                }
            }
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
        else:
            es_query['sort'] = {
                'update_ts': {'order': 'desc'}
            }
        r = await self.app['es'].post(
            'messages/{[method]}/_search?filter_path=hits'.format(request.match_info), **es_query
        )
        return Response(body=await r.text(), content_type='application/json')


AGGREGATION_FILTER = {
    'aggs': {
        '_': {
            'filter': {
                'term': {
                    'company': 'foobar'
                }
                # TODO allow more filtering here, filter to last X days.
            },
            'aggs': {
                '_': {
                    'date_histogram': {
                        'field': 'send_ts',
                        'interval': 'day'
                    },
                    'aggs': {
                        'all': {
                            'filter': {
                                'match_all': {}
                            }
                        },
                    }
                }
            }
        }
    }
}
for status in MessageStatus:
    AGGREGATION_FILTER['aggs']['_']['aggs']['_'][status] = {
        'filter': {
            'term': {
                'status': status
            }
        }
    }


class UserAggregationView(UserView):
    async def call(self, request):
        r = await self.app['es'].post(
            'messages/{[method]}/_search?size=0&filter_path=aggregations'.format(request.match_info),
            **AGGREGATION_FILTER
        )
        return Response(body=await r.text(), content_type='application/json')


class UserTaggedMessageView(UserView):
    async def call(self, request):
        r = await self.app['es'].post(
            'messages/{[method]}/_search?filter_path=hits'.format(request.match_info),
            query={
                'bool': {
                    'filter': [
                        {'term': {'company': self.session.company}},
                    ] + [
                        {'term': {'tags': t}} for t in request.GET.get('q')
                    ]
                }
            },
            sort={'update_ts': {'order': 'desc'}}
        )
        return Response(body=await r.text(), content_type='application/json')
