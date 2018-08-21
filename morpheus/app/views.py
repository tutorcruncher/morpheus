import base64
import hashlib
import hmac
import json
import logging
import re
from datetime import datetime
from html import escape
from itertools import product
from operator import itemgetter
from statistics import mean, stdev
from time import time

import msgpack
import pytz
import ujson
from aiohttp.web import HTTPBadRequest, HTTPConflict, HTTPForbidden, HTTPNotFound, HTTPTemporaryRedirect, Response
from aiohttp_jinja2 import template
from arq.utils import from_unix_ms, truncate
from markupsafe import Markup
from pydantic.datetime_parse import parse_datetime
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer

from .models import (EmailSendModel, MandrillSingleWebhook, MessageBirdWebHook, MessageStatus, SendMethod,
                     SmsNumbersModel, SmsSendModel, SubaccountModel)
from .utils import Mandrill  # noqa
from .utils import AdminView, AuthView, ServiceView, TemplateView, UserView, View

logger = logging.getLogger('morpheus.web')


@template('index.jinja')
async def index(request):
    settings = request.app['settings']
    return {k: escape(v) for k, v in settings.dict(include=('commit', 'release_date')).items()}


class ClickRedirectView(TemplateView):
    template = 'not-found.jinja'

    async def call(self, request):
        token = request.match_info['token'].rstrip('.')
        r = await self.app['es'].get(
            f'links/_search?filter_path=hits',
            query={
                'bool': {
                    'filter': [
                        {'term': {'token': token}},
                        {'range': {'expires_ts': {'gte': 'now'}}},
                    ]
                }
            },
            size=1,
        )
        data = await r.json()
        arg_url = request.query.get('u')
        if arg_url:
            try:
                arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
            except ValueError:
                arg_url = None

        if data['hits']['total']:
            hit = data['hits']['hits'][0]
            source = hit['_source']
            ip_address = request.headers.get('X-Forwarded-For')
            if ip_address:
                ip_address = ip_address.split(',', 1)[0]

            try:
                ts = float(request.headers.get('X-Request-Start', '.'))
            except ValueError:
                ts = time()
            await self.sender.store_click(
                target=source['url'],
                ip=ip_address,
                user_agent=request.headers.get('User-Agent'),
                ts=ts,
                send_method=source['send_method'],
                send_message_id=source['send_message_id']
            )
            url = source['url']
            if arg_url and arg_url != url:
                logger.warning('db url does not match arg url: "%s" !+ "%s"', url, arg_url)
            raise HTTPTemporaryRedirect(location=url)
        elif arg_url:
            logger.warning('no url found, using arg url "%s"', arg_url)
            raise HTTPTemporaryRedirect(location=arg_url)
        else:
            return dict(
                url=request.url,
                http_status_=404,
            )


class EmailSendView(ServiceView):
    async def call(self, request):
        m = await self.request_data(EmailSendModel)
        redis_pool = await request.app['sender'].get_redis()
        with await redis_pool as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.uid}" already exists\n')
            recipients_key = f'recipients:{m.uid}'
            data = m.dict(exclude={'recipients', 'from_address'})
            data.update(
                from_email=m.from_address.email,
                from_name=m.from_address.name,
            )
            pipe = redis.pipeline()
            pipe.lpush(recipients_key, *[msgpack.packb(r.dict(), use_bin_type=True) for r in m.recipients])
            pipe.expire(group_key, 86400)
            pipe.expire(recipients_key, 86400)
            await pipe.execute()
            await self.sender.send_emails(recipients_key, **data)
            logger.info('%s sending %d emails', m.company_code, len(m.recipients))
        return Response(text='201 job enqueued\n', status=201)


class SmsSendView(ServiceView):
    async def call(self, request):
        m = await self.request_data(SmsSendModel)
        spend = None
        redis_pool = await request.app['sender'].get_redis()
        with await redis_pool as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise HTTPConflict(text=f'Send group with id "{m.uid}" already exists\n')
            if m.cost_limit is not None:
                spend = await self.sender.check_sms_limit(m.company_code)
                if spend >= m.cost_limit:
                    return self.json_response(
                        status='send limit exceeded',
                        cost_limit=m.cost_limit,
                        spend=spend,
                        status_=402,
                    )
            recipients_key = f'recipients:{m.uid}'
            data = m.dict(exclude={'recipients'})
            pipe = redis.pipeline()
            pipe.lpush(recipients_key, *[msgpack.packb(r.dict(), use_bin_type=True) for r in m.recipients])
            pipe.expire(group_key, 86400)
            pipe.expire(recipients_key, 86400)
            await pipe.execute()
            await self.sender.send_smss(recipients_key, **data)
            logger.info('%s sending %d SMSs', m.company_code, len(m.recipients))
        return self.json_response(
            status='enqueued',
            spend=spend,
            status_=201,
        )


class SmsValidateView(ServiceView):
    async def call(self, request):
        m = await self.request_data(SmsNumbersModel)
        result = {str(k): self.to_dict(self.sender.validate_number(n, m.country_code)) for k, n in m.numbers.items()}
        return self.json_response(**result)

    @classmethod
    def to_dict(cls, v):
        return v and dict(v._asdict())


class TestWebhookView(View):
    """
    Simple view to update messages faux-sent with email-test
    """

    async def call(self, request):
        m = await self.request_data(MandrillSingleWebhook)
        await self.sender.update_message_status('email-test', m)
        return Response(text='message status updated\n')


class MandrillWebhookView(View):
    """
    Update messages sent with mandrill
    """

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
            events = ujson.loads(event_data)
        except ValueError as e:
            raise HTTPBadRequest(text=f'invalid json data: {e}')

        await self.sender.update_mandrill_webhooks(events)
        return Response(text='message status updated\n')


class MessageBirdWebhookView(View):
    """
    Update messages sent with message bird
    """
    async def call(self, request):
        # TODO looks like "ts" might be wrong here, appears to always be send time.
        m = MessageBirdWebHook(**request.query)
        await self.sender.update_message_status('sms-messagebird', m)
        return Response(text='message status updated\n')


class CreateSubaccountView(ServiceView):
    """
    Create a new subaccount with mandrill for new sending company
    """
    async def call(self, request) -> Response:
        method = request.match_info['method']
        if method != SendMethod.email_mandrill:
            return Response(text=f'no subaccount creation required for "{method}"\n')

        m = await self.request_data(SubaccountModel)
        mandrill: Mandrill = self.app['mandrill']

        r = await mandrill.post(
            'subaccounts/add.json',
            id=m.company_code,
            name=m.company_name,
            allowed_statuses=(200, 500),
            timeout_=12,
        )
        data = await r.json()
        if r.status == 200:
            return Response(text='subaccount created\n', status=201)

        assert r.status == 500, r.status
        if f'A subaccount with id {m.company_code} already exists' not in data.get('message', ''):
            return Response(text=f'error from mandrill: {json.dumps(data, indent=2)}\n', status=400)

        r = await mandrill.get('subaccounts/info.json', id=m.company_code, timeout_=12)
        data = await r.json()
        total_sent = data['sent_total']
        if total_sent > 100:
            return Response(text=f'subaccount already exists with {total_sent} emails sent, '
                                 f'reuse of subaccount id not permitted\n', status=409)
        else:
            return Response(text=f'subaccount already exists with only {total_sent} emails sent, '
                                 f'reuse of subaccount id permitted\n')


class _UserMessagesView(UserView):
    es_from = True

    def _strftime(self, ts):
        dt_tz = self.request.query.get('dttz') or 'utc'
        try:
            dt_tz = pytz.timezone(dt_tz)
        except pytz.UnknownTimeZoneError:
            raise HTTPBadRequest(text=f'unknown timezone: "{dt_tz}"')

        dt_fmt = self.request.query.get('dtfmt') or '%a %Y-%m-%d %H:%M'
        return from_unix_ms(ts, 0).astimezone(dt_tz).strftime(dt_fmt)

    async def query(self, *, message_id=None, tags=None, query=None):
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
            'from': self.get_arg_int('from', 0) if self.es_from else 0,
            'size': 100,
        }
        if message_id:
            es_query['query']['bool']['filter'].append({'term': {'_id': message_id}})
        elif query:
            es_query['query']['bool']['should'] = [
                {'simple_query_string': {
                    'query': query,
                    'fields': ['to_*^3', 'subject^2', '_all'],
                    'lenient': True,
                }}
            ]
            es_query['min_score'] = 0.01
        elif tags:
            es_query['query']['bool']['filter'] += [{'term': {'tags': t}} for t in tags]
        else:
            es_query['sort'] = [
                {'send_ts': 'desc'},
            ]

        r = await self.app['es'].get(
            f'messages/{self.request.match_info["method"]}/_search?filter_path=hits', **es_query
        )
        assert r.status == 200, r.status
        return await r.json()

    @staticmethod
    def _event_data(e):
        data = e['_source']
        data.pop('message')
        return data

    async def insert_events(self, data):
        t = self.request.match_info['method']
        for hit in data['hits']['hits']:
            r = await self.app['es'].get(
                f'events/{t}/_search?filter_path=hits',
                query={
                    'term': {'message': hit['_id']}
                },
                size=100,
            )
            assert r.status == 200, r.status
            event_data = await r.json()
            hit['_source']['events'] = [self._event_data(e) for e in event_data['hits']['hits']]


class UserMessagesJsonView(_UserMessagesView):
    async def call(self, request):
        data = await self.query(
            message_id=request.query.get('message_id'),
            tags=request.query.getall('tags', None),
            query=request.query.get('q')
        )
        if 'sms' in request.match_info['method'] and self.session.company != '__all__':
            data['spend'] = await self.sender.check_sms_limit(self.session.company)

        await self.insert_events(data)
        return self.json_response(**data)


class UserMessageDetailView(TemplateView, _UserMessagesView):
    template = 'user/details.jinja'
    es_from = False

    async def call(self, request):
        msg_id = self.request.match_info['id']
        data = await self.query(message_id=msg_id)
        await self.insert_events(data)
        if len(data['hits']['hits']) == 0:
            raise HTTPNotFound(text='message not found')
        data = data['hits']['hits'][0]

        preview_path = self.app.router['user-preview'].url_for(**self.request.match_info)
        return dict(
            base_template='user/base-{}.jinja'.format('raw' if self.request.query.get('raw') else 'page'),
            title='{_type} - {_id}'.format(**data),
            id=data['_id'],
            method=data['_type'],
            details=self._details(data),
            events=list(self._events(data)),
            preview_url=self.full_url(f'{preview_path}?{self.request.query_string}'),
            attachments=list(self._attachments(data)),
        )

    def _details(self, data):
        yield 'ID', data['_id']
        source = data['_source']
        yield 'Status', source['status'].title()

        dst = f'{source["to_first_name"] or ""} {source["to_last_name"] or ""} <{source["to_address"]}>'.strip(' ')
        link = source.get('to_user_link')
        if link:
            yield 'To', dict(
                href=link,
                value=dst,
            )
        else:
            yield 'To', dst

        yield 'Subject', source.get('subject')
        yield 'Send Time', self._strftime(source['send_ts'])
        yield 'Last Updated', self._strftime(source['update_ts'])

    def _attachments(self, data):
        for a in data['_source'].get('attachments', []):
            name = None
            try:
                doc_id, name = a.split('::')
                doc_id = int(doc_id)
            except ValueError:
                yield '#', name or a
            else:
                yield f'/attachment-doc/{doc_id}/', name

    def _events(self, data):
        events = sorted(data['_source'].get('events', []), key=itemgetter('ts'), reverse=True)
        for event in events[:50]:
            yield dict(
                status=event['status'].title(),
                datetime=self._strftime(event['ts']),
                details=Markup(json.dumps(event['extra'], indent=2)),
            )
        if len(events) > 50:
            yield dict(
                status=f'{len(events) - 50} more',
                datetime='...',
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2))
            )


class UserMessageListView(TemplateView, _UserMessagesView):
    template = 'user/list.jinja'

    async def call(self, request):
        data = await self.query(
            tags=request.query.getall('tags', None),
            query=request.query.get('q', None)
        )
        total_sms_spend = None
        if 'sms' in request.match_info['method'] and self.session.company != '__all__':
            total_sms_spend = '{:,.3f}'.format(await self.sender.check_sms_limit(self.session.company))
        hits = data['hits']['hits']
        headings = ['To', 'Send Time', 'Status', 'Subject']
        total = data['hits']['total']
        size = 100
        offset = self.get_arg_int('from', 0)
        pagination = {}
        if len(hits) == size:
            next_offset = offset + size
            pagination['next'] = dict(
                href=f'?from={next_offset}',
                pfrom=next_offset,
                text=f'{next_offset + 1} - {min(next_offset + size, total)}'
            )
        if offset:
            previous_offset = offset - size
            pagination['previous'] = dict(
                href=f'?from={previous_offset}',
                pfrom=previous_offset,
                text=f'{previous_offset + 1} - {max(offset, 0)}'
            )

        return dict(
            base_template='user/base-{}.jinja'.format('raw' if self.request.query.get('raw') else 'page'),
            title=f'{self.request.match_info["method"]} - {total}',
            total=total,
            total_sms_spend=total_sms_spend,
            table_headings=headings,
            table_body=self._table_body(hits),
            pagination=pagination,
        )

    def _table_body(self, hits):
        for msg in hits:
            msg_source = msg['_source']
            subject = msg_source.get('subject') or msg_source.get('body', '')
            yield [
                {
                    'href': msg['_id'],
                    'text': msg_source['to_address'],
                },
                self._strftime((msg_source['send_ts'])),
                msg_source['status'].title(),
                truncate(subject, 40),
            ]


class UserMessagePreviewView(TemplateView, UserView):
    """
    preview a message
    """
    template = 'user/preview.jinja'

    async def call(self, request):
        es_query = {
            'bool': {
                'filter': [
                    {'match_all': {}}
                    if self.session.company == '__all__' else
                    {'term': {'company': self.session.company}},
                ] + [
                    {'term': {'_id': request.match_info['id']}}
                ]
            }
        }
        method = request.match_info['method']
        r = await self.app['es'].get(
            f'messages/{method}/_search?filter_path=hits', query=es_query
        )
        data = await r.json()
        if data['hits']['total'] != 1:
            raise HTTPNotFound(text='message not found')
        source = data['hits']['hits'][0]['_source']
        body = source['body']
        if method.startswith('sms'):
            # need to render the sms so it makes sense to users
            return {
                'from': source['from_name'],
                'to': source['to_last_name'] or source['to_address'],
                'status': source['status'],
                'message': body,
                'extra': source.get('extra') or {},
            }
        else:
            return {'raw': body}


class UserAggregationView(UserView):
    """
    Aggregated sends and opens over time for an authenticated user
    """
    async def call(self, request):
        # TODO allow more filtering here, filter to last X days.
        r = await self.app['es'].get(
            'messages/{[method]}/_search?size=0&filter_path=hits.total,aggregations'.format(request.match_info),
            query={
                'bool': {
                    'filter': [
                        {'match_all': {}} if self.session.company == '__all__' else
                        {'term': {'company': self.session.company}},
                        {
                            'range': {'send_ts': {'gte': 'now-90d'}}
                        }
                    ]
                }
            },
            aggs={
                'histogram': {
                    'date_histogram': {
                        'field': 'send_ts',
                        'interval': 'day'
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
                },
                'all_opened': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'term': {'status': MessageStatus.open}}
                            ]
                        }
                    }
                },
                '7_days_all': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'range': {'send_ts': {'gte': 'now-7d'}}}
                            ]
                        }
                    }
                },
                '7_days_opened': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'range': {'send_ts': {'gte': 'now-7d'}}},
                                {'term': {'status': MessageStatus.open}}
                            ]
                        }
                    }
                },
                '28_days_all': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'range': {'send_ts': {'gte': 'now-28d'}}}
                            ]
                        }
                    }
                },
                '28_days_opened': {
                    'filter': {
                        'bool': {
                            'filter': [
                                {'range': {'send_ts': {'gte': 'now-28d'}}},
                                {'term': {'status': MessageStatus.open}}
                            ]
                        }
                    }
                }
            }
        )
        return Response(body=await r.text(), content_type='application/json')


class AdminAggregatedView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill)
        url = self.app.router['user-aggregation'].url_for(method=method)

        r = await morpheus_api.get(url)
        data = await r.json()
        bucket_data = data['aggregations']['histogram']['buckets']
        # ignore "click" and "unsub"
        headings = ['date', 'deferral', 'send', 'open', 'reject', 'soft_bounce', 'hard_bounce', 'spam', 'open rate']
        was_sent_statuses = 'send', 'open', 'soft_bounce', 'hard_bounce', 'spam', 'click'
        table_body = []
        for period in reversed(bucket_data):
            row = [datetime.strptime(period['key_as_string'][:10], '%Y-%m-%d').strftime('%a %Y-%m-%d')]
            row += [period[h]['doc_count'] or '0' for h in headings[1:-1]]
            was_sent = sum(period[h]['doc_count'] or 0 for h in was_sent_statuses)
            opened = period['open']['doc_count']
            if was_sent > 0:
                row.append(f'{opened / was_sent * 100:0.2f}%')
            else:
                row.append(f'{0:0.2f}%')
            table_body.append(row)
        return dict(
            total=data['hits']['total'],
            table_headings=headings,
            table_body=table_body,
            sub_heading=f'Aggregated {method} data',
        )


class AdminListView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill)
        offset = int(self.request.query.get('offset', '0'))
        search = self.request.query.get('search', '')
        tags = self.request.query.get('tags', '')
        query = {
            'size': 100,
            'from': offset,
            'q': search,
        }
        # tags is a list so has to be processed separately
        if tags:
            query['tags'] = tags
        url = self.app.router['user-messages'].url_for(method=method).with_query(query)

        r = await morpheus_api.get(url)
        data = await r.json()

        headings = ['score', 'message id', 'company', 'to', 'status', 'sent at', 'updated at', 'subject']
        table_body = []
        for i, message in enumerate(data['hits']['hits']):
            score, source, id = message['_score'], message['_source'], message['_id']
            subject = source.get('subject') or source.get('body', '')[:50]
            table_body.append([
                str(i + 1 + offset) if score is None else f'{score:6.3f}',
                {
                    'href': self.app.router['admin-get'].url_for(method=method, id=id),
                    'text': id,
                },
                source['company'],
                source['to_address'],
                source['status'],
                from_unix_ms(source['send_ts']).strftime('%a %Y-%m-%d %H:%M'),
                from_unix_ms(source['update_ts']).strftime('%a %Y-%m-%d %H:%M'),
                Markup(f'<span class="subject">{subject}</span>'),
            ])

        if len(data['hits']['hits']) == 100:
            next_offset = offset + 100
            query = {
                'method': method,
                'search': search,
                'tags': tags,
                'offset': next_offset,
            }
            next_page = dict(
                href=self.app.router['admin-list'].url_for().with_query(query),
                text=f'Next: {next_offset} - {next_offset + 100}'
            )
        else:
            next_page = None
        user_list_path = morpheus_api.modify_url(self.app.router['user-message-list'].url_for(method=method))
        return dict(
            total=data['hits']['total'],
            table_headings=headings,
            table_body=table_body,
            sub_heading=f'List {method} messages',
            search=search,
            tags=tags,
            next_page=next_page,
            user_list_url=self.full_url(user_list_path),
        )


class AdminGetView(AdminView):
    template = 'admin-get.jinja'

    @staticmethod
    def replace_data(m):
        dt = parse_datetime(m.group())
        # WARNING: this means the output is not valid json, but is more readable
        return f'{m.group()} ({dt:%a %Y-%m-%d %H:%M})'

    async def get_context(self, morpheus_api):
        method = self.request.match_info['method']
        message_id = self.request.match_info['id']
        url = self.app.router['user-messages'].url_for(method=method).with_query({'message_id': message_id})

        r = await morpheus_api.get(url)
        data = await r.json()
        data = json.dumps(data, indent=2)
        data = re.sub('14\d{8,11}', self.replace_data, data)

        preview_path = morpheus_api.modify_url(self.app.router['user-preview'].url_for(method=method, id=message_id))
        deets_path = morpheus_api.modify_url(self.app.router['user-message-get'].url_for(method=method, id=message_id))
        return dict(
            sub_heading=f'Message {message_id}',
            preview_url=self.full_url(preview_path),
            details_url=self.full_url(deets_path),
            json_display=highlight(data, JsonLexer(), HtmlFormatter()),
            form_action=self.app.router['admin-list'].url_for(),
        )


class RequestStatsView(AuthView):
    auth_token_field = 'stats_token'

    @classmethod
    def process_values(cls, request_count, request_list):
        groups = {k: {'request_count': int(v.decode())} for k, v in request_count.items()}

        for v in request_list:
            k, time_ = v.rsplit(b':', 1)
            time_ = float(time_.decode()) / 1000
            g = groups.get(k)
            if g:
                if 'times' in g:
                    g['times'].append(time_)
                else:
                    g['times'] = [time_]
            else:
                groups[k] = {'times': [time_]}

        data = []
        for k, v in groups.items():
            method, status = k.decode().split(':')
            v.update(
                method=method,
                status=status + 'XX'
            )
            times = v.pop('times', None)
            if times:
                times = sorted(times)
                times_count = len(times)
                v.update(
                    time_min=times[0],
                    time_max=times[-1],
                    time_mean=mean(times),
                    request_count_interval=times_count,
                )
                if times_count > 2:
                    v.update(
                        time_stdev=stdev(times),
                        time_90=times[int(times_count*0.9)],
                        time_95=times[int(times_count*0.95)],
                    )
            data.append(v)
        return ujson.dumps(data).encode()

    async def call(self, request):
        stats_cache_key = 'request-stats-cache'
        redis_pool = await self.sender.get_redis()
        with await redis_pool as redis:
            response_data = await redis.get(stats_cache_key)
            if not response_data:
                tr = redis.multi_exec()
                tr.hgetall(request.app['stats_request_count'])
                tr.lrange(request.app['stats_request_list'], 0, -1)

                tr.delete(request.app['stats_request_list'])
                request_count, request_list, _ = await tr.execute()
                response_data = self.process_values(request_count, request_list)
                await redis.setex(stats_cache_key, 8, response_data)
        return Response(body=response_data, content_type='application/json')


class MessageStatsView(AuthView):
    auth_token_field = 'stats_token'

    async def call(self, request):
        cache_key = 'message-stats'
        redis_pool = await request.app['sender'].get_redis()
        with await redis_pool as redis:
            response_data = await redis.get(cache_key)
            if not response_data:
                r = await self.app['es'].get(
                    'messages/_search?size=0&filter_path=aggregations',
                    query={
                        'bool': {
                            'filter': {
                                'range': {
                                    'update_ts': {'gte': 'now-10m'}
                                }
                            }
                        }
                    },
                    aggs={
                        f'{method}.{status}': {
                            'aggs': {
                                'age': {
                                    'avg': {
                                        'script': {
                                            'source': 'doc.update_ts.value - doc.send_ts.value'
                                        }
                                    },
                                },
                                # 'event_count': {
                                #     'avg': {
                                #         'field': 'events',
                                #         'script': {
                                #             'inline': '_value.length',
                                #         },
                                #         'missing': 0,
                                #     },
                                # }
                            },
                            'filter': {
                                'bool': {
                                    'filter': [
                                        {'type': {'value': method}},
                                        {'term': {'status': status}},
                                    ]
                                }
                            }
                        } for method, status in product(SendMethod, MessageStatus)
                    }
                )
                data = await r.json()
                result = []
                for k, v in data['aggregations'].items():
                    method, status = k.split('.', 1)
                    result.append(dict(
                        method=method,
                        status=status,
                        count=v['doc_count'],
                        age=round((v['age']['value'] or 0) / 1000),
                        # events=int(v['event_count']['value'] or 0),
                    ))
                response_data = ujson.dumps(result).encode()
                await redis.setex(cache_key, 598, response_data)
        return Response(body=response_data, content_type='application/json')
