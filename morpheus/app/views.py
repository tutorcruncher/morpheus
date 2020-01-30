import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
from dataclasses import asdict
from datetime import datetime, timezone
from html import escape
from itertools import groupby
from operator import itemgetter
from time import time
from typing import Tuple

import pytz
import ujson
from aiohttp.web import HTTPTemporaryRedirect
from aiohttp_jinja2 import template
from arq.utils import truncate
from asyncpg import Connection
from atoolbox import JsonErrors
from buildpg import Func, Values, Var
from buildpg.asyncpg import BuildPgPool
from buildpg.clauses import Select
from markupsafe import Markup
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer

from .ext import Mandrill
from .models import (
    EmailSendModel,
    MandrillSingleWebhook,
    MessageBirdWebHook,
    SendMethod,
    SmsBillingModel,
    SmsNumbersModel,
    SmsSendModel,
    SubaccountModel,
)
from .utils import AdminView, AuthView, PreResponse, ServiceView, TemplateView, UserView, View
from .worker import validate_number

logger = logging.getLogger('morpheus.web')


@template('index.jinja')
async def index(request):
    settings = request.app['settings']
    return {k: escape(v) for k, v in settings.dict(include={'commit', 'release_date', 'build_time'}).items()}


class ClickRedirectView(TemplateView):
    template = 'not-found.jinja'

    async def call(self, request):
        token = request.match_info['token'].rstrip('.')
        async with self.app['pg'].acquire() as conn:
            link = await conn.fetchrow('select id, url from links where token=$1', token)

        arg_url = request.query.get('u')
        if arg_url:
            try:
                arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
            except ValueError:
                arg_url = None

        if link:
            ip_address = request.headers.get('X-Forwarded-For')
            if ip_address:
                ip_address = ip_address.split(',', 1)[0]

            try:
                ts = float(request.headers.get('X-Request-Start', '.'))
            except ValueError:
                ts = time()

            link_id, url = link

            await self.redis.enqueue_job(
                'store_click', link_id=link_id, ip=ip_address, user_agent=request.headers.get('User-Agent'), ts=ts
            )
            if arg_url and arg_url != url:
                logger.warning('db url does not match arg url: %r !+ %r', url, arg_url)
            raise HTTPTemporaryRedirect(location=url)
        elif arg_url:
            logger.warning('no url found, using arg url "%s"', arg_url)
            raise HTTPTemporaryRedirect(location=arg_url)
        else:
            return dict(url=request.url, http_status_=404)


async def get_create_company_id(conn, company_code: str) -> int:
    company_id = await conn.fetchval('select id from companies where code=$1', company_code)
    if not company_id:
        company_id = await conn.fetchval(
            """
            insert into companies (code) values ($1)
            on conflict (code) do update set code=excluded.code
            returning id
            """,
            company_code,
        )
    return company_id


async def get_company_id(conn, company_code: str) -> int:
    company_id = await conn.fetchval('select id from companies where code=$1', company_code)
    if not company_id:
        raise JsonErrors.HTTPNotFound('company not found')
    return company_id


class EmailSendView(ServiceView):
    async def call(self, request):
        m: EmailSendModel = await self.request_data(EmailSendModel)
        with await self.redis as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise JsonErrors.HTTPConflict(f'Send group with id "{m.uid}" already exists\n')
            await redis.expire(group_key, 86400)

        logger.info('sending %d emails (group %s) via %s for %s', len(m.recipients), m.uid, m.method, m.company_code)
        async with self.app['pg'].acquire() as conn:
            company_id = await get_create_company_id(conn, m.company_code)
            group_id = await conn.fetchval_b(
                'insert into message_groups (:values__names) values :values returning id',
                values=Values(
                    uuid=m.uid,
                    company_id=company_id,
                    message_method=m.method,
                    from_email=m.from_address.email,
                    from_name=m.from_address.name,
                ),
            )
        recipients = m.recipients
        m_base = m.copy(exclude={'recipients'})
        del m
        for recipient in recipients:
            await self.redis.enqueue_job('send_email', group_id, company_id, recipient, m_base)
        return PreResponse(text='201 job enqueued\n', status=201)


async def get_sms_spend(conn: Connection, company_code: str, start: datetime, end: datetime, method: str):
    v = await conn.fetchval(
        """
        select sum(cost)
        from messages
        join companies c on messages.company_id = c.id
        where c.code=$1 and method = $4 and send_ts between $2 and $3
        """,
        company_code,
        start,
        end,
        method,
    )
    return v or 0


class SmsBillingView(ServiceView):
    async def call(self, request) -> PreResponse:
        m = await self.request_data(SmsBillingModel)
        company_code = self.request.match_info['company_code']
        method = self.request.match_info['method']
        total_spend = await get_sms_spend(self.app['pg'], company_code, m.start, m.end, method)
        data = {
            'company': company_code,
            'start': m.start.strftime('%Y-%m-%d'),
            'end': m.end.strftime('%Y-%m-%d'),
            'spend': total_spend,
        }
        return self.json_response(**data)


def month_interval() -> Tuple[datetime, datetime]:
    n = datetime.utcnow().replace(tzinfo=timezone.utc)
    return n.replace(day=1, hour=0, minute=0, second=0, microsecond=0), n


class SmsSendView(ServiceView):
    async def call(self, request):
        m = await self.request_data(SmsSendModel)
        with await self.redis as redis:
            group_key = f'group:{m.uid}'
            v = await redis.incr(group_key)
            if v > 1:
                raise JsonErrors.HTTPConflict(f'Send group with id "{m.uid}" already exists\n')
            await redis.expire(group_key, 86400)

        month_spend = None
        if m.cost_limit is not None:
            start, end = month_interval()
            month_spend = await get_sms_spend(self.app['pg'], m.company_code, start, end, m.method)
            if month_spend >= m.cost_limit:
                return self.json_response(
                    status='send limit exceeded', cost_limit=m.cost_limit, spend=month_spend, status_=402
                )

        async with self.app['pg'].acquire() as conn:
            company_id = await get_create_company_id(conn, m.company_code)
            group_id = await conn.fetchval_b(
                'insert into message_groups (:values__names) values :values returning id',
                values=Values(uuid=m.uid, company_id=company_id, message_method=m.method, from_name=m.from_name),
            )
        logger.info('%s sending %d SMSs', m.company_code, len(m.recipients))

        recipients = m.recipients
        m_base = m.copy(exclude={'recipients'})
        del m
        for recipient in recipients:
            await self.redis.enqueue_job('send_sms', group_id, company_id, recipient, m_base)

        return self.json_response(status='enqueued', spend=month_spend, status_=201)


class SmsValidateView(ServiceView):
    async def call(self, request):
        m = await self.request_data(SmsNumbersModel)
        result = {str(k): self.to_dict(validate_number(n, m.country_code)) for k, n in m.numbers.items()}
        return self.json_response(**result)

    @classmethod
    def to_dict(cls, v):
        return v and asdict(v)


class TestWebhookView(View):
    """
    Simple view to update messages faux-sent with email-test
    """

    async def call(self, request):
        m = await self.request_data(MandrillSingleWebhook)
        await self.redis.enqueue_job('update_message_status', SendMethod.email_test, m)
        return PreResponse(text='message status updated\n')


class MandrillWebhookView(View):
    """
    Update messages sent with mandrill
    """

    async def call(self, request):
        try:
            event_data = (await request.post())['mandrill_events']
        except KeyError:
            raise JsonErrors.HTTPBadRequest('"mandrill_events" not found in post data')

        sig_generated = base64.b64encode(
            hmac.new(
                self.app['webhook_auth_key'],
                msg=(self.app['mandrill_webhook_url'] + 'mandrill_events' + event_data).encode(),
                digestmod=hashlib.sha1,
            ).digest()
        )
        sig_given = request.headers.get('X-Mandrill-Signature', '<missing>').encode()
        if not hmac.compare_digest(sig_generated, sig_given):
            raise JsonErrors.HTTPForbidden('invalid signature')
        try:
            events = ujson.loads(event_data)
        except ValueError as e:
            raise JsonErrors.HTTPBadRequest(f'invalid json data: {e}')

        await self.redis.enqueue_job('update_mandrill_webhooks', events)
        return PreResponse(text='message status updated\n')


class MessageBirdWebhookView(View):
    """
    Update messages sent with message bird
    """

    async def call(self, request):
        # TODO looks like "ts" might be wrong here, appears to always be send time.
        m = MessageBirdWebHook(**request.query)
        await self.redis.enqueue_job('update_message_status', SendMethod.sms_messagebird, m)
        return PreResponse(text='message status updated\n')


class CreateSubaccountView(ServiceView):
    """
    Create a new subaccount with mandrill for new sending company
    """

    async def call(self, request) -> PreResponse:
        method = request.match_info['method']
        if method != SendMethod.email_mandrill:
            return PreResponse(text=f'no subaccount creation required for "{method}"\n')

        m = await self.request_data(SubaccountModel)
        mandrill: Mandrill = self.app['mandrill']

        r = await mandrill.post(
            'subaccounts/add.json', id=m.company_code, name=m.company_name, allowed_statuses=(200, 500), timeout_=12
        )
        data = await r.json()
        if r.status == 200:
            return PreResponse(text='subaccount created\n', status=201)

        assert r.status == 500, r.status
        if f'A subaccount with id {m.company_code} already exists' not in data.get('message', ''):
            return PreResponse(text=f'error from mandrill: {json.dumps(data, indent=2)}\n', status=400)

        r = await mandrill.get('subaccounts/info.json', id=m.company_code, timeout_=12)
        data = await r.json()
        total_sent = data['sent_total']
        if total_sent > 100:
            return PreResponse(
                text=f'subaccount already exists with {total_sent} emails sent, '
                f'reuse of subaccount id not permitted\n',
                status=409,
            )
        else:
            return PreResponse(
                text=f'subaccount already exists with only {total_sent} emails sent, '
                f'reuse of subaccount id permitted\n'
            )


class DeleteSubaccountView(ServiceView):
    """
    Delete an existing subaccount with mandrill
    """

    async def call(self, request) -> PreResponse:
        method = request.match_info['method']
        if method != SendMethod.email_mandrill:
            return PreResponse(text=f'no subaccount deletion required for "{method}"\n')

        m = await self.request_data(SubaccountModel)
        mandrill: Mandrill = self.app['mandrill']

        r = await mandrill.post('subaccounts/delete.json', allowed_statuses=(200, 500), id=m.company_code, timeout_=12)
        data = await r.json()
        if r.status == 200:
            async with self.app['pg'].acquire() as conn:
                company_id = await conn.fetchval('select id from companies where code=$1', m.company_code)
                if company_id:
                    async with conn.transaction() as tr:
                        del_messages_resp = await tr.execute('delete from messages where company_id=$1', company_id)
                        del_groups_resp = await tr.execute('delete from message_groups where company_id=$1', company_id)
                        await tr.execute('delete from companies where id=$1', company_id)
                    del_messages_count = int(del_messages_resp.replace('DELETE ', ''))
                    del_groups_count = int(del_groups_resp.replace('DELETE ', ''))
                else:
                    del_messages_count = del_groups_count = 0
            msg = f'deleted_messages={del_messages_count} deleted_message_groups={del_groups_count}'
            logger.info('deleting company=%s %s', m.company_name, msg)
            return PreResponse(text=msg + '\n', status=200)

        if data.get('name') == 'Unknown_Subaccount':
            return PreResponse(text=data.get('message', 'sub-account not found') + '\n', status=404)

        assert r.status == 500, r.status
        return PreResponse(text=f'error from mandrill: {json.dumps(data, indent=2)}\n', status=400)


class _UserMessagesView(UserView):
    offset = True

    def get_dt_tz(self):
        dt_tz = self.request.query.get('dttz') or 'utc'
        try:
            pytz.timezone(dt_tz)
        except KeyError:
            raise JsonErrors.HTTPBadRequest(f'unknown timezone: "{dt_tz}"')
        return dt_tz

    def get_date_func(self):
        pretty_ts = bool(self.request.query.get('pretty_ts'))
        return 'pretty_ts' if pretty_ts else 'iso_ts'

    def _select_fields(self):
        tz = self.get_dt_tz()
        date_func = self.get_date_func()
        return [
            Var('m.id').as_('id'),
            Func(date_func, Var('send_ts'), tz).as_('send_ts'),
            Func(date_func, Var('update_ts'), tz).as_('update_ts'),
            'external_id',
            'status',
            'to_first_name',
            'to_last_name',
            'to_user_link',
            'to_address',
            'm.company_id',
            'method',
            'subject',
            'body',
            'tags',
            'attachments',
            'from_name',
            'from_name',
            'cost',
            'extra',
        ]

    async def query(self, *, message_id=None, tags=None, query=None):
        where = Var('method') == self.request.match_info['method']
        pg_pool: BuildPgPool = self.app['pg']
        if self.session.company != '__all__':
            company_id = await get_company_id(pg_pool, self.session.company)
            where &= Var('company_id') == company_id

        if message_id:
            where &= Var('id') == message_id
        elif tags:
            where &= Var('tags').contains(tags)
        elif query:
            return await self.query_general(where, query)

        # count is limited to 10,000 as it speeds up the query massively
        count, items = await asyncio.gather(
            pg_pool.fetchval_b(
                """
                select count(*)
                from (
                  select 1
                  from messages
                  where :where
                  limit 10000
                ) as t
                """,
                where=where,
            ),
            pg_pool.fetch_b(
                """
                :select
                from messages m
                join message_groups j on m.group_id = j.id
                where m.id in (
                  select id from messages
                  where :where
                  order by id desc
                  limit 100
                  offset :offset
                )
                order by m.id desc
                """,
                select=Select(self._select_fields()),
                where=where,
                offset=self.get_arg_int('from', 0) if self.offset else 0,
            ),
        )
        return {'count': count, 'items': [dict(r) for r in items]}

    async def query_general(self, where, query):
        async with self.app['pg'].acquire() as conn:
            items = await conn.fetch_b(
                """
                :select
                from messages m
                join message_groups j on m.group_id = j.id
                where m.id in (
                  select id from messages
                  where :where and vector @@ plainto_tsquery(:query)
                  order by id desc
                  limit 100
                  offset :offset
                )
                order by m.id desc
                """,
                select=Select(self._select_fields()),
                tz=self.get_dt_tz(),
                query=query,
                where=where,
                offset=self.get_arg_int('from', 0) if self.offset else 0,
            )
        return {'count': len(items), 'items': [dict(r) for r in items]}


class UserMessagesJsonView(_UserMessagesView):
    def _select_fields(self):
        tz = self.get_dt_tz()
        date_func = self.get_date_func()
        fields = [
            Var('m.id'),
            Func(date_func, Var('send_ts'), tz).as_('send_ts'),
            Func(date_func, Var('update_ts'), tz).as_('update_ts'),
            'external_id',
            'method',
            'subject',
            'status',
            'to_first_name',
            'to_last_name',
            'to_user_link',
            'to_address',
            'm.company_id',
            'tags',
            'from_name',
            'from_name',
            'cost',
            'extra',
        ]
        if self.sms_method:
            fields.append('body')
        return fields

    async def call(self, request):
        self.sms_method = 'sms' in request.match_info['method']
        data = await self.query(
            message_id=self.get_arg_int('message_id'),
            tags=request.query.getall('tags', None),
            query=request.query.get('q'),
        )
        company_code = self.session.company
        if self.sms_method and company_code != '__all__':
            start, end = month_interval()
            data['spend'] = await get_sms_spend(self.app['pg'], company_code, start, end, request.match_info['method'])

        if len(data['items']) == 1:
            data['events'] = await self.events(data)
        return self.json_response(**data)

    async def events(self, data):
        async with self.app['pg'].acquire() as conn:
            events = await conn.fetch(
                """
                select status, iso_ts(ts, $2) ts, extra
                from events where message_id = $1
                """,
                data['items'][0]['id'],
                self.get_dt_tz(),
            )
        return [dict(e) for e in events]


class UserMessageDetailView(TemplateView, _UserMessagesView):
    template = 'user/details.jinja'

    async def call(self, request):
        data = await self.query(message_id=int(self.request.match_info['id']))
        if data['count'] == 0:
            raise JsonErrors.HTTPNotFound('message not found')
        data = data['items'][0]

        preview_path = self.app.router['user-preview'].url_for(**self.request.match_info)
        return dict(
            base_template='user/base-{}.jinja'.format('raw' if self.request.query.get('raw') else 'page'),
            title='{method} - {external_id}'.format(**data),
            id=data['external_id'],
            method=data['method'],
            details=self._details(data),
            events=[e async for e in self._events(data['id'])],
            preview_url=self.full_url(f'{preview_path}?{self.request.query_string}'),
            attachments=list(self._attachments(data)),
        )

    def _details(self, data):
        yield 'ID', data['external_id']
        yield 'Status', data['status'].title()

        dst = f'{data["to_first_name"] or ""} {data["to_last_name"] or ""} <{data["to_address"]}>'.strip(' ')
        link = data.get('to_user_link')
        if link:
            yield 'To', dict(href=link, value=dst)
        else:
            yield 'To', dst

        yield 'Subject', data.get('subject')
        # could do with using prettier timezones here
        yield 'Send Time', {'class': 'datetime', 'value': data['send_ts']}
        yield 'Last Updated', {'class': 'datetime', 'value': data['update_ts']}

    def _attachments(self, data):
        attachments = data['attachments']
        if attachments:
            for a in attachments:
                name = None
                try:
                    doc_id, name = a.split('::')
                    doc_id = int(doc_id)
                except ValueError:
                    yield '#', name or a
                else:
                    yield f'/attachment-doc/{doc_id}/', name

    async def _events(self, message_id):
        events = await self.app['pg'].fetch(
            """
            select status, message_id, iso_ts(ts, $2) as ts, extra
            from events where message_id = $1
            order by id
            limit 51
            """,
            message_id,
            self.get_dt_tz(),
        )
        for event in events[:50]:
            data = dict(status=event['status'].title(), datetime=event['ts'])
            if event['extra']:
                data['details'] = Markup(json.dumps(json.loads(event['extra']), indent=2))
            yield data

        if len(events) > 50:
            extra = await self.app['pg'].fetchval('select count(*) - 50 from events where message_id = $1', message_id)
            yield dict(
                status=f'{extra} more',
                datetime=None,
                details=Markup(json.dumps({'msg': 'extra values not shown'}, indent=2)),
            )


class UserMessageListView(TemplateView, _UserMessagesView):
    template = 'user/list.jinja'

    async def call(self, request):
        data = await self.query(tags=request.query.getall('tags', None), query=request.query.get('q', None))
        monthly_spend = None
        company_code = self.session.company
        method = request.match_info['method']
        if 'sms' in method and company_code != '__all__':
            start, end = month_interval()
            monthly_spend = '{:,.3f}'.format(await get_sms_spend(self.app['pg'], company_code, start, end, method))
        hits = data['items']
        headings = ['To', 'Send Time', 'Status', 'Subject']
        total = data['count']
        size = 100
        offset = self.get_arg_int('from', 0)
        pagination = {}
        if len(hits) == size:
            next_offset = offset + size
            pagination['next'] = dict(
                href=f'?from={next_offset}',
                pfrom=next_offset,
                text=f'{next_offset + 1} - {min(next_offset + size, total)}',
            )
        if offset:
            previous_offset = offset - size
            pagination['previous'] = dict(
                href=f'?from={previous_offset}', pfrom=previous_offset, text=f'{previous_offset + 1} - {max(offset, 0)}'
            )

        return dict(
            base_template='user/base-{}.jinja'.format('raw' if self.request.query.get('raw') else 'page'),
            title=f'{self.request.match_info["method"]} - {total}',
            total=total,
            total_sms_spend=monthly_spend,
            table_headings=headings,
            table_body=self._table_body(hits),
            pagination=pagination,
        )

    def _table_body(self, items):
        for msg in items:
            subject = msg.get('subject') or msg.get('body', '')
            yield [
                {'href': msg['id'], 'value': msg['to_address']},
                {'class': 'datetime', 'value': msg['send_ts']},
                msg['status'].title(),
                truncate(subject, 40),
            ]


class UserMessagePreviewView(TemplateView, UserView):
    """
    preview a message
    """

    template = 'user/preview.jinja'

    async def call(self, request):
        method = self.request.match_info['method']
        where = (Var('m.method') == method) & (Var('m.id') == int(request.match_info['id']))

        if self.session.company != '__all__':
            where &= Var('c.code') == self.session.company

        async with self.app['pg'].acquire() as conn:
            data = await conn.fetchrow_b(
                """
                select from_name, to_last_name, to_address, status, body, extra
                from messages m
                join message_groups j on m.group_id = j.id
                join companies c on m.company_id = c.id
                where :where
                """,
                where=where,
            )

        if not data:
            raise JsonErrors.HTTPNotFound('message not found')

        data = dict(data)
        body = data['body']
        # Remove links from preview
        body = re.sub('(href=").*?"', r'\1#"', body, flags=re.S | re.I)

        extra = json.loads(data['extra']) if data.get('extra') else {}
        if method.startswith('sms'):
            # need to render the sms so it makes sense to users
            return {
                'from': data['from_name'],
                'to': data['to_last_name'] or data['to_address'],
                'status': data['status'],
                'message': body,
                'extra': extra,
            }
        else:
            return {'raw': body}


agg_sql = """
select json_build_object(
  'histogram', histogram,
  'all_90_day', agg.all_90,
  'open_90_day', agg.open_90,
  'all_28_day', agg.all_28,
  'open_28_day', agg.open_28,
  'all_7_day', agg.all_7,
  'open_7_day', agg.open_7
)
from (
  select coalesce(json_agg(t), '[]') AS histogram from (
    select count(*), to_char(day, 'YYYY-MM-DD') as day, status
    from (
      select date_trunc('day', send_ts) as day, status
      from messages
      where :where and send_ts > current_timestamp::date - '28 days'::interval
    ) as t
    group by day, status
  ) as t
) as histogram,
(
  select
    count(*) as all_90,
    count(*) filter (where status = 'open') as open_90,
    count(*) filter (where send_ts > current_timestamp::date - '28 days'::interval) as all_28,
    count(*) filter (where send_ts > current_timestamp::date - '28 days'::interval and status = 'open') as open_28,
    count(*) filter (where send_ts > current_timestamp::date - '7 days'::interval) as all_7,
    count(*) filter (where send_ts > current_timestamp::date - '7 days'::interval and status = 'open') as open_7
  from messages
  where :where and send_ts > current_timestamp::date - '90 days'::interval
) as agg
"""


class UserAggregationView(UserView):
    """
    Aggregated sends and opens over time for an authenticated user
    """

    async def call(self, request):
        # TODO allow more filtering here, filter to last X days.
        where = Var('method') == self.request.match_info['method']

        async with self.app['pg'].acquire() as conn:
            if self.session.company != '__all__':
                where &= Var('company_id') == await get_company_id(conn, self.session.company)
            data = await conn.fetchval_b(agg_sql, where=where)
        return PreResponse(text=data, content_type='application/json')


class AdminAggregatedView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill.value)
        url = self.app.router['user-aggregation'].url_for(method=method)

        r = await morpheus_api.get(url)
        data = await r.json()
        # ignore "click" and "unsub"
        headings = ['date', 'deferral', 'send', 'open', 'reject', 'soft_bounce', 'hard_bounce', 'spam', 'open rate']
        was_sent_statuses = 'send', 'open', 'soft_bounce', 'hard_bounce', 'spam', 'click'
        table_body = []
        hist = sorted(data['histogram'], key=itemgetter('day'), reverse=True)
        for period, g in groupby(hist, key=itemgetter('day')):
            row = [period]
            counts = {v['status']: v['count'] for v in g}
            row += [counts.get(h) or 0 for h in headings[1:-1]]
            was_sent = sum(counts.get(h) or 0 for h in was_sent_statuses)
            opened = counts.get('open') or 0
            if was_sent > 0:
                row.append(f'{opened / was_sent * 100:0.2f}%')
            else:
                row.append(f'{0:0.2f}%')
            table_body.append(row)
        return dict(
            total=data['all_28_day'],
            table_headings=headings,
            table_body=table_body,
            sub_heading=f'Aggregated {method} data',
        )


class AdminListView(AdminView):
    async def get_context(self, morpheus_api):
        method = self.request.query.get('method', SendMethod.email_mandrill.value)
        offset = int(self.request.query.get('offset', '0'))
        search = self.request.query.get('search', '')
        tags = self.request.query.get('tags', '')
        query = {'size': 100, 'from': offset, 'q': search, 'pretty_ts': '1'}
        # tags is a list so has to be processed separately
        if tags:
            query['tags'] = tags
        url = self.app.router['user-messages'].url_for(method=method).with_query(query)

        r = await morpheus_api.get(url)
        data = await r.json()

        company_ids = {m['company_id'] for m in data['items']}
        company_lookup = dict(
            await self.app['pg'].fetch('select id, code from companies where id = any($1)', company_ids)
        )

        headings = ['score', 'to', 'company', 'status', 'sent at', 'updated at', 'subject']
        table_body = []
        for i, message in enumerate(data['items']):
            subject = message.get('subject') or message.get('body', '')[:50]
            score = message.get('score') or None
            table_body.append(
                [
                    str(i + 1 + offset) if score is None else f'{score:6.3f}',
                    {
                        'href': self.app.router['admin-get'].url_for(method=method, id=str(message['id'])),
                        'text': message['to_address'],
                    },
                    company_lookup[message['company_id']],
                    message['status'],
                    message['send_ts'],
                    message['update_ts'],
                    Markup(f'<span class="subject">{subject}</span>'),
                ]
            )

        if len(data['items']) == 100:
            next_offset = offset + 100
            query = {'method': method, 'search': search, 'tags': tags, 'offset': next_offset}
            next_page = dict(
                href=self.app.router['admin-list'].url_for().with_query(query),
                text=f'Next: {next_offset} - {next_offset + 100}',
            )
        else:
            next_page = None
        user_list_path = morpheus_api.modify_url(self.app.router['user-message-list'].url_for(method=method))
        return dict(
            total=data['count'],
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

    async def get_context(self, morpheus_api):
        method = self.request.match_info['method']
        message_id = self.request.match_info['id']
        url = self.app.router['user-messages'].url_for(method=method).with_query({'message_id': message_id})

        r = await morpheus_api.get(url)
        data = await r.json()
        data = json.dumps(data, indent=2)

        preview_path = morpheus_api.modify_url(self.app.router['user-preview'].url_for(method=method, id=message_id))
        deets_path = morpheus_api.modify_url(self.app.router['user-message-get'].url_for(method=method, id=message_id))
        return dict(
            sub_heading=f'Message {message_id}',
            preview_url=self.full_url(preview_path),
            details_url=self.full_url(deets_path),
            json_display=highlight(data, JsonLexer(), HtmlFormatter()),
            form_action=self.app.router['admin-list'].url_for(),
        )


msg_stats_sql = """
select coalesce(json_agg(t), '[]') from (
  select count(*), extract(epoch from avg(update_ts - send_ts))::int as age, method, status
  from messages m
  join message_groups j on m.group_id = j.id
  where m.update_ts > current_timestamp - '10 mins'::interval
  group by method, status
) as t
"""


class MessageStatsView(AuthView):
    auth_token_field = 'stats_token'

    async def call(self, request):
        cache_key = 'message-stats'
        with await self.redis as redis:
            results = await redis.get(cache_key)
            if not results:
                async with self.app['pg'].acquire() as conn:
                    results = await conn.fetchval_b(msg_stats_sql)
                await redis.setex(cache_key, 598, results)
        return PreResponse(body=results, content_type='application/json')
