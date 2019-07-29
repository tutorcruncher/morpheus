import asyncio
import base64
import hashlib
import json
import logging
import re
from asyncio import TimeoutError
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from itertools import chain
from pathlib import Path
from typing import Dict, List, Optional

import chevron
from aiohttp import ClientConnectionError, ClientSession
from arq import Retry
from arq.utils import to_unix_ms
from buildpg import MultipleValues, Values, asyncpg
from chevron import ChevronError
from phonenumbers import (
    NumberParseException,
    PhoneNumberFormat,
    PhoneNumberType,
    format_number,
    is_valid_number,
    number_type,
    parse as parse_number,
)
from phonenumbers.geocoder import country_name_for_number, description_for_number
from pydantic.datetime_parse import parse_datetime
from ua_parser.user_agent_parser import Parse as ParseUserAgent

from .ext import ApiError, Mandrill, MessageBird
from .models import (
    THIS_DIR,
    AttachmentModel,
    BaseWebhook,
    EmailRecipientModel,
    EmailSendMethod,
    EmailSendModel,
    MandrillWebhook,
    MessageStatus,
    SendMethod,
    SmsSendMethod,
)
from .render import EmailInfo, render_email
from .render.main import MessageDef, MessageTooLong, SmsLength, apply_short_links, sms_length
from .settings import Settings

test_logger = logging.getLogger('morpheus.worker.test')
main_logger = logging.getLogger('morpheus.worker')
MOBILE_NUMBER_TYPES = PhoneNumberType.MOBILE, PhoneNumberType.FIXED_LINE_OR_MOBILE
ONE_DAY = 86400
ONE_YEAR = ONE_DAY * 365
STYLES_SASS = (THIS_DIR / 'extra' / 'default-styles.scss').read_text()


worker_functions = []


def worker_function(f):
    worker_functions.append(f)
    return f


@dataclass
class EmailJob:
    group_id: int
    group_uuid: str
    send_method: str
    first_name: str
    last_name: str
    user_link: int
    address: str
    tags: List[str]
    pdf_attachments: List[dict]
    attachments: List[dict]
    main_template: str
    mustache_partials: Dict[str, dict]
    macros: Dict[str, dict]
    subject_template: str
    company_code: str
    from_email: str
    from_name: str
    subaccount: str
    important: bool
    context: dict
    headers: dict


@dataclass
class SmsJob:
    group_id: str
    group_uuid: str
    send_method: str
    first_name: str
    last_name: str
    user_link: int
    number: str
    tags: List[str]
    main_template: str
    company_code: str
    country_code: str
    from_name: str
    context: dict


@dataclass
class Number:
    number: str
    country_code: str
    number_formatted: str
    descr: str
    is_mobile: bool


@dataclass
class SmsData:
    number: Number
    message: str
    shortened_link: dict
    length: SmsLength


class UpdateStatus(str, Enum):
    duplicate = 'duplicate'
    missing = 'missing'
    added = 'added'


async def startup(ctx):
    settings = ctx.get('settings') or Settings()
    ctx.update(
        settings=settings,
        email_click_url=f'https://{settings.click_host_name}/l',
        sms_click_url=f'{settings.click_host_name}/l',
        pg=ctx.get('pg') or await asyncpg.create_pool_b(dsn=settings.pg_dsn, min_size=2),
        session=ClientSession(),
        mandrill=Mandrill(settings=settings),
        messagebird=MessageBird(settings=settings),
    )


async def shutdown(ctx):
    await asyncio.gather(ctx['session'].close(), ctx['pg'].close(), ctx['mandrill'].close(), ctx['messagebird'].close())


email_retrying = [5, 10, 60, 600, 1800, 3600, 12 * 3600]


@worker_function
async def send_email(ctx, group_id: int, recipient: EmailRecipientModel, m: EmailSendModel):
    s = SendEmail(ctx, group_id, recipient, m)
    return await s.run()


class SendEmail:
    __slots__ = 'ctx', 'settings', 'recipient', 'group_id', 'm', 'tags'

    def __init__(self, ctx: dict, group_id: int, recipient: EmailRecipientModel, m: EmailSendModel):
        self.ctx = ctx
        self.settings: Settings = ctx['settings']
        self.recipient: EmailRecipientModel = recipient
        self.group_id = group_id
        self.m: EmailSendModel = m
        self.tags = list(set(self.recipient.tags + self.m.tags + [self.m.uid.hex]))

    async def run(self):
        if self.ctx['job_try'] > len(email_retrying):
            main_logger.error('%s: tried to send email %d times, all failed', self.group_id, self.ctx['job_try'])
            await self._store_email_failed(MessageStatus.send_request_failed, 'upstream error')
            return

        context = dict(self.m.context, **self.recipient.context)
        if 'styles__sass' not in context and re.search(r'\{\{\{ *styles *\}\}\}', self.m.main_template):
            context['styles__sass'] = STYLES_SASS

        headers = dict(self.m.headers, **self.recipient.headers)

        email_info: EmailInfo = await self._render_email(context, headers)
        if not email_info:
            return

        attachments = [a async for a in self._generate_base64_pdf(self.recipient.pdf_attachments)]
        attachments += [a async for a in self._generate_base64(self.recipient.attachments)]

        if self.m.method == EmailSendMethod.email_mandrill:
            if self.recipient.address.endswith('@example.com'):
                _id = re.sub(r'[^a-zA-Z0-9\-]', '', f'mandrill-{self.recipient.address}')
                await self._store_email(_id, utcnow(), email_info)
            else:
                await self._send_mandrill(email_info, attachments)
        elif self.m.method == EmailSendMethod.email_test:
            await self._send_test_email(email_info, attachments)
        else:
            raise NotImplementedError()

    async def _send_mandrill(self, email_info: EmailInfo, attachments: List[dict]):
        data = {
            'async': True,
            'message': dict(
                html=email_info.html_body,
                subject=email_info.subject,
                from_email=self.m.from_address.email,
                from_name=self.m.from_address.name,
                to=[dict(email=self.recipient.address, name=email_info.full_name, type='to')],
                headers=email_info.headers,
                track_opens=True,
                track_clicks=False,
                auto_text=True,
                view_content_link=False,
                signing_domain=self.m.from_address.email[self.m.from_address.email.index('@') + 1 :],
                subaccount=self.m.subaccount,
                tags=self.tags,
                inline_css=True,
                important=self.m.important,
                attachments=attachments,
            ),
        }
        send_ts = utcnow()
        try:
            response = await self.ctx['mandrill'].post('messages/send.json', **data)
        except (ClientConnectionError, TimeoutError) as e:
            main_logger.info('%s: client connection error, retrying %d...', self.group_id, self.ctx['job_try'])
            raise Retry(defer=email_retrying[self.ctx['job_try'] - 1]) from e
        except ApiError as e:
            if e.status in {502, 504} or (e.status == 500 and '<center>nginx/' in e.body):
                raise Retry(defer=email_retrying[self.ctx['job_try'] - 1]) from e
            else:
                # if the status is not 502 or 504, or 500 from nginx then raise
                raise

        data = await response.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == self.recipient.address, data
        await self._store_email(data['_id'], send_ts, email_info)

    async def _send_test_email(self, email_info: EmailInfo, attachments: List[dict]):
        data = dict(
            from_email=self.m.from_address.email,
            from_name=self.m.from_address.name,
            group_uuid=self.group_id,
            headers=email_info.headers,
            to_address=self.recipient.address,
            to_name=email_info.full_name,
            to_user_link=self.recipient.user_link,
            tags=self.tags,
            important=self.m.important,
            attachments=[
                f'{a["name"]}:{base64.b64decode(a["content"]).decode(errors="ignore"):.40}' for a in attachments
            ],
        )
        msg_id = re.sub(r'[^a-zA-Z0-9\-]', '', f'{self.m.uid.hex}-{self.recipient.address}')
        send_ts = utcnow()
        output = (
            f'to: {self.recipient.address}\n'
            f'msg id: {msg_id}\n'
            f'ts: {send_ts}\n'
            f'subject: {email_info.subject}\n'
            f'data: {json.dumps(data, indent=2)}\n'
            f'content:\n'
            f'{email_info.html_body}\n'
        )
        if self.settings.test_output:  # pragma: no branch
            Path.mkdir(self.settings.test_output, parents=True, exist_ok=True)
            save_path = self.settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        await self._store_email(msg_id, send_ts, email_info)

    async def _render_email(self, context, headers) -> EmailInfo:
        m = MessageDef(
            first_name=self.recipient.first_name,
            last_name=self.recipient.last_name,
            main_template=self.m.main_template,
            mustache_partials=self.m.mustache_partials,
            macros=self.m.macros,
            subject_template=self.m.subject_template,
            context=context,
            headers=headers,
        )
        try:
            return render_email(m, self.ctx['email_click_url'])
        except ChevronError as e:
            await self._store_email_failed(MessageStatus.render_failed, f'Error rendering email: {e}')

    async def _generate_base64_pdf(self, pdf_attachments):
        headers = dict(pdf_page_size='A4', pdf_zoom='1.25', pdf_margin_left='8mm', pdf_margin_right='8mm')
        for a in pdf_attachments:
            async with self.ctx['session'].get(self.settings.pdf_generation_url, data=a.html, headers=headers) as r:
                if r.status == 200:
                    pdf_content = await r.read()
                    yield dict(type='application/pdf', name=a.name, content=base64.b64encode(pdf_content).decode())
                else:
                    data = await r.text()
                    main_logger.warning('error generating pdf %s, data: %s', r.status, data)

    async def _generate_base64(self, attachments: List[AttachmentModel]):
        for attachment in attachments:
            yield dict(
                name=attachment.name, type=attachment.mime_type, content=base64.b64encode(attachment.content).decode()
            )

    async def _store_email(self, external_id, send_ts, email_info: EmailInfo):
        data = dict(
            external_id=external_id,
            group_id=self.group_id,
            send_ts=send_ts,
            status=MessageStatus.send,
            to_first_name=self.recipient.first_name,
            to_last_name=self.recipient.last_name,
            to_user_link=self.recipient.user_link,
            to_address=self.recipient.address,
            tags=self.tags,
            subject=email_info.subject,
            body=email_info.html_body,
        )
        attachments = [
            f'{getattr(a, "id", None) or ""}::{a.name}'
            for a in chain(self.recipient.pdf_attachments, self.recipient.attachments)
        ]
        if attachments:
            data['attachments'] = attachments
        message_id = await self.ctx['pg'].fetchval_b(
            'insert into messages (:values__names) values :values returning id', values=Values(**data)
        )
        if email_info.shortened_link:
            await self.ctx['pg'].execute_b(
                'insert into links (:values__names) values :values',
                values=MultipleValues(
                    *[Values(message_id=message_id, token=token, url=url) for url, token in email_info.shortened_link]
                ),
            )

    async def _store_email_failed(self, status: MessageStatus, error_msg):
        await self.ctx['pg'].execute_b(
            'insert into messages (:values__names) values :values',
            values=Values(
                group_id=self.group_id,
                status=status,
                to_first_name=self.recipient.first_name,
                to_last_name=self.recipient.last_name,
                to_user_link=self.recipient.user_link,
                to_address=self.recipient.address,
                tags=self.tags,
                body=error_msg,
            ),
        )


class SMS:
    @classmethod
    def validate_number(cls, number, country, include_description=True) -> Optional[Number]:
        try:
            p = parse_number(number, country)
        except NumberParseException:
            return

        if not is_valid_number(p):
            return

        is_mobile = number_type(p) in MOBILE_NUMBER_TYPES
        descr = None
        if include_description:
            country = country_name_for_number(p, 'en')
            region = description_for_number(p, 'en')
            descr = country if country == region else f'{region}, {country}'

        return Number(
            number=format_number(p, PhoneNumberFormat.E164),
            country_code=f'{p.country_code}',
            number_formatted=format_number(p, PhoneNumberFormat.INTERNATIONAL),
            descr=descr,
            is_mobile=is_mobile,
        )

    # @concurrent
    async def send_smss(
        self,
        recipients_key,
        *,
        uid,
        main_template,
        company_code,
        cost_limit,
        country_code,
        from_name,
        method,
        context,
        tags,
    ):
        if method == SmsSendMethod.sms_test:
            coro = self._test_send_sms
        elif method == SmsSendMethod.sms_messagebird:
            coro = self._messagebird_send_sms
        else:
            raise NotImplementedError()
        tags.append(uid)
        main_logger.info('sending group %s via %s', uid, method)
        group_id = await self._store_sms_group(group_uuid=uid, company=company_code, method=method, from_name=from_name)
        base_kwargs = dict(
            group_id=group_id,
            group_uuid=uid,
            send_method=method,
            main_template=main_template,
            company_code=company_code,
            country_code=country_code,
            from_name=from_name if country_code != 'US' else self.settings.us_send_number,
        )
        drain = Drain(
            redis=await self.get_redis(), raise_task_exception=True, max_concurrent_tasks=10, shutdown_delay=60
        )
        jobs = 0
        async with drain:
            async for raw_queue, raw_data in drain.iter(recipients_key):
                if not raw_queue:
                    break

                msg_data = msgpack.unpackb(raw_data, encoding='utf8')
                data = dict(
                    context=dict(context, **msg_data.pop('context')),
                    tags=list(set(tags + msg_data.pop('tags'))),
                    **base_kwargs,
                    **msg_data,
                )
                drain.add(coro, SmsJob(**data))
                # TODO stop if worker is not running
                jobs += 1
        return jobs

    async def _sms_prep(self, j: SmsJob) -> Optional[SmsData]:
        number_info = self.validate_number(j.number, j.country_code, include_description=False)
        msg, error, shortened_link, msg_length = None, None, None, None
        if not number_info or not number_info.is_mobile:
            error = f'invalid mobile number "{j.number}"'
            main_logger.warning('invalid mobile number "%s" for "%s", not sending', j.number, j.company_code)
        else:
            shortened_link = apply_short_links(j.context, self.sms_click_url, 12)
            try:
                msg = chevron.render(j.main_template, data=j.context)
            except ChevronError as e:
                error = f'Error rendering SMS: {e}'
            else:
                try:
                    msg_length = sms_length(msg)
                except MessageTooLong as e:
                    error = str(e)

        if error:
            async with self.pg.acquire() as conn:
                await conn.execute_b(
                    'insert into messages (:values__names) values :values',
                    values=Values(
                        group_id=j.group_id,
                        status=MessageStatus.render_failed,
                        to_first_name=j.first_name,
                        to_last_name=j.last_name,
                        to_user_link=j.user_link,
                        to_address=number_info.number_formatted if number_info else j.number,
                        tags=j.tags,
                        body=error,
                    ),
                )
        else:
            return SmsData(number=number_info, message=msg, shortened_link=shortened_link, length=msg_length)

    async def _test_send_sms(self, j: SmsJob):
        sms_data = await self._sms_prep(j)
        if not sms_data:
            return

        # remove the + from the beginning of the number
        msg_id = f'{j.group_uuid}-{sms_data.number.number[1:]}'
        send_ts = utcnow()
        cost = 0.012 * sms_data.length.parts
        output = (
            f'to: {sms_data.number}\n'
            f'msg id: {msg_id}\n'
            f'ts: {send_ts}\n'
            f'group_id: {j.group_id}\n'
            f'tags: {j.tags}\n'
            f'company_code: {j.company_code}\n'
            f'from_name: {j.from_name}\n'
            f'cost: {cost}\n'
            f'length: {sms_data.length}\n'
            f'message:\n'
            f'{sms_data.message}\n'
        )
        if self.settings.test_output:  # pragma: no branch
            Path.mkdir(self.settings.test_output, parents=True, exist_ok=True)
            save_path = self.settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        await self._store_sms(msg_id, send_ts, j, sms_data, cost)

    async def _messagebird_get_mcc_cost(self, redis, mcc):
        rates_key = 'messagebird-rates'
        if not await redis.exists(rates_key):
            # get fresh data on rates by mcc
            main_logger.info('getting fresh pricing data from messagebird...')
            url = (
                f'{self.settings.messagebird_pricing_api}'
                f'?username={self.settings.messagebird_pricing_username}'
                f'&password={self.settings.messagebird_pricing_password}'
            )
            async with self.session.get(url) as r:
                assert r.status == 200, (r.status, await r.text())
                data = await r.json()
            if not next((1 for g in data if g['mcc'] == '0'), None):
                main_logger.error('no default messagebird pricing with mcc "0"', extra={'data': data})
            data = {g['mcc']: f'{float(g["rate"]):0.5f}' for g in data}
            await asyncio.gather(redis.hmset_dict(rates_key, data), redis.expire(rates_key, ONE_DAY))
        rate = await redis.hget(rates_key, mcc, encoding='utf8')
        if not rate:
            main_logger.warning('no rate found for mcc: "%s", using default', mcc)
            rate = await redis.hget(rates_key, '0', encoding='utf8')
        assert rate, f'no rate found for mcc: {mcc}'
        return float(rate)

    async def _messagebird_get_number_cost(self, number: Number):
        cc_mcc_key = f'messagebird-cc:{number.country_code}'
        pool = await self.get_redis()
        with await pool as redis:
            mcc = await redis.get(cc_mcc_key)
            if mcc is None:
                main_logger.info('no mcc for %s, doing HLR lookup...', number.number)
                api_number = number.number.replace('+', '')
                await self.messagebird.post(f'lookup/{api_number}/hlr')
                data = None
                for i in range(30):
                    r = await self.messagebird.get(f'lookup/{api_number}')
                    data = await r.json()
                    hlr = data.get('hlr')
                    if not hlr:
                        return
                    network = hlr.get('network')
                    if not network:
                        return
                    elif hlr['status'] == 'active':
                        main_logger.info(
                            'found result for %s after %d attempts %s', number.number, i, json.dumps(data, indent=2)
                        )
                        break
                    await asyncio.sleep(1)
                mcc = str(network)[:3]
                await redis.setex(cc_mcc_key, ONE_YEAR, mcc)
            return await self._messagebird_get_mcc_cost(redis, mcc)

    async def _messagebird_send_sms(self, j: SmsJob):
        sms_data = await self._sms_prep(j)
        if sms_data is None:
            return
        msg_cost = await self._messagebird_get_number_cost(sms_data.number)
        if msg_cost is None:
            return

        cost = sms_data.length.parts * msg_cost
        send_ts = utcnow()
        main_logger.info(
            'sending SMS to %s, parts: %d, cost: %0.2fp', sms_data.number.number, sms_data.length.parts, cost * 100
        )
        r = await self.messagebird.post(
            'messages',
            originator=j.from_name,
            body=sms_data.message,
            recipients=[sms_data.number.number],
            datacoding='auto',
            reference='morpheus',  # required to prompt status updates to occur
            allowed_statuses=201,
        )
        data = await r.json()
        if data['recipients']['totalCount'] != 1:
            main_logger.error('not one recipients in send response', extra={'data': data})
        await self._store_sms(data['id'], send_ts, j, sms_data, cost)

    async def _store_sms_group(self, *, group_uuid: str, company: str, method: EmailSendMethod, from_name: str):
        async with self.pg.acquire() as conn:
            return await conn.fetchval_b(
                'insert into message_groups (:values__names) values :values returning id',
                values=Values(uuid=group_uuid, company=company, method=method, from_name=from_name),
            )

    async def _store_sms(self, external_id, send_ts, j: SmsJob, sms_data: SmsData, cost: float):
        async with self.pg.acquire() as conn:
            message_id = await conn.fetchval_b(
                'insert into messages (:values__names) values :values returning id',
                values=Values(
                    external_id=external_id,
                    group_id=j.group_id,
                    send_ts=send_ts,
                    status=MessageStatus.send,
                    to_first_name=j.first_name,
                    to_last_name=j.last_name,
                    to_user_link=j.user_link,
                    to_address=sms_data.number.number_formatted,
                    tags=j.tags,
                    body=sms_data.message,
                    cost=cost,
                    extra=json.dumps(asdict(sms_data.length)),
                ),
            )
            if sms_data.shortened_link:
                await conn.execute_b(
                    'insert into links (:values__names) values :values',
                    values=MultipleValues(
                        *[Values(message_id=message_id, token=token, url=url) for url, token in sms_data.shortened_link]
                    ),
                )

    async def check_sms_limit(self, company_code):
        async with self.pg.acquire() as conn:
            return (
                await conn.fetchval(
                    """
                select sum(m.cost)
                from messages as m
                join message_groups j on m.group_id = j.id
                where j.company=$1 and send_ts > (current_timestamp - '28days'::interval)
                """,
                    company_code,
                )
                or 0
            )


@worker_function
async def update_mandrill_webhooks(ctx, events):
    mandrill_webhook = MandrillWebhook(events=events)
    statuses = {}
    for m in mandrill_webhook.events:
        status = await update_message_status(ctx, SendMethod.email_mandrill, m, log_each=False)
        if status in statuses:
            statuses[status] += 1
        else:
            statuses[status] = 1
    main_logger.info(
        'updating %d messages: %s', len(mandrill_webhook.events), ' '.join(f'{k}={v}' for k, v in statuses.items())
    )
    return len(mandrill_webhook.events)


@worker_function
async def store_click(ctx, *, link_id, ip, ts, user_agent):
    cache_key = f'click-{link_id}-{ip}'
    with await ctx['redis'] as redis:
        v = await redis.incr(cache_key)
        if v > 1:
            return 'recently_clicked'
        await redis.expire(cache_key, 60)

    async with ctx['pg'].acquire() as conn:
        message_id, target = await conn.fetchrow('select message_id, url from links where id=$1', link_id)
        extra = {'target': target, 'ip': ip, 'user_agent': user_agent}
        if user_agent:
            ua_dict = ParseUserAgent(user_agent)
            platform = ua_dict['device']['family']
            if platform in {'Other', None}:
                platform = ua_dict['os']['family']
            extra['user_agent_display'] = (
                ('{user_agent[family]} {user_agent[major]} on ' '{platform}')
                .format(platform=platform, **ua_dict)
                .strip(' ')
            )

        ts = parse_datetime(ts)
        status = 'click'
        await conn.execute_b(
            'insert into events (:values__names) values :values',
            values=Values(message_id=message_id, status=status, ts=ts, extra=json.dumps(extra)),
        )


@worker_function
async def update_message_status(ctx, send_method: SendMethod, m: BaseWebhook, log_each=True) -> UpdateStatus:
    h = hashlib.md5(f'{m.message_id}-{to_unix_ms(m.ts)}-{m.status}-{m.extra_json(sort_keys=True)}'.encode())
    ref = f'event-{h.hexdigest()}'
    with await ctx['redis'] as redis:
        v = await redis.incr(ref)
        if v > 1:
            log_each and main_logger.info(
                'event already exists %s, ts: %s, ' 'status: %s. skipped', m.message_id, m.ts, m.status
            )
            return UpdateStatus.duplicate
        await redis.expire(ref, 86400)

    async with ctx['pg'].acquire() as conn:
        message_id = await conn.fetchval(
            """
            select m.id from messages m
            join message_groups j on m.group_id = j.id
            where j.method = $1 and m.external_id = $2
            """,
            send_method,
            m.message_id,
        )
        if not message_id:
            return UpdateStatus.missing

        if not m.ts.tzinfo:
            m.ts = m.ts.replace(tzinfo=timezone.utc)

        log_each and main_logger.info('adding event %s, ts: %s, status: %s', m.message_id, m.ts, m.status)

        await conn.execute_b(
            'insert into events (:values__names) values :values',
            values=Values(message_id=message_id, status=m.status, ts=m.ts, extra=m.extra_json()),
        )
        return UpdateStatus.added


def utcnow():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


class WorkerSettings:
    max_jobs = 4
    max_tries = len(email_retrying) + 1  # so we try all values in email_retrying
    functions = worker_functions
    on_startup = startup
    on_shutdown = shutdown
