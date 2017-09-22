import asyncio
import base64
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

import chevron
import msgpack
from aiohttp import ClientConnectionError, ClientError, ClientSession
from arq import Actor, BaseWorker, Drain, concurrent, cron
from arq.utils import from_unix_ms, to_unix_ms, truncate
from chevron import ChevronError
from phonenumbers import parse as parse_number
from phonenumbers import (NumberParseException, PhoneNumberFormat, PhoneNumberType, format_number, is_valid_number,
                          number_type)
from phonenumbers.geocoder import country_name_for_number, description_for_number
from ua_parser.user_agent_parser import Parse as ParseUserAgent

from .es import ElasticSearch
from .models import THIS_DIR, BaseWebhook, ClickInfo, EmailSendMethod, MandrillWebhook, MessageStatus, SmsSendMethod
from .render import EmailInfo, render_email
from .render.main import MessageTooLong, SmsLength, apply_short_links, sms_length
from .settings import Settings
from .utils import ApiError, Mandrill, MessageBird

test_logger = logging.getLogger('morpheus.worker.test')
main_logger = logging.getLogger('morpheus.worker')
MOBILE_NUMBER_TYPES = PhoneNumberType.MOBILE, PhoneNumberType.FIXED_LINE_OR_MOBILE
ONE_DAY = 86400
ONE_YEAR = ONE_DAY * 365


class EmailJob(NamedTuple):
    group_id: str
    send_method: str
    first_name: str
    last_name: str
    user_link: int
    address: str
    tags: List[str]
    pdf_attachments: List[dict]
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


class SmsJob(NamedTuple):
    group_id: str
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


class Number(NamedTuple):
    number: str
    country_code: str
    number_formatted: str
    descr: str
    is_mobile: bool


class SmsData(NamedTuple):
    number: Number
    message: str
    shortened_link: dict
    length: SmsLength


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = self.es = self.mandrill = self.messagebird = None
        self.mandrill_webhook_auth_key = None
        self.email_click_url = f'https://{self.settings.click_host_name}/l'
        self.sms_click_url = f'{self.settings.click_host_name}/l'

    async def startup(self):
        main_logger.info('Sender initialising session and elasticsearch and mandrill...')
        self.session = ClientSession(loop=self.loop)
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)
        self.mandrill = Mandrill(settings=self.settings, loop=self.loop)
        self.messagebird = MessageBird(settings=self.settings, loop=self.loop)

    async def shutdown(self):
        self.session.close()
        self.es.close()
        self.mandrill.close()
        self.messagebird.close()

    @concurrent
    async def send_emails(self,
                          recipients_key, *,
                          uid,
                          main_template,
                          mustache_partials,
                          macros,
                          subject_template,
                          company_code,
                          from_email,
                          from_name,
                          method,
                          subaccount,
                          important,
                          tags,
                          context,
                          headers):
        if method == EmailSendMethod.email_mandrill:
            coro = self._send_mandrill
        elif method == EmailSendMethod.email_test:
            coro = self._send_test_email
        else:
            raise NotImplementedError()
        tags.append(uid)
        main_logger.info('sending email group %s via %s', uid, method)
        base_kwargs = dict(
            group_id=uid,
            send_method=method,
            main_template=main_template,
            mustache_partials=mustache_partials,
            macros=macros,
            subject_template=subject_template,
            company_code=company_code,
            from_email=from_email,
            from_name=from_name,
            subaccount=subaccount,
            important=important,
        )
        if 'styles__sass' not in context and re.search('\{\{\{ *styles *\}\}\}', main_template):
            context['styles__sass'] = (THIS_DIR / 'extra' / 'default-styles.scss').read_text()

        drain = Drain(
            redis_pool=await self.get_redis_pool(),
            raise_task_exception=True,
            max_concurrent_tasks=10,
            shutdown_delay=60,
        )
        jobs = 0
        async with drain:
            async for raw_queue, raw_data in drain.iter(recipients_key):
                if not raw_queue:
                    break

                msg_data = msgpack.unpackb(raw_data, encoding='utf8')
                data = dict(
                    context=dict(context, **msg_data.pop('context')),
                    headers=dict(headers, **msg_data.pop('headers')),
                    tags=list(set(tags + msg_data.pop('tags'))),
                    **base_kwargs,
                    **msg_data,
                )
                drain.add(coro, EmailJob(**data))
                # TODO stop if worker is not running
                jobs += 1
        return jobs

    async def _send_mandrill(self, j: EmailJob):
        email_info = await self._render_email(j)
        if not email_info:
            return
        main_logger.info('%s: send to "%s" subject="%s" body=%d attachments=[%s]',
                         j.group_id, j.address, truncate(email_info.subject, 40), len(email_info.html_body),
                         ', '.join(f'{a["name"]}:{len(a["html"])}' for a in j.pdf_attachments))
        data = {
            'async': True,
            'message': dict(
                html=email_info.html_body,
                subject=email_info.subject,
                from_email=j.from_email,
                from_name=j.from_name,
                to=[
                    dict(
                        email=j.address,
                        name=email_info.full_name,
                        type='to'
                    )
                ],
                headers=email_info.headers,
                track_opens=True,
                track_clicks=False,
                auto_text=True,
                view_content_link=False,
                signing_domain=j.from_email[j.from_email.index('@') + 1:],
                subaccount=j.subaccount,
                tags=j.tags,
                inline_css=True,
                important=j.important,
                attachments=[a async for a in self._generate_base64_pdf(j.pdf_attachments)]
            ),
        }
        send_ts = datetime.utcnow()
        response, exc = None, None
        for i in range(3):
            try:
                response = await self.mandrill.post('messages/send.json', **data)
            except ClientConnectionError as e:
                exc = e
                main_logger.info('%s: client connection error, email: "%s", retrying...', j.group_id, j.address)
                await asyncio.sleep(0.5)
            except (ClientError, ApiError) as e:
                exc = e
                break
            else:
                exc = None
                break

        if exc or response is None:
            e_name = exc.__class__.__name__
            main_logger.exception('%s: error while posting to mandrill, email: "%s", %s: %s',
                                  j.group_id, j.address, e_name, exc)
            await self._store_email_failed(MessageStatus.send_request_failed, j, f'Error sending email: {e_name}')
            return

        data = await response.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == j.address, data
        await self._store_email(data['_id'], send_ts, j, email_info)

    async def _send_test_email(self, j: EmailJob):
        email_info = await self._render_email(j)
        if not email_info:
            return

        data = dict(
            from_email=j.from_email,
            from_name=j.from_name,
            group_id=j.group_id,
            headers=email_info.headers,
            to_address=j.address,
            to_name=email_info.full_name,
            to_user_link=j.user_link,
            tags=j.tags,
            important=j.important,
            attachments=[f'{a["name"]}:{base64.b64decode(a["content"]).decode(errors="ignore"):.40}'
                         async for a in self._generate_base64_pdf(j.pdf_attachments)],
        )
        msg_id = re.sub(r'[^a-zA-Z0-9\-]', '', f'{j.group_id}-{j.address}')
        send_ts = datetime.utcnow()
        output = (
            f'to: {j.address}\n'
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
        await self._store_email(msg_id, send_ts, j, email_info)

    async def _render_email(self, j: EmailJob):
        try:
            return render_email(j, self.email_click_url)
        except ChevronError as e:
            await self._store_email_failed(MessageStatus.render_failed, j, f'Error rendering email: {e}')

    async def _generate_base64_pdf(self, pdf_attachments):
        headers = dict(
            pdf_page_size='A4',
            pdf_zoom='1.25',
            pdf_margin_left='8mm',
            pdf_margin_right='8mm',
        )
        for a in pdf_attachments:
            async with self.session.get(self.settings.pdf_generation_url, data=a['html'], headers=headers) as r:
                if r.status == 200:
                    pdf_content = await r.read()
                    yield dict(
                        type='application/pdf',
                        name=a['name'],
                        content=base64.b64encode(pdf_content).decode(),
                    )
                else:
                    data = await r.text()
                    main_logger.warning('error generating pdf %s, data: %s', r.status, data)

    async def _store_email(self, uid, send_ts, j: EmailJob, email_info: EmailInfo):
        await self.es.post(
            f'messages/{j.send_method}/{uid}',
            company=j.company_code,
            send_ts=send_ts,
            update_ts=send_ts,
            status=MessageStatus.send,
            group_id=j.group_id,
            to_first_name=j.first_name,
            to_last_name=j.last_name,
            to_user_link=j.user_link,
            to_address=j.address,
            from_email=j.from_email,
            from_name=j.from_name,
            tags=j.tags,
            subject=email_info.subject,
            body=email_info.html_body,
            attachments=[f'{a["id"] or ""}::{a["name"]}' for a in j.pdf_attachments],
            events=[]
        )
        for url, token in email_info.shortened_link:
            await self.es.post(
                'links/c/',
                token=token,
                url=url,
                company=j.company_code,
                send_method=j.send_method,
                send_message_id=uid,
                expires_ts=datetime.utcnow() + timedelta(days=365*50)
            )

    async def _store_email_failed(self, status: MessageStatus, j: EmailJob, error_msg):
        await self.es.post(
            f'messages/{j.send_method}',
            company=j.company_code,
            send_ts=datetime.utcnow(),
            update_ts=datetime.utcnow(),
            status=status,
            group_id=j.group_id,
            to_first_name=j.first_name,
            to_last_name=j.last_name,
            to_user_link=j.user_link,
            to_address=j.address,
            from_email=j.from_email,
            from_name=j.from_name,
            tags=j.tags,
            body=error_msg,
            attachments=[a['name'] for a in j.pdf_attachments]
        )

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

    @concurrent
    async def send_smss(self,
                        recipients_key, *,
                        uid,
                        main_template,
                        company_code,
                        cost_limit,
                        country_code,
                        from_name,
                        method,
                        context,
                        tags):
        if method == SmsSendMethod.sms_test:
            coro = self._test_send_sms
        elif method == SmsSendMethod.sms_messagebird:
            coro = self._messagebird_send_sms
        else:
            raise NotImplementedError()
        tags.append(uid)
        main_logger.info('sending group %s via %s', uid, method)
        base_kwargs = dict(
            group_id=uid,
            send_method=method,
            main_template=main_template,
            company_code=company_code,
            country_code=country_code,
            from_name=from_name if country_code != 'US' else self.settings.us_send_number,
        )
        drain = Drain(
            redis_pool=await self.get_redis_pool(),
            raise_task_exception=True,
            max_concurrent_tasks=10,
            shutdown_delay=60,
        )
        jobs = 0
        async with drain:
            async for raw_queue, raw_data in drain.iter(recipients_key):
                if not raw_queue:
                    break

                if cost_limit is not None:
                    spend = await self.check_sms_limit(company_code)
                    if spend >= cost_limit:
                        main_logger.warning('cost limit exceeded %0.2f >= %0.2f, %s', spend, cost_limit, company_code)
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
            await self.es.post(
                f'messages/{j.send_method}',
                company=j.company_code,
                send_ts=datetime.utcnow(),
                update_ts=datetime.utcnow(),
                status=MessageStatus.render_failed,
                to_first_name=j.first_name,
                to_last_name=j.last_name,
                to_user_link=j.user_link,
                to_address=number_info.number_formatted if number_info else j.number,
                group_id=j.group_id,
                from_name=j.from_name,
                tags=j.tags,
                body=error,
            )
        else:
            return SmsData(number=number_info, message=msg, shortened_link=shortened_link, length=msg_length)

    async def _test_send_sms(self, j: SmsJob):
        sms_data = await self._sms_prep(j)
        if not sms_data:
            return

        # remove the + from the beginning of the number
        msg_id = f'{j.group_id}-{sms_data.number.number[1:]}'
        send_ts = datetime.utcnow()
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
                main_logger.error('no default messagebird pricing with mcc "0"', extra={
                    'data': data,
                })
            data = {g['mcc']: f'{float(g["rate"]):0.5f}' for g in data}
            await asyncio.gather(
                redis.hmset_dict(rates_key, data),
                redis.expire(rates_key, ONE_DAY),
            )
        rate = await redis.hget(rates_key, mcc, encoding='utf8')
        if not rate:
            main_logger.warning('no rate found for mcc: "%s", using default', mcc)
            rate = await redis.hget(rates_key, '0', encoding='utf8')
        assert rate, f'no rate found for mcc: {mcc}'
        return float(rate)

    async def _messagebird_get_number_cost(self, number: Number):
        cc_mcc_key = f'messagebird-cc:{number.country_code}'
        pool = await self.get_redis_pool()
        async with pool.get() as redis:
            mcc = await redis.get(cc_mcc_key)
            if mcc is None:
                main_logger.info('no mcc for %s, doing HLR lookup...', number.number)
                api_number = number.number.replace('+', '')
                await self.messagebird.post(f'lookup/{api_number}/hlr')
                data = None
                for i in range(30):
                    r = await self.messagebird.get(f'lookup/{api_number}')
                    data = await r.json()
                    if data['hlr']['status'] == 'active':
                        main_logger.info('found result for %s after %d attempts %s',
                                         number.number, i, json.dumps(data, indent=2))
                        break
                    await asyncio.sleep(1)
                mcc = str(data['hlr']['network'])[:3]
                await redis.setex(cc_mcc_key, ONE_YEAR, mcc)
            return await self._messagebird_get_mcc_cost(redis, mcc)

    async def _messagebird_send_sms(self, j: SmsJob):
        sms_data = await self._sms_prep(j)
        if sms_data is None:
            return
        msg_cost = await self._messagebird_get_number_cost(sms_data.number)

        cost = sms_data.length.parts * msg_cost
        send_ts = datetime.utcnow()
        main_logger.info('sending SMS to %s, parts: %d, cost: %0.2fp',
                         sms_data.number.number, sms_data.length.parts, cost * 100)
        r = await self.messagebird.post(
            'messages',
            originator=j.from_name,
            body=sms_data.message,
            recipients=[sms_data.number.number],
            allowed_statuses=201,
            reference='morpheus',  # required to prompt status updates to occur
        )
        data = await r.json()
        if data['recipients']['totalCount'] != 1:
            main_logger.error('not one recipients in send response', extra={'data': data})
        await self._store_sms(data['id'], send_ts, j, sms_data, cost)

    async def _store_sms(self, uid, send_ts, j: SmsJob, sms_data: SmsData, cost: float):
        await self.es.post(
            f'messages/{j.send_method}/{uid}',
            company=j.company_code,
            send_ts=send_ts,
            update_ts=send_ts,
            status=MessageStatus.send,
            group_id=j.group_id,
            to_first_name=j.first_name,
            to_last_name=j.last_name,
            to_user_link=j.user_link,
            to_address=sms_data.number.number_formatted,
            from_name=j.from_name,
            tags=j.tags,
            body=sms_data.message,
            cost=cost,
            extra=sms_data.length._asdict(),
            events=[],
        )
        for url, token in sms_data.shortened_link:
            await self.es.post(
                'links/c/',
                token=token,
                url=url,
                company=j.company_code,
                send_method=j.send_method,
                send_message_id=uid,
                expires_ts=datetime.utcnow() + timedelta(days=90)
            )

    async def check_sms_limit(self, company_code):
        r = await self.es.get(
            'messages/_search?size=0',
            query={
                'bool': {
                    'filter': [
                        {
                            'term': {'company': company_code}
                        },
                        {
                            'range': {'send_ts': {'gte': 'now-28d/d'}}
                        }
                    ]
                }
            },
            aggs={
                'total_spend': {'sum': {'field': 'cost'}}
            }
        )
        data = await r.json()
        return data['aggregations']['total_spend']['value']

    @concurrent(Actor.LOW_QUEUE)
    async def update_mandrill_webhooks(self, events):
        mandrill_webhook = MandrillWebhook(events=events)
        main_logger.info('updating %d messages', len(mandrill_webhook.events))
        # do in a loop to avoid elastic search conflict
        for m in mandrill_webhook.events:
            await self.update_message_status('email-mandrill', m, log_each=False)

    @concurrent
    async def store_click(self, *, target, ip, ts, user_agent, send_method, send_message_id):
        extra = {
            'target': target,
            'ip': ip,
            'user_agent': user_agent,
        }
        if user_agent:
            ua_dict = ParseUserAgent(user_agent)
            platform = ua_dict['device']['family']
            if platform in {'Other', None}:
                platform = ua_dict['os']['family']
            extra['user_agent_display'] = ('{user_agent[family]} {user_agent[major]} on '
                                           '{platform}').format(platform=platform, **ua_dict).strip(' ')

        # TODO process ip and add geo info
        m = ClickInfo(
            ts=ts,
            status='click',
            message_id=send_message_id,
            extra_=extra
        )
        await self.update_message_status(send_method, m)

    async def update_message_status(self, es_type, m: BaseWebhook, log_each=True):
        r = await self.es.get(f'messages/{es_type}/{m.message_id}', allowed_statuses=(200, 404))
        if r.status == 404:
            return
        data = await r.json()

        events = data['_source']['events']
        if events:
            last_event = data['_source']['events'][-1]
            if (to_unix_ms(m.ts) == last_event['ts'] and
                    m.status == last_event['status'] and
                    json.dumps(m.extra(), sort_keys=True) == json.dumps(last_event['extra'], sort_keys=True)):
                main_logger.info('event already exists %s, ts: %s, status: %s. skipped', m.message_id, m.ts, m.status)
                return

        old_update_ts = from_unix_ms(data['_source']['update_ts'])
        if m.ts.tzinfo:
            old_update_ts = old_update_ts.replace(tzinfo=timezone.utc)

        update_uri = f'messages/{es_type}/{m.message_id}/_update?retry_on_conflict=10'
        try:
            # give 1 second "lee way" for new event to have happened just before the old event
            if m.ts >= (old_update_ts - timedelta(seconds=1)):
                await self.es.post(update_uri, doc={'update_ts': m.ts, 'status': m.status})
            log_each and main_logger.info('updating message %s, ts: %s, status: %s', m.message_id, m.ts, m.status)
            await self.es.post(
                update_uri,
                script={
                    'inline': 'ctx._source.events.add(params.event)',
                    'params': {
                        'event': {
                            'ts': m.ts,
                            'status': m.status,
                            'extra': m.extra(),
                        }
                    }
                }
            )
        except ApiError as e:  # pragma: no cover
            # no error here if we know the problem
            if e.status == 409:
                main_logger.info('ElasticSearch conflict for %s, ts: %s, status: %s', m.message_id, m.ts, m.status)
                return
            else:
                raise


class AuxActor(Actor):  # pragma: no cover
    def __init__(self, settings: Settings = None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.es = None

    async def startup(self):
        main_logger.info('AuxActor initialising elasticsearch...')
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)

    async def shutdown(self):
        self.es.close()

    @cron(hour=3, minute=0)
    async def snapshot_es(self):
        await self.es.create_snapshot()


class Worker(BaseWorker):  # pragma: no cover
    max_concurrent_tasks = 4
    timeout_seconds = 1200
    shadows = [Sender, AuxActor]

    def __init__(self, **kwargs):
        self.settings = Settings(sender_cls='app.worker.Sender')
        kwargs['redis_settings'] = self.settings.redis_settings
        super().__init__(**kwargs)

    async def shadow_kwargs(self):
        d = await super().shadow_kwargs()
        d['settings'] = self.settings
        return d
