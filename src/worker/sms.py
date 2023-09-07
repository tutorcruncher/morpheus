from dataclasses import asdict, dataclass

import asyncio
import chevron
import json
import logging
from buildpg import MultipleValues, Values
from chevron import ChevronError
from foxglove import glove
from pathlib import Path
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
from typing import Optional

from src.ext import MessageBird
from src.render.main import MessageTooLong, SmsLength, apply_short_links, sms_length
from src.schemas.messages import MessageStatus, SmsRecipientModel, SmsSendMethod, SmsSendModel
from src.settings import Settings
from src.worker.email import utcnow

main_logger = logging.getLogger('worker.sms')
test_logger = logging.getLogger('worker.test')


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


async def send_sms(ctx, group_id: int, company_id: int, recipient: SmsRecipientModel, m: SmsSendModel):
    s = SendSMS(ctx, group_id, company_id, recipient, m)
    return await s.run()


class SendSMS:
    __slots__ = ('ctx', 'settings', 'recipient', 'group_id', 'company_id', 'm', 'tags', 'messagebird', 'from_name')

    def __init__(self, ctx: dict, group_id: int, company_id: int, recipient: SmsRecipientModel, m: SmsSendModel):
        self.ctx = ctx
        self.settings: Settings = glove.settings
        self.group_id = group_id
        self.company_id = company_id
        self.recipient: SmsRecipientModel = recipient
        self.m: SmsSendModel = m
        self.tags = list(set(self.recipient.tags + self.m.tags + [str(self.m.uid)]))
        self.messagebird: MessageBird = ctx['messagebird']
        self.from_name = (
            self.settings.tc_registered_originator if self.m.country_code != 'US' else self.settings.us_send_number
        )

    async def run(self):
        sms_data = await self._sms_prep()
        if not sms_data:
            return

        if self.m.method == SmsSendMethod.sms_test:
            await self._test_send_sms(sms_data)
        elif self.m.method == SmsSendMethod.sms_messagebird:
            await self._messagebird_send_sms(sms_data)
        else:
            raise NotImplementedError()

    async def _sms_prep(self) -> Optional[SmsData]:
        number_info = validate_number(self.recipient.number, self.m.country_code, include_description=False)
        msg, error, shortened_link, msg_length = None, None, None, None
        if not number_info or not number_info.is_mobile:
            error = f'invalid mobile number "{self.recipient.number}"'
            main_logger.warning(
                'invalid mobile number "%s" for "%s", not sending', self.recipient.number, self.m.company_code
            )
        else:
            context = dict(self.m.context, **self.recipient.context)
            shortened_link = apply_short_links(context, self.ctx['sms_click_url'], 12)
            try:
                msg = chevron.render(self.m.main_template, data=context)
            except ChevronError as e:
                error = f'Error rendering SMS: {e}'
            else:
                try:
                    msg_length = sms_length(msg)
                except MessageTooLong as e:
                    error = str(e)

        if error:
            await glove.pg.fetchval_b(
                'insert into messages (:values__names) values :values returning id',
                values=Values(
                    group_id=self.group_id,
                    company_id=self.company_id,
                    method=self.m.method,
                    status=MessageStatus.render_failed,
                    to_first_name=self.recipient.first_name,
                    to_last_name=self.recipient.last_name,
                    to_user_link=self.recipient.user_link,
                    to_address=number_info.number_formatted if number_info else self.recipient.number,
                    tags=self.tags,
                    body=error,
                ),
            )
        else:
            return SmsData(number=number_info, message=msg, shortened_link=shortened_link, length=msg_length)

    async def _test_send_sms(self, sms_data: SmsData):
        # remove the + from the beginning of the number
        msg_id = f'{self.m.uid}-{sms_data.number.number[1:]}'
        send_ts = utcnow()
        cost = 0.012 * sms_data.length.parts
        output = (
            f'to: {sms_data.number}\n'
            f'msg id: {msg_id}\n'
            f'ts: {send_ts}\n'
            f'group_id: {self.group_id}\n'
            f'tags: {self.tags}\n'
            f'company_code: {self.m.company_code}\n'
            f'from_name: {self.from_name}\n'
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
        await self._store_sms(msg_id, send_ts, sms_data, cost)

    async def _messagebird_get_mcc_cost(self, redis, mcc):
        rates_key = 'messagebird-rates'
        if not await redis.exists(rates_key):
            # get fresh data on rates by mcc
            main_logger.info('getting fresh pricing data from messagebird...')
            r = await self.messagebird.get('pricing/sms/outbound')
            if r.status_code != 200:
                response = r.text
                main_logger.error(
                    'error getting messagebird api', extra={'status': r.status_code, 'response': response}
                )
                raise MessageBirdExternalError((r.status_code, response))
            data = r.json()
            prices = data['prices']
            if not next((1 for g in prices if g['mcc'] == '0'), None):
                main_logger.error('no default messagebird pricing with mcc "0"', extra={'prices': prices})
            prices = {g['mcc']: f'{float(g["price"]):0.5f}' for g in prices}
            await asyncio.gather(redis.hmset_dict(rates_key, prices), redis.expire(rates_key, ONE_DAY))
        rate = await redis.hget(rates_key, mcc, encoding='utf8')
        if not rate:
            main_logger.warning('no rate found for mcc: "%s", using default', mcc)
            rate = await redis.hget(rates_key, '0', encoding='utf8')
        assert rate, f'no rate found for mcc: {mcc}'
        return float(rate)

    async def _messagebird_get_number_cost(self, number: Number):
        cc_mcc_key = f'messagebird-cc:{number.country_code}'
        with await self.ctx['redis'] as redis:
            mcc = await redis.get(cc_mcc_key)
            if mcc is None:
                main_logger.info('no mcc for %s, doing HLR lookup...', number.number)
                api_number = number.number.replace('+', '')
                await self.messagebird.post(f'lookup/{api_number}/hlr')
                network, hlr = None, None
                for i in range(30):
                    r = await self.messagebird.get(f'lookup/{api_number}')
                    data = r.json()
                    hlr = data.get('hlr')
                    if not hlr:
                        continue
                    network = hlr.get('network')
                    if not network:
                        continue
                    elif hlr['status'] == 'active':
                        main_logger.info(
                            'found result for %s after %d attempts %s', number.number, i, json.dumps(data, indent=2)
                        )
                        break
                    await asyncio.sleep(1)
                if not hlr or not network:
                    main_logger.warning('No HLR result found for %s after 30 attempts', number.number, extra=data)
                    return
                mcc = str(network)[:3]
                await redis.setex(cc_mcc_key, ONE_YEAR, mcc)
            return await self._messagebird_get_mcc_cost(redis, mcc)

    async def _messagebird_send_sms(self, sms_data: SmsData):
        try:
            msg_cost = await self._messagebird_get_number_cost(sms_data.number)
        except MessageBirdExternalError:
            msg_cost = 0  # Set to SMS cost to 0 until cost API is working/changed
        if msg_cost is None:
            return

        cost = sms_data.length.parts * msg_cost
        send_ts = utcnow()
        main_logger.info(
            'sending SMS to %s, parts: %d, cost: %0.2fp', sms_data.number.number, sms_data.length.parts, cost * 100
        )
        r = await self.messagebird.post(
            'messages',
            originator=self.from_name,
            body=sms_data.message,
            recipients=[sms_data.number.number],
            datacoding='auto',
            reference='morpheus',  # required to prompt status updates to occur
            allowed_statuses=201,
        )
        data = r.json()
        if data['recipients']['totalCount'] != 1:
            main_logger.error('not one recipients in send response', extra={'data': data})
        await self._store_sms(data['id'], send_ts, sms_data, cost)

    async def _store_sms(self, external_id, send_ts, sms_data: SmsData, cost: float):
        message_id = await glove.pg.fetchval_b(
            'insert into messages (:values__names) values :values returning id',
            values=Values(
                external_id=external_id,
                group_id=self.group_id,
                company_id=self.company_id,
                method=self.m.method,
                send_ts=send_ts,
                status=MessageStatus.send,
                to_first_name=self.recipient.first_name,
                to_last_name=self.recipient.last_name,
                to_user_link=self.recipient.user_link,
                to_address=sms_data.number.number_formatted,
                tags=self.tags,
                body=sms_data.message,
                cost=cost,
                extra=json.dumps(asdict(sms_data.length)),
            ),
        )
        if sms_data.shortened_link:
            await glove.pg.execute_b(
                'insert into links (:values__names) values :values',
                values=MultipleValues(
                    *[Values(message_id=message_id, token=token, url=url) for url, token in sms_data.shortened_link]
                ),
            )


def validate_number(number, country, include_description=True) -> Optional[Number]:
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


MOBILE_NUMBER_TYPES = PhoneNumberType.MOBILE, PhoneNumberType.FIXED_LINE_OR_MOBILE


class MessageBirdExternalError(Exception):
    pass


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


ONE_DAY = 86400
ONE_YEAR = ONE_DAY * 365
