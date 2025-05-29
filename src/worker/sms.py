from dataclasses import asdict, dataclass

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
        if self.m.country_code == 'US':
            self.from_name = self.settings.us_send_number
        elif self.m.country_code == 'CA':
            self.from_name = self.settings.canada_send_number
        else:
            self.from_name = self.settings.tc_registered_originator

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
        output = (
            f'to: {sms_data.number}\n'
            f'msg id: {msg_id}\n'
            f'ts: {send_ts}\n'
            f'group_id: {self.group_id}\n'
            f'tags: {self.tags}\n'
            f'company_code: {self.m.company_code}\n'
            f'from_name: {self.from_name}\n'
            f'length: {sms_data.length}\n'
            f'message:\n'
            f'{sms_data.message}\n'
        )
        if self.settings.test_output:  # pragma: no branch
            Path.mkdir(self.settings.test_output, parents=True, exist_ok=True)
            save_path = self.settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        await self._store_sms(msg_id, send_ts, sms_data)

    async def _messagebird_send_sms(self, sms_data: SmsData):
        send_ts = utcnow()
        main_logger.info('sending SMS to %s, parts: %d', sms_data.number.number, sms_data.length.parts)

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
        await self._store_sms(data['id'], send_ts, sms_data)

    async def _store_sms(self, external_id, send_ts, sms_data: SmsData):
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
