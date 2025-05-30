import base64
import binascii
import json
import logging
import re
from arq import Retry
from asyncio import CancelledError
from buildpg import MultipleValues, Values
from chevron import ChevronError
from concurrent.futures import TimeoutError
from datetime import datetime, timezone
from foxglove import glove
from httpcore import ReadTimeout as HttpReadTimeout
from httpx import ConnectError, ReadTimeout
from itertools import chain
from pathlib import Path
from fpdf import FPDF
from typing import List, Optional

from src.ext import ApiError
from src.render import EmailInfo, MessageDef, render_email
from src.schemas.messages import (
    THIS_DIR,
    AttachmentModel,
    EmailRecipientModel,
    EmailSendMethod,
    EmailSendModel,
    MessageStatus,
)
from src.settings import Settings

main_logger = logging.getLogger('worker.email')
test_logger = logging.getLogger('worker.test')

STYLES_SASS = (THIS_DIR / 'extra' / 'default-styles.scss').read_text()
email_retrying = [5, 10, 60, 600, 1800, 3600, 12 * 3600]


def generate_pdf_from_html(html: str, page_size='A4', zoom='1.25', margin_left='8mm', margin_right='8mm') -> bytes:
    pdf = FPDF(orientation='P', unit='mm', format=page_size.upper())
    pdf.add_page()

    left_margin = float(margin_left.replace('mm', ''))
    right_margin = float(margin_right.replace('mm', ''))

    pdf.set_left_margin(left_margin)
    pdf.set_right_margin(right_margin)
    pdf.write_html(html)

    return pdf.output()


def utcnow():
    return datetime.utcnow().replace(tzinfo=timezone.utc)


class SendEmail:
    __slots__ = 'ctx', 'settings', 'recipient', 'group_id', 'company_id', 'm', 'tags'

    def __init__(self, ctx: dict, group_id: int, company_id: int, recipient: EmailRecipientModel, m: EmailSendModel):
        self.ctx = ctx
        self.settings: Settings = ctx['settings']
        self.group_id = group_id
        self.company_id = company_id
        self.recipient: EmailRecipientModel = recipient
        self.m: EmailSendModel = m
        self.tags = list(set(self.recipient.tags + self.m.tags + [str(self.m.uid)]))

    async def run(self):
        main_logger.info('Sending email to %s via %s', self.recipient.address, self.m.method)
        if self.ctx['job_try'] > len(email_retrying):
            main_logger.error('%s: tried to send email %d times, all failed', self.group_id, self.ctx['job_try'])
            await self._store_email_failed(MessageStatus.send_request_failed, 'upstream error')
            return

        context = dict(self.m.context, **self.recipient.context)
        if 'styles__sass' not in context and re.search(r'\{\{\{ *styles *\}\}\}', self.m.main_template):
            context['styles__sass'] = STYLES_SASS

        headers = dict(self.m.headers, **self.recipient.headers)

        if self.ctx['job_try'] >= 2:
            main_logger.info('%s: rending email', self.group_id)
        email_info = await self._render_email(context, headers)
        if self.ctx['job_try'] >= 2:
            main_logger.info('%s: finished rending email', self.group_id)
        if not email_info:
            return

        if self.ctx['job_try'] >= 2:
            main_logger.info(
                '%s: generating %d PDF attachments and %d other attachments',
                self.group_id,
                len(self.recipient.pdf_attachments),
                len(self.recipient.attachments),
            )
        attachments = [a async for a in self._generate_base64_pdf(self.recipient.pdf_attachments)]
        attachments += [a async for a in self._generate_base64(self.recipient.attachments)]
        if self.ctx['job_try'] >= 2:
            main_logger.info('%s: finished generating all attachments', self.group_id)

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
            'timeout_': 15,
        }
        send_ts = utcnow()
        job_try = self.ctx['job_try']
        defer = email_retrying[job_try - 1]
        try:
            if job_try >= 2:
                main_logger.info('%s: ', self.group_id)
            r = await self.ctx['mandrill'].post('messages/send.json', **data)
            if job_try >= 2:
                main_logger.info('%s: finished sending data to mandrill', self.group_id)
        except (ConnectError, TimeoutError, ReadTimeout, HttpReadTimeout, CancelledError) as e:
            main_logger.info('client connection error group_id=%s job_try=%s defer=%ss', self.group_id, job_try, defer)
            raise Retry(defer=defer) from e
        except ApiError as e:
            if e.status in {502, 504} or (e.status == 500 and '<center>nginx/' in e.body):
                main_logger.info(
                    'temporary mandrill error group_id=%s status=%s job_try=%s defer=%ss',
                    self.group_id,
                    e.status,
                    job_try,
                    defer,
                )
                raise Retry(defer=defer) from e
            else:
                # if the status is not 502 or 504, or 500 from nginx then raise
                raise

        data = r.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == self.recipient.address, data
        await self._store_email(data['_id'], send_ts, email_info)

    async def _send_test_email(self, email_info: EmailInfo, attachments: List[dict]):
        data = dict(
            from_email=self.m.from_address.email,
            from_name=self.m.from_address.name,
            group_uuid=str(self.m.uid),
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
        msg_id = re.sub(r'[^a-zA-Z0-9\-]', '', f'{self.m.uid}-{self.recipient.address}')
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

    async def _render_email(self, context, headers) -> Optional[EmailInfo]:
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
        for a in pdf_attachments:
            if a.html:
                try:
                    pdf_content = generate_pdf_from_html(a.html, page_size='A4', zoom='1.25', margin_left='8mm', margin_right='8mm')
                except Exception as e:
                    main_logger.warning('error generating pdf, data: %s', e)
                else:
                    yield dict(type='application/pdf', name=a.name, content=base64.b64encode(pdf_content).decode())

    async def _generate_base64(self, attachments: List[AttachmentModel]):
        for attachment in attachments:
            try:
                # Check to see if content can be decoded from base64
                base64.b64decode(attachment.content, validate=True)
            except binascii.Error:
                # Content has not yet been base64 encoded so needs to be encoded
                content = base64.b64encode(attachment.content).decode()
            else:
                # Content has already been base64 encoded so just pass content through
                content = attachment.content.decode()
            yield dict(name=attachment.name, type=attachment.mime_type, content=content)

    async def _store_email(self, external_id, send_ts, email_info: EmailInfo):
        data = dict(
            external_id=external_id,
            group_id=self.group_id,
            company_id=self.company_id,
            method=self.m.method,
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
        message_id = await glove.pg.fetchval_b(
            'insert into messages (:values__names) values :values returning id', values=Values(**data)
        )
        if email_info.shortened_link:
            await glove.pg.execute_b(
                'insert into links (:values__names) values :values',
                values=MultipleValues(
                    *[Values(message_id=message_id, token=token, url=url) for url, token in email_info.shortened_link]
                ),
            )

    async def _store_email_failed(self, status: MessageStatus, error_msg):
        await glove.pg.fetchval_b(
            'insert into messages (:values__names) values :values returning id',
            values=Values(
                group_id=self.group_id,
                company_id=self.company_id,
                method=self.m.method,
                status=status,
                to_first_name=self.recipient.first_name,
                to_last_name=self.recipient.last_name,
                to_user_link=self.recipient.user_link,
                to_address=self.recipient.address,
                tags=self.tags,
                body=error_msg,
            ),
        )


async def send_email(ctx, group_id: int, company_id: int, recipient: EmailRecipientModel, m: EmailSendModel):
    s = SendEmail(ctx, group_id, company_id, recipient, m)
    return await s.run()
