import base64
import binascii
import hashlib
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from itertools import chain
from pathlib import Path
from typing import Optional

import chevron
import httpx
import redis as redis_lib
import sentry_sdk
from celery import Task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
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
from pydantic import ValidationError
from pydf import generate_pdf
from sqlalchemy import delete, func, text
from sqlmodel import select
from ua_parser.user_agent_parser import Parse as ParseUserAgent

from app.core.celery import celery_app
from app.core.config import settings
from app.core.database import get_session
from app.ext.clients import ApiError, Mandrill, MessageBird, post_chat_reply
from app.inbound.reply import read_reply
from app.inbound.sender_proof import DkimDnsError
from app.messages.models import DELETED_COMPANY_PREFIX, Company, Event, Link, Message, MessageGroup, MessageStatus
from app.messages.schemas import (
    BaseWebhook,
    EmailRecipientModel,
    EmailSendMethod,
    EmailSendModel,
    MandrillSingleWebhook,
    MessageBirdWebHook,
    SendMethod,
    SmsRecipientModel,
    SmsSendMethod,
    SmsSendModel,
)
from app.render.main import (
    EmailInfo,
    MessageDef,
    MessageTooLong,
    SmsLength,
    apply_short_links,
    render_email,
    sms_length,
)

main_logger = logging.getLogger('worker')
test_logger = logging.getLogger('worker.test')

EXTRA_DIR = Path(__file__).parent.parent / 'extra'
STYLES_SASS = (EXTRA_DIR / 'default-styles.scss').read_text()
EMAIL_RETRYING = [5, 10, 60, 600, 1800, 3600, 12 * 3600]
EMAIL_CLICK_URL = f'https://{settings.click_host_name}/l'
SMS_CLICK_URL = f'{settings.click_host_name}/l'

_redis_client: redis_lib.Redis | None = None


def get_redis() -> redis_lib.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis_lib.Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


# A bulk send stores its shared message (m_base) once instead of in every queued task: per-recipient
# copies of a few large sends filled Redis to maxmemory. A day covers the full email retry chain
# (EMAIL_RETRYING, ~13h41m) with ~10h to spare for queue backlog, then the key expires on its own.
SEND_BASE_TTL = 86400


class SendBaseMissing(RuntimeError):
    pass


def _send_base_key(group_id: int) -> str:
    return f'send-base:{group_id}'


def share_send_base(group_id: int, m_base: dict, recipient_count: int) -> tuple[dict, ...]:
    """Return the trailing send_email/send_sms args for each recipient of a send.

    Bulk sends store m_base once and pass nothing; a single-recipient send keeps m_base inline, the
    form tasks queued by older releases use, so its copy is freed as soon as it is sent.
    """
    if recipient_count <= 1:
        return (m_base,)
    get_redis().set(_send_base_key(group_id), json.dumps(m_base), ex=SEND_BASE_TTL)
    return ()


def load_send_base(group_id: int, m: dict | None = None) -> dict:
    if m is not None:
        return m
    raw = get_redis().get(_send_base_key(group_id))
    if raw is None:
        raise SendBaseMissing(f'stored message for group {group_id} has expired or was never written')
    return json.loads(raw)


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------- Email ----------------


class _SendEmailTask(Task):
    """Celery Task subclass so we can catch retry exhaustion and write a failed Message row.

    Celery raises MaxRetriesExceededError instead of re-running the task body after the final
    retry, so the in-body `if job_try > len(EMAIL_RETRYING)` branch can never store the failure.
    on_failure runs after retry exhaustion (and any other unhandled exception).
    """

    def on_failure(self, exc, task_id, args, kwargs, einfo):  # noqa: D401
        if not isinstance(exc, MaxRetriesExceededError):
            return
        try:
            group_id, company_id, recipient_payload, *m_payload = args
            recipient = EmailRecipientModel.model_validate(recipient_payload)
            m = EmailSendModel.model_validate(load_send_base(group_id, *m_payload))
        except Exception:
            main_logger.exception('failed to record send_email exhaustion for task %s', task_id)
            return
        tags = list(set(recipient.tags + m.tags + [str(m.uid)]))
        with get_session() as db:
            db.add(
                Message(
                    group_id=group_id,
                    company_id=company_id,
                    method=m.method.value,
                    status=MessageStatus.send_request_failed.value,
                    to_first_name=recipient.first_name,
                    to_last_name=recipient.last_name,
                    to_user_link=recipient.user_link,
                    to_address=recipient.address,
                    tags=tags,
                    body='upstream error',
                )
            )
            db.commit()


@celery_app.task(
    name='app.messages.tasks.send_email',
    base=_SendEmailTask,
    bind=True,
    max_retries=len(EMAIL_RETRYING),
)
def send_email(self: Task, group_id: int, company_id: int, recipient: dict, m: dict | None = None) -> None:
    recipient_model = EmailRecipientModel.model_validate(recipient)
    m_model = EmailSendModel.model_validate(load_send_base(group_id, m))
    SendEmail(self, group_id, company_id, recipient_model, m_model).run()


class SendEmail:
    def __init__(
        self,
        task: Task,
        group_id: int,
        company_id: int,
        recipient: EmailRecipientModel,
        m: EmailSendModel,
    ):
        self.task = task
        self.group_id = group_id
        self.company_id = company_id
        self.recipient = recipient
        self.m = m
        self.tags = list(set(self.recipient.tags + self.m.tags + [str(self.m.uid)]))
        self.job_try = (task.request.retries or 0) + 1

    def run(self) -> None:
        main_logger.info('Sending email to %s via %s', self.recipient.address, self.m.method)
        # Retry exhaustion is normally handled by _SendEmailTask.on_failure (celery raises
        # MaxRetriesExceededError instead of re-invoking the body once max_retries is reached).
        # This guard exists for the direct-call path used by worker_send_email tests, which
        # manually pass job_try beyond the retry budget.
        if self.job_try > len(EMAIL_RETRYING):
            main_logger.error('%s: tried to send email %d times, all failed', self.group_id, self.job_try)
            self._store_email_failed(MessageStatus.send_request_failed.value, 'upstream error')
            return

        context = dict(self.m.context, **self.recipient.context)
        if 'styles__sass' not in context and re.search(r'\{\{\{ *styles *\}\}\}', self.m.main_template):
            context['styles__sass'] = STYLES_SASS

        headers = dict(self.m.headers, **self.recipient.headers)

        email_info = self._render_email(context, headers)
        if not email_info:
            return

        attachments = list(self._generate_base64_pdf(self.recipient.pdf_attachments))
        attachments += list(self._generate_base64(self.recipient.attachments))

        if self.m.method == EmailSendMethod.email_mandrill:
            if self.recipient.address.endswith('@example.com'):
                _id = re.sub(r'[^a-zA-Z0-9\-]', '', f'mandrill-{self.recipient.address}')
                self._store_email(_id, utcnow(), email_info)
            else:
                self._send_mandrill(email_info, attachments)
        elif self.m.method == EmailSendMethod.email_test:
            self._send_test_email(email_info, attachments)
        else:
            raise NotImplementedError()

    def _send_mandrill(self, email_info: EmailInfo, attachments: list[dict]) -> None:
        from_email = self.m.from_address.email
        data = {
            'async': True,
            'message': dict(
                html=email_info.html_body,
                subject=email_info.subject,
                from_email=from_email,
                from_name=self.m.from_address.name,
                to=[dict(email=self.recipient.address, name=email_info.full_name, type='to')],
                headers=email_info.headers,
                track_opens=True,
                track_clicks=False,
                auto_text=True,
                view_content_link=False,
                signing_domain=from_email[from_email.index('@') + 1 :],
                subaccount=self.m.subaccount,
                tags=self.tags,
                inline_css=True,
                important=self.m.important,
                attachments=attachments,
            ),
            'timeout_': 15,
        }
        send_ts = utcnow()
        defer = EMAIL_RETRYING[self.job_try - 1]
        try:
            r = Mandrill().post('messages/send.json', **data)
        # httpx.TransportError covers all connection/timeout failures (ConnectTimeout,
        # ReadTimeout, ConnectError, ...) — previously ConnectTimeout slipped through and
        # the task failed without retrying, silently dropping the email.
        except (ConnectionError, TimeoutError, httpx.TransportError) as e:
            main_logger.info(
                'client connection error group_id=%s job_try=%s defer=%ss',
                self.group_id,
                self.job_try,
                defer,
            )
            raise self.task.retry(exc=e, countdown=defer)
        except ApiError as e:
            if e.status in {502, 504} or (e.status == 500 and '<center>nginx/' in (e.body or '')):
                main_logger.info(
                    'temporary mandrill error group_id=%s status=%s job_try=%s defer=%ss',
                    self.group_id,
                    e.status,
                    self.job_try,
                    defer,
                )
                raise self.task.retry(exc=e, countdown=defer)
            # Non-retryable Mandrill error (e.g. 400/401/422): record a failure row so the
            # send is not silently lost. The old arq path reached send_request_failed via
            # retry exhaustion; here we store it directly without the wasted retries.
            main_logger.warning('non-retryable mandrill error group_id=%s status=%s', self.group_id, e.status)
            self._store_email_failed(MessageStatus.send_request_failed.value, 'upstream error')
            return

        data = r.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == self.recipient.address, data
        self._store_email(data['_id'], send_ts, email_info)

    def _send_test_email(self, email_info: EmailInfo, attachments: list[dict]) -> None:
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
        if settings.test_output:  # pragma: no branch
            Path.mkdir(settings.test_output, parents=True, exist_ok=True)
            save_path = settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        self._store_email(msg_id, send_ts, email_info)

    def _render_email(self, context: dict, headers: dict) -> Optional[EmailInfo]:
        m = MessageDef(
            first_name=self.recipient.first_name,  # ty:ignore[invalid-argument-type]
            last_name=self.recipient.last_name,  # ty:ignore[invalid-argument-type]
            main_template=self.m.main_template,
            mustache_partials=self.m.mustache_partials or {},  # ty:ignore[invalid-argument-type]
            macros=self.m.macros,  # ty:ignore[invalid-argument-type]
            subject_template=self.m.subject_template,
            context=context,
            headers=headers,
        )
        try:
            return render_email(m, EMAIL_CLICK_URL)
        except ChevronError as e:
            self._store_email_failed(MessageStatus.render_failed.value, f'Error rendering email: {e}')
            return None

    @staticmethod
    def _generate_base64_pdf(pdf_attachments):
        kwargs = dict(page_size='A4', zoom='1.25', margin_left='8mm', margin_right='8mm')
        for a in pdf_attachments:  # pragma: no cover  -- requires arch-specific wkhtmltopdf
            if a.html:
                try:
                    pdf_content = generate_pdf(a.html, **kwargs)  # ty:ignore[invalid-argument-type]
                except RuntimeError as e:
                    main_logger.warning('error generating pdf, data: %s', e)
                else:
                    yield dict(type='application/pdf', name=a.name, content=base64.b64encode(pdf_content).decode())

    @staticmethod
    def _generate_base64(attachments):
        for attachment in attachments:
            try:
                base64.b64decode(attachment.content, validate=True)
            except binascii.Error:
                content = base64.b64encode(attachment.content).decode()
            else:
                content = attachment.content.decode()
            yield dict(name=attachment.name, type=attachment.mime_type, content=content)

    def _store_email(self, external_id: str, send_ts: datetime, email_info: EmailInfo) -> None:
        attachments = [
            f'{getattr(a, "id", None) or ""}::{a.name}'
            for a in chain(self.recipient.pdf_attachments, self.recipient.attachments)
        ]
        with get_session() as db:
            msg = Message(
                external_id=external_id,
                group_id=self.group_id,
                company_id=self.company_id,
                method=self.m.method.value,
                send_ts=send_ts,
                status=MessageStatus.send.value,
                to_first_name=self.recipient.first_name,
                to_last_name=self.recipient.last_name,
                to_user_link=self.recipient.user_link,
                to_address=self.recipient.address,
                tags=self.tags,
                subject=email_info.subject,
                body=email_info.html_body,
                attachments=attachments or None,
            )
            db.add(msg)
            db.commit()
            db.refresh(msg)
            if email_info.shortened_link:
                for url, token in email_info.shortened_link:
                    db.add(Link(message_id=msg.id, token=token, url=url))  # ty:ignore[invalid-argument-type]
                db.commit()

    def _store_email_failed(self, status: str, error_msg: str) -> None:
        with get_session() as db:
            db.add(
                Message(
                    group_id=self.group_id,
                    company_id=self.company_id,
                    method=self.m.method.value,
                    status=status,
                    to_first_name=self.recipient.first_name,
                    to_last_name=self.recipient.last_name,
                    to_user_link=self.recipient.user_link,
                    to_address=self.recipient.address,
                    tags=self.tags,
                    body=error_msg,
                )
            )
            db.commit()


# ---------------- SMS ----------------


@dataclass
class Number:
    number: str
    country_code: str
    number_formatted: str
    is_mobile: bool


@dataclass
class SmsData:
    number: Number
    message: str
    shortened_link: list
    length: SmsLength


MOBILE_NUMBER_TYPES = (PhoneNumberType.MOBILE, PhoneNumberType.FIXED_LINE_OR_MOBILE)


def validate_number(number: str, country: str) -> Optional[Number]:
    try:
        p = parse_number(number, country)
    except NumberParseException:
        return None

    if not is_valid_number(p):
        return None

    return Number(
        number=format_number(p, PhoneNumberFormat.E164),
        country_code=f'{p.country_code}',
        number_formatted=format_number(p, PhoneNumberFormat.INTERNATIONAL),
        is_mobile=number_type(p) in MOBILE_NUMBER_TYPES,
    )


SMS_MAX_RETRIES = 4
SMS_RETRY_DELAY = 60


class _SendSMSTask(Task):
    """Celery Task subclass so a failed Message row is written on retry exhaustion.

    Mirrors _SendEmailTask: without this, a MessageBird outage would silently drop the SMS
    (arq used to auto-retry any task exception; Celery does not).
    """

    def on_failure(self, exc, task_id, args, kwargs, einfo):  # noqa: D401
        if not isinstance(exc, MaxRetriesExceededError):
            return
        try:
            group_id, company_id, recipient_payload, *m_payload = args
            recipient = SmsRecipientModel.model_validate(recipient_payload)
            m = SmsSendModel.model_validate(load_send_base(group_id, *m_payload))
        except Exception:
            main_logger.exception('failed to record send_sms exhaustion for task %s', task_id)
            return
        tags = list(set(recipient.tags + m.tags + [str(m.uid)]))
        with get_session() as db:
            db.add(
                Message(
                    group_id=group_id,
                    company_id=company_id,
                    method=m.method.value,
                    status=MessageStatus.send_request_failed.value,
                    to_first_name=recipient.first_name,
                    to_last_name=recipient.last_name,
                    to_user_link=recipient.user_link,
                    to_address=recipient.number,
                    tags=tags,
                    body='upstream error',
                )
            )
            db.commit()


@celery_app.task(
    name='app.messages.tasks.send_sms',
    base=_SendSMSTask,
    bind=True,
    max_retries=SMS_MAX_RETRIES,
)
def send_sms(self: Task, group_id: int, company_id: int, recipient: dict, m: dict | None = None) -> None:
    recipient_model = SmsRecipientModel.model_validate(recipient)
    m_model = SmsSendModel.model_validate(load_send_base(group_id, m))
    SendSMS(self, group_id, company_id, recipient_model, m_model).run()


class SendSMS:
    def __init__(self, task: Task, group_id: int, company_id: int, recipient: SmsRecipientModel, m: SmsSendModel):
        self.task = task
        self.group_id = group_id
        self.company_id = company_id
        self.recipient = recipient
        self.m = m
        self.tags = list(set(self.recipient.tags + self.m.tags + [str(self.m.uid)]))
        if self.m.country_code == 'US':
            self.from_name = settings.us_send_number
        elif self.m.country_code == 'CA':
            self.from_name = settings.canada_send_number
        elif self.m.country_code == 'AU':
            # ACMA's SMS Sender ID Register (mandatory since 1 July 2026) labels unregistered
            # alphanumeric sender IDs "Unverified"; a numeric originator is exempt. See #554.
            self.from_name = settings.australia_send_number
        else:
            self.from_name = settings.tc_registered_originator

    def run(self) -> None:
        sms_data = self._sms_prep()
        if not sms_data:
            return

        if self.m.method == SmsSendMethod.sms_test:
            self._test_send_sms(sms_data)
        elif self.m.method == SmsSendMethod.sms_messagebird:
            self._messagebird_send_sms(sms_data)
        else:
            raise NotImplementedError()

    def _sms_prep(self) -> Optional[SmsData]:
        number_info = validate_number(self.recipient.number, self.m.country_code)
        msg, error, shortened_link, msg_length = None, None, None, None
        if not number_info or not number_info.is_mobile:
            error = f'invalid mobile number "{self.recipient.number}"'
            main_logger.warning(
                'invalid mobile number "%s" for "%s", not sending', self.recipient.number, self.m.company_code
            )
        else:
            context = dict(self.m.context, **self.recipient.context)
            shortened_link = apply_short_links(context, SMS_CLICK_URL, 12)
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
            with get_session() as db:
                db.add(
                    Message(
                        group_id=self.group_id,
                        company_id=self.company_id,
                        method=self.m.method.value,
                        status=MessageStatus.render_failed.value,
                        to_first_name=self.recipient.first_name,
                        to_last_name=self.recipient.last_name,
                        to_user_link=self.recipient.user_link,
                        to_address=number_info.number_formatted if number_info else self.recipient.number,
                        tags=self.tags,
                        body=error,
                    )
                )
                db.commit()
            return None
        return SmsData(number=number_info, message=msg, shortened_link=shortened_link, length=msg_length)  # ty:ignore[invalid-argument-type]

    def _test_send_sms(self, sms_data: SmsData) -> None:
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
        if settings.test_output:  # pragma: no branch
            Path.mkdir(settings.test_output, parents=True, exist_ok=True)
            save_path = settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        self._store_sms(msg_id, send_ts, sms_data)

    def _messagebird_send_sms(self, sms_data: SmsData) -> None:
        send_ts = utcnow()
        main_logger.info('sending SMS to %s, parts: %d', sms_data.number.number, sms_data.length.parts)

        try:
            r = MessageBird().post(
                'messages',
                originator=self.from_name,
                body=sms_data.message,
                recipients=[sms_data.number.number],
                datacoding='auto',
                reference='morpheus',
                allowed_statuses=201,
            )
        except (ConnectionError, TimeoutError, httpx.ConnectError, httpx.ReadTimeout) as e:
            main_logger.info('messagebird connection error group_id=%s', self.group_id)
            raise self.task.retry(exc=e, countdown=SMS_RETRY_DELAY)
        except ApiError as e:
            if e.status is None or e.status >= 500:
                main_logger.info('temporary messagebird error group_id=%s status=%s', self.group_id, e.status)
                raise self.task.retry(exc=e, countdown=SMS_RETRY_DELAY)
            # Non-retryable (4xx) error: record a failure row instead of silently dropping the SMS.
            main_logger.warning('non-retryable messagebird error group_id=%s status=%s', self.group_id, e.status)
            self._store_sms_failed('upstream error', address=sms_data.number.number_formatted)
            return
        data = r.json()
        if data['recipients']['totalCount'] != 1:  # pragma: no cover  -- upstream invariant breach
            main_logger.error('not one recipients in send response', extra={'data': data})
        self._store_sms(data['id'], send_ts, sms_data)

    def _store_sms_failed(self, error_msg: str, *, address: Optional[str] = None) -> None:
        with get_session() as db:
            db.add(
                Message(
                    group_id=self.group_id,
                    company_id=self.company_id,
                    method=self.m.method.value,
                    status=MessageStatus.send_request_failed.value,
                    to_first_name=self.recipient.first_name,
                    to_last_name=self.recipient.last_name,
                    to_user_link=self.recipient.user_link,
                    to_address=address or self.recipient.number,
                    tags=self.tags,
                    body=error_msg,
                )
            )
            db.commit()

    def _store_sms(self, external_id: str, send_ts: datetime, sms_data: SmsData) -> None:
        with get_session() as db:
            msg = Message(
                external_id=external_id,
                group_id=self.group_id,
                company_id=self.company_id,
                method=self.m.method.value,
                send_ts=send_ts,
                status=MessageStatus.send.value,
                to_first_name=self.recipient.first_name,
                to_last_name=self.recipient.last_name,
                to_user_link=self.recipient.user_link,
                to_address=sms_data.number.number_formatted,
                tags=self.tags,
                body=sms_data.message,
                extra=asdict(sms_data.length),
            )
            db.add(msg)
            db.commit()
            db.refresh(msg)
            if sms_data.shortened_link:
                for url, token in sms_data.shortened_link:
                    db.add(Link(message_id=msg.id, token=token, url=url))  # ty:ignore[invalid-argument-type]
                db.commit()


# ---------------- Webhooks ----------------


def _to_unix_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _update_message_status(send_method: SendMethod, m: BaseWebhook, log_each: bool = True) -> str:
    h = hashlib.md5(f'{m.message_id}-{_to_unix_ms(m.ts)}-{m.status}-{m.extra_json(sort_keys=True)}'.encode())
    ref = f'event-{h.hexdigest()}'
    redis = get_redis()
    if not redis.set(ref, '1', ex=86400, nx=True):
        if log_each:
            main_logger.info('event already exists %s, ts: %s, status: %s. skipped', m.message_id, m.ts, m.status)
        return 'duplicate'

    with get_session() as db:
        message = db.exec(
            select(Message).where(Message.external_id == m.message_id, Message.method == send_method.value)
        ).first()
        if not message:
            return 'missing'

        ts = m.ts if m.ts.tzinfo else m.ts.replace(tzinfo=timezone.utc)

        if log_each:
            main_logger.info('adding event %s, ts: %s, status: %s', m.message_id, ts, m.status)

        status_value = m.status.value if hasattr(m.status, 'value') else m.status
        db.add(Event(message_id=message.id, status=status_value, ts=ts, extra=json.loads(m.extra_json())))  # ty:ignore[invalid-argument-type]
        if isinstance(m, MessageBirdWebHook) and m.price_amount is not None:
            message.cost = m.price_amount
            db.add(message)
        db.commit()

    return 'added'


@celery_app.task(name='app.messages.tasks.update_message_status')
def update_message_status(send_method: str, payload: dict) -> str:
    method = SendMethod(send_method)
    if method in (SendMethod.sms_messagebird, SendMethod.sms_test):
        m = MessageBirdWebHook.model_validate(payload)
    else:
        m = MandrillSingleWebhook.model_validate(payload)
    return _update_message_status(method, m)


@celery_app.task(name='app.messages.tasks.update_mandrill_webhooks')
def update_mandrill_webhooks(events: list) -> int:
    statuses: dict[str, int] = {}
    for event in events:
        # Validate each event individually so one unrecognised event type (Mandrill adds
        # them without notice, e.g. 'delivered') doesn't fail the whole batch.
        try:
            m = MandrillSingleWebhook.model_validate(event)
        except ValidationError as e:
            main_logger.warning('ignoring invalid mandrill webhook event: %s', e)
            statuses['ignored'] = statuses.get('ignored', 0) + 1
            continue
        status = _update_message_status(SendMethod.email_mandrill, m, log_each=False)
        statuses[status] = statuses.get(status, 0) + 1
    main_logger.info(
        'updating %d messages: %s',
        len(events),
        ' '.join(f'{k}={v}' for k, v in statuses.items()),
    )
    return len(events)


@celery_app.task(name='app.messages.tasks.store_click')
def store_click(link_id: int, ip: Optional[str], user_agent: Optional[str], ts: float) -> Optional[str]:
    cache_key = f'click-{link_id}-{ip}'
    redis = get_redis()
    if not redis.set(cache_key, '1', ex=60, nx=True):
        return 'recently_clicked'

    with get_session() as db:
        link = db.exec(select(Link).where(Link.id == link_id)).first()
        if not link:
            return None
        message_id = link.message_id
        url = link.url

        extra = {'target': url, 'ip': ip, 'user_agent': user_agent}
        if user_agent:
            ua_dict = ParseUserAgent(user_agent)
            platform = ua_dict['device']['family']
            if platform in {'Other', None}:
                platform = ua_dict['os']['family']
            extra['user_agent_display'] = '{user_agent[family]} {user_agent[major]} on {platform}'.format(
                platform=platform, **ua_dict
            ).strip(' ')

        # Heroku's router sets X-Request-Start in epoch milliseconds. pydantic's old
        # parse_datetime auto-scaled values above its watershed (~2e10) from ms to s;
        # fromtimestamp does not, so scale ms→s here to avoid a year-out-of-range crash.
        if ts > 2e10:
            ts /= 1000
        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        db.add(Event(message_id=message_id, status='click', ts=ts_dt, extra=extra))
        db.commit()
    return None


# ---------------- Scheduler ----------------


@celery_app.task(name='app.messages.tasks.update_aggregation_view')
def update_aggregation_view() -> None:
    if not settings.update_aggregation_view:
        main_logger.info('settings.update_aggregation_view False, not running')
        return
    with get_session() as db:
        db.execute(text('refresh materialized view message_aggregation'))  # ty:ignore[deprecated]
        db.commit()


@celery_app.task(name='app.messages.tasks.delete_old_emails')
def delete_old_emails() -> None:
    if not settings.delete_old_emails:
        main_logger.info('settings.delete_old_emails False, not running')
        return
    cutoff = datetime.now() - timedelta(days=365)
    with get_session() as db:
        result = db.execute(  # ty:ignore[deprecated]
            text('delete from message_groups where id in (select id from message_groups where created_ts < :cutoff)'),
            {'cutoff': cutoff},
        )
        db.commit()
        main_logger.info('deleted %s old messages', result.rowcount)  # ty:ignore[unresolved-attribute]


@celery_app.task(name='app.messages.tasks.delete_company_messages')
def delete_company_messages(company_ids: list[int], company_code: str) -> str:
    """Delete the message history of companies whose subaccount has been deleted.

    Production carries the legacy ON DELETE RESTRICT constraints on messages.company_id and
    message_groups.company_id, so the children go before the companies rather than relying on a
    cascade; events and links do cascade off messages.
    """
    with get_session() as db:
        m_count = db.execute(delete(Message).where(Message.company_id.in_(company_ids))).rowcount  # ty:ignore[deprecated, unresolved-attribute]
        g_count = db.execute(delete(MessageGroup).where(MessageGroup.company_id.in_(company_ids))).rowcount  # ty:ignore[deprecated, unresolved-attribute]
        db.execute(delete(Company).where(Company.id.in_(company_ids)))  # ty:ignore[deprecated, unresolved-attribute]
        db.commit()
    msg_summary = f'deleted_messages={m_count} deleted_message_groups={g_count}'
    main_logger.info('deleted company=%s companies=%s %s', company_code, company_ids, msg_summary)
    return msg_summary


@celery_app.task(name='app.messages.tasks.purge_deleted_companies')
def purge_deleted_companies() -> int:
    """Re-queue purges that never reached a worker.

    delete_subaccount renames a company before queueing its purge, so a task lost in between (a
    broker blip, or a worker still on the previous release that does not know the task name) would
    otherwise leave the row under a code no later delete can match. Re-running a purge that did
    land deletes nothing.

    The match is the exact code the rename writes, not its prefix: /send/ creates a company for
    whatever code it is handed, so a prefix would put a live company that merely looks tombstoned
    into the delete path.
    """
    with get_session() as db:
        tombstone = func.concat(DELETED_COMPANY_PREFIX, Company.id)
        company_ids = db.exec(select(Company.id).where(Company.code == tombstone)).all()
    for company_id in company_ids:
        delete_company_messages.delay([company_id], 'sweep')
        main_logger.info('re-queued stranded purge for company=%s', company_id)
    return len(company_ids)


# a reply whose signing key could not be looked up is read again this many times, this far apart
INBOUND_DNS_RETRIES = 3
INBOUND_DNS_RETRY_SECONDS = 120
# how long after each failed post to TutorCruncher the next is made: about 1h20m in all, which covers a deploy
# The last is as long as Celery's default visibility timeout on Redis, so a worker other than the one holding
# that retry can be handed it too and the post made twice: TutorCruncher drops the second by its key.
CHAT_REPLY_RETRYING = [10, 60, 300, 900, 3600]
# These tasks share a queue and worker children with every email and SMS send. Reading an email is bounded by
# the limits in `app.inbound`, and a post to TutorCruncher by its timeout; the limits here are for whatever
# those miss. The soft one raises in the task, which reports it; the hard one ends the child.
READ_REPLY_TIME_LIMITS = {'soft_time_limit': 60, 'time_limit': 90}
DELIVER_REPLY_TIME_LIMITS = {'soft_time_limit': 45, 'time_limit': 60}
PRIVATE_ARGS = '[Filtered]'


class ChatReplyNotDelivered(Exception):
    """A post to TutorCruncher that is tried again: it timed out, did not connect or was answered 5xx."""


class _PrivateArgsTask(Task):
    """
    A task whose arguments are never written to a log. Celery sends the start of every task's arguments with it
    (`argsrepr`) and puts that in the records it logs when the task is received, succeeds or fails, which the
    Logfire handler forwards. Every way of queueing a task, a retry included, goes through `apply_async`.
    """

    def apply_async(self, args=None, kwargs=None, **options):  # ty:ignore[invalid-method-override]
        options.update(argsrepr=PRIVATE_ARGS, kwargsrepr=PRIVATE_ARGS)
        return super().apply_async(args, kwargs, **options)


# Neither task logs the reply address, the sender, the subject or the text, and neither lets an exception that
# could quote the email leave it: the address posts to a chat as its owner, and the rest is what somebody wrote.
@celery_app.task(
    name='app.messages.tasks.read_inbound_reply',
    base=_PrivateArgsTask,
    bind=True,
    max_retries=INBOUND_DNS_RETRIES,
    **READ_REPLY_TIME_LIMITS,
)
def read_inbound_reply(self: Task, raw_message: str, reply_address: str, received_at: str) -> None:
    """
    Read one email Mandrill received at a chat reply address, and queue what was read of it for TutorCruncher.
    The email itself goes no further than this task: `deliver_chat_reply` is a second task so that a retried
    post to TutorCruncher neither holds the email in the queue nor looks its signing key up again.
    """
    final_try = (self.request.retries or 0) >= INBOUND_DNS_RETRIES
    try:
        payload = read_reply(raw_message, reply_address, datetime.fromisoformat(received_at), final_try=final_try)
    except DkimDnsError as e:
        # its message is the name of the DNS error and nothing else
        raise self.retry(exc=e, countdown=INBOUND_DNS_RETRY_SECONDS)
    except Exception as e:
        # Not raised: Celery logs a failed task's exception message and Logfire records it on the task's span,
        # and a parser's message can quote the email. Sentry gets it through `before_send`, which filters it.
        # Logged as a warning, as an error would be a second Sentry event for the one failure. `Exception`
        # takes in the soft time limit.
        sentry_sdk.capture_exception(e)
        main_logger.warning('inbound reply failed: %s', type(e).__name__)
        return
    if payload is None:
        main_logger.info('inbound reply dropped: auto_reply')
    else:
        deliver_chat_reply.delay(payload)


@celery_app.task(
    name='app.messages.tasks.deliver_chat_reply',
    base=_PrivateArgsTask,
    bind=True,
    max_retries=len(CHAT_REPLY_RETRYING),
    **DELIVER_REPLY_TIME_LIMITS,
)
def deliver_chat_reply(self: Task, payload: dict) -> None:
    """
    Post what `read_inbound_reply` read of one email to TutorCruncher, which decides whether it becomes a chat
    message. TutorCruncher drops a repeat of the same `idempotency_key`, so a post is safe to make again.
    """
    key = str(payload.get('idempotency_key'))[:12]
    if not settings.inbound_replies_set_up:
        main_logger.error('inbound reply %s: not delivered, inbound replies are not set up', key)
        return
    try:
        r = post_chat_reply(payload)
    except (httpx.TransportError, SoftTimeLimitExceeded) as e:
        # the soft time limit is TutorCruncher answering a little at a time for longer than any one read waits
        failure = type(e).__name__
    except Exception as e:
        # not raised, for the reason given in `read_inbound_reply`
        sentry_sdk.capture_exception(e)
        main_logger.warning('inbound reply %s: not delivered, %s', key, type(e).__name__)
        return
    else:
        if r.status_code == 200:
            try:
                answer = r.json()
            except ValueError:
                answer = None
            if not isinstance(answer, dict):
                answer = {}
            outcome, reason = str(answer.get('outcome'))[:40], str(answer.get('reason') or '')[:40]
            main_logger.info('inbound reply %s: outcome=%s reason=%s', key, outcome, reason)
            return
        elif r.status_code < 500:
            # 400 is a body TutorCruncher does not accept and 403 a signature it does not: the same post again
            # would get the same answer. Its endpoint answers 200 or one of those; anything else under 500 is
            # not it answering, and is not retried either
            main_logger.error('inbound reply %s: not delivered, TutorCruncher answered %s', key, r.status_code)
            return
        failure = f'status {r.status_code}'
    retries = self.request.retries or 0
    if retries >= len(CHAT_REPLY_RETRYING):
        main_logger.error('inbound reply %s: not delivered after %s tries, last failure %s', key, retries + 1, failure)
        return
    raise self.retry(exc=ChatReplyNotDelivered(failure), countdown=CHAT_REPLY_RETRYING[retries])
