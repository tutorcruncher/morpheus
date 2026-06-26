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
from celery import Task
from celery.exceptions import MaxRetriesExceededError
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
from pydf import generate_pdf
from sqlalchemy import text
from sqlmodel import select
from ua_parser.user_agent_parser import Parse as ParseUserAgent

from app.core.celery import celery_app
from app.core.config import settings
from app.core.database import get_session
from app.ext.clients import ApiError, Mandrill, MessageBird
from app.messages.models import Event, Link, Message, MessageStatus
from app.messages.schemas import (
    BaseWebhook,
    EmailRecipientModel,
    EmailSendMethod,
    EmailSendModel,
    MandrillWebhook,
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
            group_id, company_id, recipient_payload, m_payload = args
            recipient = EmailRecipientModel.model_validate(recipient_payload)
            m = EmailSendModel.model_validate(m_payload)
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
def send_email(self: Task, group_id: int, company_id: int, recipient: dict, m: dict) -> None:
    recipient_model = EmailRecipientModel.model_validate(recipient)
    m_model = EmailSendModel.model_validate(m)
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
        except (ConnectionError, TimeoutError, httpx.ConnectError, httpx.ReadTimeout) as e:
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
    descr: Optional[str]
    is_mobile: bool


@dataclass
class SmsData:
    number: Number
    message: str
    shortened_link: list
    length: SmsLength


MOBILE_NUMBER_TYPES = (PhoneNumberType.MOBILE, PhoneNumberType.FIXED_LINE_OR_MOBILE)


def validate_number(number: str, country: str, include_description: bool = True) -> Optional[Number]:
    try:
        p = parse_number(number, country)
    except NumberParseException:
        return None

    if not is_valid_number(p):
        return None

    is_mobile = number_type(p) in MOBILE_NUMBER_TYPES
    descr = None
    if include_description:
        country_n = country_name_for_number(p, 'en')
        region = description_for_number(p, 'en')
        descr = country_n if country_n == region else f'{region}, {country_n}'

    return Number(
        number=format_number(p, PhoneNumberFormat.E164),
        country_code=f'{p.country_code}',
        number_formatted=format_number(p, PhoneNumberFormat.INTERNATIONAL),
        descr=descr,
        is_mobile=is_mobile,
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
            group_id, company_id, recipient_payload, m_payload = args
            recipient = SmsRecipientModel.model_validate(recipient_payload)
            m = SmsSendModel.model_validate(m_payload)
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
def send_sms(self: Task, group_id: int, company_id: int, recipient: dict, m: dict) -> None:
    recipient_model = SmsRecipientModel.model_validate(recipient)
    m_model = SmsSendModel.model_validate(m)
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
        number_info = validate_number(self.recipient.number, self.m.country_code, include_description=False)
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
    from app.messages.schemas import MandrillSingleWebhook

    if method in (SendMethod.sms_messagebird, SendMethod.sms_test):
        m = MessageBirdWebHook.model_validate(payload)
    else:
        m = MandrillSingleWebhook.model_validate(payload)
    return _update_message_status(method, m)


@celery_app.task(name='app.messages.tasks.update_mandrill_webhooks')
def update_mandrill_webhooks(events: list) -> int:
    mandrill_webhook = MandrillWebhook(events=events)
    statuses: dict[str, int] = {}
    for m in mandrill_webhook.events:
        status = _update_message_status(SendMethod.email_mandrill, m, log_each=False)
        statuses[status] = statuses.get(status, 0) + 1
    main_logger.info(
        'updating %d messages: %s',
        len(mandrill_webhook.events),
        ' '.join(f'{k}={v}' for k, v in statuses.items()),
    )
    return len(mandrill_webhook.events)


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
