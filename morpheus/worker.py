import base64
import json
import logging
import re
import uuid
from collections import namedtuple

import chevron
import misaka
import msgpack
from datetime import datetime
from misaka import HtmlRenderer, Markdown
from aiohttp import ClientSession
from arq import Actor, BaseWorker, Drain, concurrent
from arq.utils import to_unix_ms
from pydf import AsyncPydf

from .es import ElasticSearch
from .logs import setup_logging
from .models import SendMethod, MessageStatus
from .settings import Settings

test_logger = logging.getLogger('morpheus.test')
main_logger = logging.getLogger('morpheus.main')


class TCHtmlRenderer(HtmlRenderer, object):
    def __del__(self):  # pragma: no cover
        try:
            HtmlRenderer.__del__(self)
        except AttributeError:
            pass


class TCMarkdown:
    def __init__(self, escape=False):
        flags = misaka.HTML_HARD_WRAP
        if escape:
            flags |= misaka.HTML_ESCAPE
        render = TCHtmlRenderer(flags=flags)
        self.md = Markdown(render, extensions=misaka.EXT_NO_INTRA_EMPHASIS)

    def __call__(self, md_str):
        if isinstance(md_str, bytes):
            md_str = md_str.decode()
        md_str = re.sub(r'\r\n', '\n', md_str)
        return self.md(md_str)


markdown = TCMarkdown()

Job = namedtuple(
    'Job',
    [
        'group_id',
        'send_method',
        'first_name',
        'last_name',
        'user_id',
        'address',
        'search_tags',
        'pdf_attachments',
        'main_template',
        'markdown_template',
        'mustache_partials',
        'subject_template',
        'company_code',
        'from_email',
        'from_name',
        'reply_to',
        'subaccount',
        'analytics_tags',
        'context',
    ]
)

EmailInfo = namedtuple('EmailInfo', ['full_name', 'subject', 'html_body', 'text_body', 'signing_domain'])


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = None
        self.apydf = AsyncPydf()
        self.mandrill_send_url = self.settings.mandrill_url + '/messages/send.json'

    async def startup(self):
        self.session = ClientSession(loop=self.loop)
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)
        setup_logging(self.settings)

    async def shutdown(self):
        self.session.close()

    @concurrent
    async def send(self,
                   recipients_key, *,
                   uid,
                   main_template,
                   markdown_template,
                   mustache_partials,
                   subject_template,
                   company_code,
                   from_email,
                   from_name,
                   reply_to,
                   method,
                   subaccount,
                   analytics_tags,
                   context):
        if method == SendMethod.email_mandrill:
            coro = self._send_mandrill
        elif method == SendMethod.email_test:
            coro = self._send_test
        else:
            raise NotImplementedError()
        analytics_tags = [uid] + analytics_tags
        main_logger.info('sending group %s via %s', uid, method)
        base_kwargs = dict(
            group_id=uid,
            send_method=method,
            main_template=main_template,
            markdown_template=markdown_template,
            mustache_partials=mustache_partials,
            subject_template=subject_template,
            company_code=company_code,
            from_email=from_email,
            from_name=from_name,
            reply_to=reply_to,
            subaccount=subaccount,
            analytics_tags=analytics_tags,
        )

        drain = Drain(redis_pool=await self.get_redis_pool(), raise_task_exception=True)
        async with drain:
            async for raw_queue, raw_data in drain.iter(recipients_key):
                if not raw_queue:
                    break

                data = msgpack.unpackb(raw_data, encoding='utf8')
                data['context'] = dict(**context, **data.pop('context'))
                data.update(base_kwargs)
                drain.add(coro, Job(**data))
                # TODO stop if worker is not running

    async def _send_mandrill(self, j: Job):
        email_info = self._get_email_info(j)
        data = {
            'key': self.settings.mandrill_key,
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
                track_opens=True,
                auto_text=True,
                view_content_link=False,
                signing_domain=email_info.signing_domain,
                subaccount=j.subaccount,
                tags=j.analytics_tags,
                inline_css=True,
                # google analytics ?
                # inline_css ?,
                attachments=[dict(
                    type='application/pdf',
                    name=a['name'],
                    content=await self._generate_base64_pdf(a['html']),
                ) for a in j.pdf_attachments]
            ),
        }
        if j.reply_to:
            data['message']['headers'] = {
                'Reply-To': j.reply_to,
            }
        async with self.session.post(self.mandrill_send_url, json=data) as r:
            if r.status == 200:
                main_logger.debug('mandrill send to %s:%s, good response', j.group_id, j.address)
            else:
                text = await r.text()
                main_logger.error('mandrill error %s:%s, response: %s\n%s', j.group_id, j.address, r.status, text)
            data = await r.json()
            print(data)

    async def _send_test(self, j: Job):
        email_info = self._get_email_info(j)
        data = dict(
            subject=email_info.subject,
            from_email=j.from_email,
            from_name=j.from_name,
            reply_to=j.reply_to,
            to_email=j.address,
            to_name=email_info.full_name,
            signing_domain=email_info.signing_domain,
            tags=j.analytics_tags,
            attachments=[dict(
                type='application/pdf',
                name=a['name'],
                content=(await self._generate_base64_pdf(a['html']))[:20] + '...',
            ) for a in j.pdf_attachments]
        )
        msg_id = uuid.uuid4()
        send_ts = datetime.utcnow()
        test_logger.info(
            'sending message to %s: "%s"\nmsg id: %s\nts: %s\ndata: %s\ncontent:\n%s',
            j.address, email_info.subject, msg_id, send_ts, json.dumps(data, indent=2), email_info.html_body
        )
        await self._store_msg(msg_id, send_ts, j, email_info)

    def _get_email_info(self, j: Job) -> EmailInfo:
        full_name = f'{j.first_name} {j.last_name}'.strip(' ')
        j.context.update(
            first_name=j.first_name,
            last_name=j.last_name,
            full_name=full_name,
        )
        subject = chevron.render(j.subject_template, data=j.context)
        j.context['subject'] = subject
        raw_message = chevron.render(j.markdown_template, data=j.context, partials_dict=j.mustache_partials)
        html_message = markdown(raw_message)
        return EmailInfo(
            full_name=full_name,
            subject=subject,
            html_body=chevron.render(
                j.main_template,
                data=dict(message=html_message, **j.context),
                partials_dict=j.mustache_partials,
            ),
            text_body=raw_message,
            signing_domain=j.from_email[j.from_email.index('@') + 1:],
        )

    async def _generate_base64_pdf(self, html):
        pdf_content = await self.apydf.generate_pdf(
            html,
            page_size='A4',
            zoom='1.25',
            margin_left='8mm',
            margin_right='8mm',
        )
        return base64.b64encode(pdf_content).decode()

    async def _store_msg(self, uid, send_ts, j: Job, email_info: EmailInfo):
        ts = to_unix_ms(send_ts)[0]
        await self.es.post(
            f'messages/{j.company_code}/{uid}',
            send_ts=ts,
            update_ts=ts,
            status=MessageStatus.sent,
            group_id=j.group_id,
            to_first_name=j.first_name,
            to_last_name=j.last_name,
            to_email=j.address,
            from_email=j.from_email,
            from_name=j.from_name,
            search_tags=j.search_tags,
            analytics_tags=j.analytics_tags,
            subject=email_info.subject,
            body=email_info.html_body,
            attachments=[a['name'] for a in j.pdf_attachments],
            events=[
                dict(
                    ts=ts,
                    status=MessageStatus.sent,
                )
            ]
        )


class Worker(BaseWorker):
    shadows = [Sender]