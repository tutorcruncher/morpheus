import asyncio
import base64
import json
import logging
import re
from collections import namedtuple
from datetime import datetime
from pathlib import Path

import chevron
import misaka
import msgpack
from aiohttp import ClientSession
from arq import Actor, BaseWorker, Drain, concurrent
from misaka import HtmlRenderer, Markdown

from .es import ElasticSearch
from .models import MessageStatus, SendMethod
from .settings import Settings
from .utils import ApiSession

test_logger = logging.getLogger('morpheus.worker.test')
main_logger = logging.getLogger('morpheus.worker')


markdown = Markdown(HtmlRenderer(flags=[misaka.HTML_HARD_WRAP]), extensions=[misaka.EXT_NO_INTRA_EMPHASIS])

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


class Mandrill(ApiSession):
    def __init__(self, settings, loop):
        super().__init__(settings.mandrill_url, settings, loop)

    def _modify_request(self, method, url, data):
        data['key'] = self.settings.mandrill_key
        return method, url, data


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = self.es = self.mandrill = None
        self.mandrill_webhook_auth_key = None
        self.mandrill_webhook_url = f'https://{self.settings.host_name}/webhook/mandrill/'

    async def startup(self):
        main_logger.info('Sender initialising session and elasticsearch...')
        self.session = ClientSession(loop=self.loop)
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)
        self.mandrill = Mandrill(settings=self.settings, loop=self.loop)

    async def shutdown(self):
        self.session.close()
        self.es.close()
        self.mandrill.close()

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

                data = msgpack.unpackb(raw_data, encoding='utf8')
                data['context'] = dict(**context, **data.pop('context'))
                data.update(base_kwargs)
                drain.add(coro, Job(**data))
                # TODO stop if worker is not running
                jobs += 1
        return jobs

    async def _check_morpheus_up(self):
        for i in range(10):
            async with self.session.head(self.mandrill_webhook_url) as r:
                if r.status == 200:
                    return
            main_logger.info('morpheus api not yet available %d...', i)
            await asyncio.sleep(1)
        raise RuntimeError("morpheus API does not appear to be responding, can't create webhook")

    @concurrent
    async def setup_mandrill_webhook(self):
        if not self.settings.mandrill_key:
            return 0
        r = await self.mandrill.get('webhooks/list.json')
        if r.status != 200:
            raise RuntimeError('invalid mandrill webhook list response {}:\n{}'.format(r.status, await r.text()))
        for hook in await r.json():
            if hook['url'] == self.mandrill_webhook_url:
                self.mandrill_webhook_auth_key = hook['auth_key']
                main_logger.info('using existing mandrill webhook "%s", key %s', hook['description'],
                                 self.mandrill_webhook_auth_key)
                return 200

        main_logger.info('about to create webhook entry via API, checking morpheus API is up...')
        await self._check_morpheus_up()
        data = {
            'url': self.mandrill_webhook_url,
            'description': 'morpheus - auto created',
            # infuriatingly this list appears to differ from those the api returns or actually submits in hooks
            'events': (
               'send', 'hard_bounce', 'soft_bounce', 'open', 'click', 'spam', 'unsub', 'reject',
               'blacklist', 'whitelist'
            ),
        }
        r = await self.mandrill.post('webhooks/add.json', **data)
        if r.status != 200:
            raise RuntimeError('invalid mandrill webhook list response {}:\n{}'.format(r.status, await r.text()))
        data = await r.json()
        self.mandrill_webhook_auth_key = data['auth_key']
        main_logger.info('created new mandrill webhook "%s", key %s', data['description'],
                         self.mandrill_webhook_auth_key)
        return 201

    async def _send_mandrill(self, j: Job):
        email_info = self._get_email_info(j)
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
                track_opens=True,
                auto_text=True,
                view_content_link=False,
                signing_domain=email_info.signing_domain,
                subaccount=j.subaccount,
                tags=j.analytics_tags,
                inline_css=True,
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
        send_ts = datetime.utcnow()
        r = await self.mandrill.post('messages/send.json', **data)
        data = await r.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == j.address, data
        if data['status'] not in ('sent', 'queued'):
            main_logger.warning('message not sent %s:%s response: %s', j.group_id, j.address, data)
        await self._store_msg(data['_id'], send_ts, j, email_info)

    async def _send_test(self, j: Job):
        email_info = self._get_email_info(j)
        data = dict(
            from_email=j.from_email,
            from_name=j.from_name,
            group_id=j.group_id,
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
        msg_id = re.sub(r'[^a-zA-Z0-9\-]', '', f'{j.group_id}-{j.address}')
        send_ts = datetime.utcnow()
        output = (
            f'to: {j.address}\n'
            f'msg id: {msg_id}\n'
            f'ts: {send_ts}\n'
            f'subject: {email_info.subject}\n'
            f'data: {json.dumps(data, indent=2)}\n'
            f'content: {email_info.html_body}\n'
        )
        Path.mkdir(self.settings.test_output, parents=True, exist_ok=True)
        save_path = self.settings.test_output / f'{msg_id}.txt'
        test_logger.info('sending message: %s (saved to %s)', output, save_path)
        save_path.write_text(output)
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
        if not self.settings.pdf_generation_url:
            return 'no-pdf-generated'
        headers = dict(
            pdf_page_size='A4',
            pdf_zoom='1.25',
            pdf_margin_left='8mm',
            pdf_margin_right='8mm',
        )
        async with self.session.get(self.settings.pdf_generation_url, data=html, headers=headers) as r:
            if r.status != 200:
                data = await r.text()
                raise RuntimeError(f'error generating pdf {r.status}, data: {data}')
            pdf_content = await r.read()
        return base64.b64encode(pdf_content).decode()

    async def _store_msg(self, uid, send_ts, j: Job, email_info: EmailInfo):
        await self.es.post(
            f'messages/{j.send_method}/{uid}',
            company=j.company_code,
            send_ts=send_ts,
            update_ts=send_ts,
            status=MessageStatus.send,
            group_id=j.group_id,
            to_first_name=j.first_name,
            to_last_name=j.last_name,
            to_email=j.address,
            from_email=j.from_email,
            from_name=j.from_name,
            tags=j.search_tags + j.analytics_tags,
            subject=email_info.subject,
            body=email_info.html_body,
            attachments=[a['name'] for a in j.pdf_attachments],
            events=[]
        )


class Worker(BaseWorker):
    shadows = [Sender]

    def __init__(self, **kwargs):
        self.settings = Settings(sender_cls='app.worker.Sender')
        kwargs['redis_settings'] = self.settings.redis_settings
        super().__init__(**kwargs)

    async def shadow_kwargs(self):
        d = await super().shadow_kwargs()
        d['settings'] = self.settings
        return d

    async def shadow_factory(self):
        shadows = await super().shadow_factory()
        sender = shadows[0]
        await sender.setup_mandrill_webhook()
        return shadows
