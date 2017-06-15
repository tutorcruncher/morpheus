import base64
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple

import chevron
import misaka
import msgpack
from aiohttp import ClientSession
from arq import Actor, BaseWorker, Drain, concurrent
from misaka import HtmlRenderer, Markdown

from .es import ElasticSearch
from .models import MessageStatus, SendMethod
from .settings import Settings
from .utils import Mandrill

test_logger = logging.getLogger('morpheus.worker.test')
main_logger = logging.getLogger('morpheus.worker')


markdown = Markdown(HtmlRenderer(flags=[misaka.HTML_HARD_WRAP]), extensions=[misaka.EXT_NO_INTRA_EMPHASIS])


class Job(NamedTuple):
    group_id: str
    send_method: str
    first_name: str
    last_name: str
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


class EmailInfo(NamedTuple):
    full_name: str
    subject: str
    html_body: str
    signing_domain: str
    headers: dict


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = self.es = self.mandrill = None
        self.mandrill_webhook_auth_key = None

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
        if method == SendMethod.email_mandrill:
            coro = self._send_mandrill
        elif method == SendMethod.email_test:
            coro = self._send_test
        else:
            raise NotImplementedError()
        tags.append(uid)
        main_logger.info('sending group %s via %s', uid, method)
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
                drain.add(coro, Job(**data))
                # TODO stop if worker is not running
                jobs += 1
        return jobs

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
                headers=email_info.headers,
                track_opens=True,
                auto_text=True,
                view_content_link=False,
                signing_domain=email_info.signing_domain,
                subaccount=j.subaccount,
                tags=j.tags,
                inline_css=True,
                important=j.important,
                attachments=[dict(
                    type='application/pdf',
                    name=a['name'],
                    content=await self._generate_base64_pdf(a['html']),
                ) for a in j.pdf_attachments]
            ),
        }
        send_ts = datetime.utcnow()
        r = await self.mandrill.post('messages/send.json', **data)
        data = await r.json()
        assert len(data) == 1, data
        data = data[0]
        assert data['email'] == j.address, data
        if data['status'] not in ('sent', 'queued'):
            main_logger.warning('message not sent %s %s response: %s', j.group_id, j.address, data)
        await self._store_msg(data['_id'], send_ts, j, email_info)

    async def _send_test(self, j: Job):
        email_info = self._get_email_info(j)
        data = dict(
            from_email=j.from_email,
            from_name=j.from_name,
            group_id=j.group_id,
            headers=email_info.headers,
            to_email=j.address,
            to_name=email_info.full_name,
            signing_domain=email_info.signing_domain,
            tags=j.tags,
            important=j.important,
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
            f'content:\n'
            f'{email_info.html_body}\n'
        )
        if self.settings.test_output:
            Path.mkdir(self.settings.test_output, parents=True, exist_ok=True)
            save_path = self.settings.test_output / f'{msg_id}.txt'
            test_logger.info('sending message: %s (saved to %s)', output, save_path)
            save_path.write_text(output)
        await self._store_msg(msg_id, send_ts, j, email_info)

    @classmethod
    def _update_context(cls, context, partials, macros):
        for k, v in context.items():
            if k.endswith('__md'):
                yield k[:-4], markdown(v)
            elif k.endswith('__render'):
                v = chevron.render(
                    cls._apply_macros(v, macros),
                    data=context,
                    partials_dict=partials
                )
                yield k[:-8], markdown(v)

    @staticmethod
    def _apply_macros(s, macros):
        if macros:
            for key, body in macros.items():
                m = re.search('^(\S+)\((.*)\) *$', key)
                if not m:
                    main_logger.warning('invalid macro "%s", skipping it', key)
                    continue
                name, arg_defs = m.groups()
                arg_defs = [a.strip(' ') for a in arg_defs.split('|') if a.strip(' ')]

                def replace_macro(m):
                    arg_values = [a.strip(' ') for a in m.groups()[0].split('|') if a.strip(' ')]
                    if len(arg_defs) != len(arg_values):
                        main_logger.warning('invalid macro call "%s", not replacing', m.group())
                        return m.group()
                    else:
                        return chevron.render(body, data=dict(zip(arg_defs, arg_values)))

                s = re.sub(r'\{\{ *%s\((.*?)\) *\}\}' % name, replace_macro, s)
        return s

    def _get_email_info(self, j: Job) -> EmailInfo:
        full_name = f'{j.first_name} {j.last_name}'.strip(' ')
        j.context.update(
            first_name=j.first_name,
            last_name=j.last_name,
            full_name=full_name,
        )
        subject = chevron.render(j.subject_template, data=j.context)
        j.context.update(
            subject=subject,
            **dict(self._update_context(j.context, j.mustache_partials, j.macros))
        )
        unsubscribe_link = j.context.get('unsubscribe_link')
        if unsubscribe_link:
            j.headers.setdefault('List-Unsubscribe', f'<{unsubscribe_link}>')

        return EmailInfo(
            full_name=full_name,
            subject=subject,
            html_body=chevron.render(
                self._apply_macros(j.main_template, j.macros),
                data=j.context,
                partials_dict=j.mustache_partials,
            ),
            signing_domain=j.from_email[j.from_email.index('@') + 1:],
            headers=j.headers,
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
            tags=j.tags,
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
