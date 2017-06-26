import base64
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple

import msgpack
from aiohttp import ClientSession
from arq import Actor, BaseWorker, Drain, concurrent, cron

from .es import ElasticSearch
from .models import THIS_DIR, EmailSendMethod, MessageStatus
from .render import EmailInfo, render_email
from .settings import Settings
from .utils import Mandrill

test_logger = logging.getLogger('morpheus.worker.test')
main_logger = logging.getLogger('morpheus.worker')


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


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = self.es = self.mandrill = None
        self.mandrill_webhook_auth_key = None

    async def startup(self):
        main_logger.info('Sender initialising session and elasticsearch and mandrill...')
        self.session = ClientSession(loop=self.loop)
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)
        self.mandrill = Mandrill(settings=self.settings, loop=self.loop)

    async def shutdown(self):
        self.session.close()
        self.es.close()
        self.mandrill.close()

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
                drain.add(coro, Job(**data))
                # TODO stop if worker is not running
                jobs += 1
        return jobs

    async def _send_mandrill(self, j: Job):
        email_info = render_email(j)
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
                signing_domain=j.from_email[j.from_email.index('@') + 1:],
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

    async def _send_test_email(self, j: Job):
        email_info = render_email(j)
        data = dict(
            from_email=j.from_email,
            from_name=j.from_name,
            group_id=j.group_id,
            headers=email_info.headers,
            to_email=j.address,
            to_name=email_info.full_name,
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


class AuxActor(Actor):
    def __init__(self, settings: Settings = None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.es = None

    async def startup(self):
        main_logger.info('Sender initialising elasticsearch...')
        self.es = ElasticSearch(settings=self.settings, loop=self.loop)

    async def shutdown(self):
        self.es.close()

    @cron(hour=3, minute=0)
    async def snapshot_es(self):
        main_logger.info('creating elastic search backup...')
        r = await self.es.put(
            f'/_snapshot/{self.settings.snapshot_repo_name}/'
            f'snapshot-{datetime.now():%Y-%m-%d_%H-%M-%S}?wait_for_completion=true'
        )
        main_logger.info('snapshot created: %s', json.dumps(await r.json(), indent=2))


class Worker(BaseWorker):
    shadows = [Sender, AuxActor]

    def __init__(self, **kwargs):
        self.settings = Settings(sender_cls='app.worker.Sender')
        kwargs['redis_settings'] = self.settings.redis_settings
        super().__init__(**kwargs)

    async def shadow_kwargs(self):
        d = await super().shadow_kwargs()
        d['settings'] = self.settings
        return d
