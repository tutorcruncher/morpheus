import base64
import re
from collections import namedtuple

import chevron
import misaka
import msgpack
import pydf
from misaka import HtmlRenderer, Markdown
from aiohttp import ClientSession
from arq import Actor, BaseWorker, Drain, concurrent

from .models import SendMethod
from .settings import Settings


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
        'first_name',
        'last_name',
        'user_id',
        'address',
        'search_tags',
        'pdf_html',
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


class Sender(Actor):
    def __init__(self, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.session = None

    async def startup(self):
        self.session = ClientSession(loop=self.loop)

    async def shutdown(self):
        self.session.close()

    @concurrent
    async def send(self,
                   recipients_key, *,
                   id,
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
        else:
            raise NotImplementedError()
        analytics_tags = [id] + analytics_tags
        base_kwargs = dict(
            group_id=id,
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

        drain = Drain(redis_pool=await self.get_redis_pool())
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
        full_name = f'{j.first_name} {j.last_name}'.strip(' ')
        j.context.update(
            first_name=j.first_name,
            last_name=j.last_name,
            full_name=full_name,
        )
        message = markdown(chevron.render(j.markdown_template, data=j.context, partials_dict=j.mustache_partials))
        html = chevron.render(
            j.main_template,
            data=dict(message=message, **j.context),
            partials_dict=j.mustache_partials,
        )
        data = {
            'key': self.settings.mandrill_key,
            'async': True,
            'message': dict(
                html=html,
                subject=chevron.render(j.subject_template, data=j.context),
                from_email=j.from_email,
                from_name=j.from_name,
                to=[
                    dict(
                        email=j.address,
                        name=full_name,
                        type='to'
                    )
                ],
                track_opens=True,
                auto_text=True,
                view_content_link=False,
                signing_domain=j.from_email[j.from_email.index('@'):],
                subaccount=j.subaccount,
                tags=j.analytics_tags,
                # google analytics ?
                # inline_css ?,
                attachments=[dict(
                    type='application/pdf',
                    name=a['name'],
                    content=base64.b64encode(pydf.generate_pdf(a['html'].encode())).decode()
                ) for a in j.pdf_html]
            ),
        }
        if j.reply_to:
            data['message']['headers'] = {
                'Reply-To': j.reply_to,
            }
        url = self.settings.mandrill_url + '/messages/send.json'
        async with self.session.post(url, json=data) as r:
            print('response status:', r.status)
            text = await r.text()
            print(f'response body:\n{text}')


class Worker(BaseWorker):
    shadows = [Sender]
