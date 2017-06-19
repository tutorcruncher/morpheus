import logging
import re
from typing import Dict, NamedTuple

import chevron
from misaka import HtmlRenderer, Markdown

markdown = Markdown(HtmlRenderer(flags=['hard-wrap']), extensions=['no-intra-emphasis'])
logger = logging.getLogger('morpheus.render')


class MessageDef(NamedTuple):
    first_name: str
    last_name: str
    main_template: str
    mustache_partials: Dict[str, dict]
    macros: Dict[str, dict]
    subject_template: str
    context: dict
    headers: dict


class EmailInfo(NamedTuple):
    full_name: str
    subject: str
    html_body: str
    headers: dict


def _update_context(context, partials, macros):
    for k, v in context.items():
        if k.endswith('__md'):
            yield k[:-4], markdown(v)
        elif k.endswith('__render'):
            v = chevron.render(
                _apply_macros(v, macros),
                data=context,
                partials_dict=partials
            )
            yield k[:-8], markdown(v)


def _apply_macros(s, macros):
    if macros:
        for key, body in macros.items():
            m = re.search('^(\S+)\((.*)\) *$', key)
            if not m:
                logger.warning('invalid macro "%s", skipping it', key)
                continue
            name, arg_defs = m.groups()
            arg_defs = [a.strip(' ') for a in arg_defs.split('|') if a.strip(' ')]

            def replace_macro(m):
                arg_values = [a.strip(' ') for a in m.groups()[0].split('|') if a.strip(' ')]
                if len(arg_defs) != len(arg_values):
                    logger.warning('invalid macro call "%s", not replacing', m.group())
                    return m.group()
                else:
                    return chevron.render(body, data=dict(zip(arg_defs, arg_values)))

            s = re.sub(r'%s\((.*?)\)' % name, replace_macro, s)
    return s


def render_email(m: MessageDef) -> EmailInfo:
    full_name = f'{m.first_name or ""} {m.last_name or ""}'.strip(' ')
    m.context.setdefault('recipient_name', full_name)
    m.context.setdefault('recipient_first_name', m.first_name or full_name)
    m.context.setdefault('recipient_last_name', m.last_name)
    subject = chevron.render(m.subject_template, data=m.context)
    m.context.update(
        email_subject=subject,
        **dict(_update_context(m.context, m.mustache_partials, m.macros))
    )
    unsubscribe_link = m.context.get('unsubscribe_link')
    if unsubscribe_link:
        m.headers.setdefault('List-Unsubscribe', f'<{unsubscribe_link}>')

    return EmailInfo(
        full_name=full_name,
        subject=subject,
        html_body=chevron.render(
            _apply_macros(m.main_template, m.macros),
            data=m.context,
            partials_dict=m.mustache_partials,
        ),
        headers=m.headers,
    )
