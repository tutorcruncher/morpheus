import logging
import re
import secrets
from typing import Dict, NamedTuple

import chevron
import sass
from chevron import ChevronError
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
    shortened_link: list


def _update_context(context, partials, macros):
    for k, v in context.items():
        if k.endswith('__md'):
            yield k[:-4], markdown(v)
        elif k.endswith('__sass'):
            yield k[:-6], sass.compile(string=v, output_style='compressed', precision=10).strip('\n')
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


def looks_like_link(s):
    return (
        isinstance(s, str) and
        re.match('^https?://', s) and
        not re.search('\.(?:png|jpg|bmp)$', s)
    )


def apply_short_links(context, click_url, click_random=30):
    shortened_link = []
    for k, v in context.items():
        # TODO deal with unsubscribe links properly
        if k != 'unsubscribe_link' and looks_like_link(v):
            r = secrets.token_urlsafe(click_random)[:click_random]
            context[k] = click_url + r
            shortened_link.append((v, r))
    return shortened_link


def render_email(m: MessageDef, click_url=None, click_random=30) -> EmailInfo:
    full_name = f'{m.first_name or ""} {m.last_name or ""}'.strip(' ')
    m.context.setdefault('recipient_name', full_name)
    m.context.setdefault('recipient_first_name', m.first_name or full_name)
    m.context.setdefault('recipient_last_name', m.last_name)
    try:
        subject = chevron.render(m.subject_template, data=m.context)
    except ChevronError as e:
        logger.warning('invalid subject template: %s', e)
        subject = m.subject_template

    shortened_link = []
    if click_url:
        shortened_link = apply_short_links(m.context, click_url, click_random)

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
        shortened_link=shortened_link,
    )
