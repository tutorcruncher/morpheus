from dataclasses import dataclass

import chevron
import logging
import re
import sass
import secrets
from base64 import urlsafe_b64encode
from chevron import ChevronError
from misaka import HtmlRenderer, Markdown
from typing import Dict

markdown = Markdown(HtmlRenderer(flags=['hard-wrap']), extensions=['no-intra-emphasis'])
logger = logging.getLogger('render')


@dataclass
class MessageDef:
    first_name: str
    last_name: str
    main_template: str
    mustache_partials: Dict[str, dict]
    macros: Dict[str, dict]
    subject_template: str
    context: dict
    headers: dict


@dataclass
class EmailInfo:
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
            v = chevron.render(_apply_macros(v, macros), data=context, partials_dict=partials)
            yield k[:-8], markdown(v)


def _apply_macros(s, macros):
    if macros:
        for key, body in macros.items():
            m = re.search(r'^(\S+)\((.*)\) *$', key)
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


SKIPPED_LINKS = [
    re.compile(r'\.(?:png|jpg|bmp)$'),
    re.compile(r'^https?://maps.googleapis.com'),
    re.compile(r'^https?://maps.google.com'),
]


def looks_like_link(s):
    return isinstance(s, str) and re.match('^https?://', s) and not any(m.search(s) for m in SKIPPED_LINKS)


def apply_short_links(context, click_url, click_random=30, backup_arg=False):
    shortened_link = []
    extra = {}
    for k, v in context.items():
        # TODO deal with unsubscribe links properly
        if k != 'unsubscribe_link' and looks_like_link(v):
            r = secrets.token_urlsafe(click_random)[:click_random]
            new_url = click_url + r
            if backup_arg:
                new_url += '?u=' + urlsafe_b64encode(v.encode()).decode()
            extra[k] = new_url
            extra[f'{k}_original'] = v
            shortened_link.append((v, r))
    context.update(extra)
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
        shortened_link = apply_short_links(m.context, click_url, click_random, backup_arg=True)
    m.context.update(email_subject=subject, **dict(_update_context(m.context, m.mustache_partials, m.macros)))
    unsubscribe_link = m.context.get('unsubscribe_link')
    if unsubscribe_link:
        m.headers.setdefault('List-Unsubscribe', f'<{unsubscribe_link}>')

    return EmailInfo(
        full_name=full_name,
        subject=subject,
        html_body=chevron.render(
            _apply_macros(m.main_template, m.macros), data=m.context, partials_dict=m.mustache_partials
        ),
        headers=m.headers,
        shortened_link=shortened_link,
    )


BASIC_CHARACTERS = {
    # basic characters from https://support.messagebird.com/hc/en-us/articles/208739765-Which-special-
    # characters-count-as-two-characters-in-a-text-message- ordered and repeats removed!
    ' ',
    '!',
    '"',
    '#',
    '$',
    '%',
    '&',
    "'",
    '(',
    ')',
    '*',
    '+',
    ',',
    '-',
    '.',
    '/',
    '0',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    ':',
    ';',
    '<',
    '=',
    '>',
    '?',
    '@',
    'A',
    'B',
    'C',
    'D',
    'E',
    'F',
    'G',
    'H',
    'I',
    'J',
    'K',
    'L',
    'M',
    'N',
    'O',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'U',
    'V',
    'W',
    'X',
    'Y',
    'Z',
    '_',
    'a',
    'b',
    'c',
    'd',
    'e',
    'f',
    'g',
    'h',
    'i',
    'j',
    'k',
    'l',
    'm',
    'n',
    'o',
    'p',
    'q',
    'r',
    's',
    't',
    'u',
    'v',
    'w',
    'x',
    'y',
    'z',
    '¡',
    '£',
    '¤',
    '¥',
    '§',
    '¿',
    'Ä',
    'Å',
    'Æ',
    'Ç',
    'É',
    'Ñ',
    'Ö',
    'Ø',
    'Ü',
    'ß',
    'à',
    'ä',
    'å',
    'æ',
    'è',
    'é',
    'ì',
    'ñ',
    'ò',
    'ö',
    'ø',
    'ù',
    'ü',
    'Γ',
    'Δ',
    'Θ',
    'Λ',
    'Ξ',
    'Π',
    'Σ',
    'Φ',
    'Ψ',
    'Ω',
    # special cases from https://support.messagebird.com/hc/en-gb/articles/200731072-In-which-charset-can-I-deliver-
    # SMS-messages-and-what-should-I-take-into-consideration- this is not an exhaustive list :(
    'ç',
    '®',
}

# from first link above
# apparently messagebird call \n an extension character which differs from https://en.wikipedia.org/wiki/GSM_03.38
# this might be because they replace \n with LF + CR which would constitute 2 characters
EXTENSION_CHARACTERS = {'\n', '[', '\\', ']', '^', '{', '|', '}', '~', '€'}

# from https://support.messagebird.com/hc/en-us/articles/208739745-How-long-is-1-SMS-Message-
MULTIPART_LENGTHS = [(1, 160), (2, 306), (3, 459), (4, 612), (5, 765), (6, 918), (7, 1071), (8, 1224), (9, 1377)]


@dataclass
class SmsLength:
    length: int
    parts: int


class MessageTooLong(ValueError):
    pass


def sms_length(msg: str) -> SmsLength:
    """
    :param msg: msg string
    :return: tuple (length of the message, number of multi-part SMSs required)
    """
    length = 0
    for c in msg:
        if c in BASIC_CHARACTERS:
            length += 1
        elif c in EXTENSION_CHARACTERS:
            length += 2
        # in theory all other characters are unavailable in GSM 03.38 and will be stripped out

    for msg_parts, max_length in MULTIPART_LENGTHS:
        if length <= max_length:
            return SmsLength(length, msg_parts)
    raise MessageTooLong(f'message length {length} exceeds maximum multi-part SMS length {max_length}')
