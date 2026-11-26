"""What somebody wrote in an email they sent in reply to one of ours."""

import re
import warnings
from collections.abc import Iterator
from email.message import EmailMessage, MIMEPart

from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, NavigableString, Tag
from mailparser_reply import EmailReplyParser
from mailparser_reply.constants import MAIL_LANGUAGES

# an email body is cut to this before anything parses it
BODY_LIMIT = 100_000
HIDDEN_STYLES = ('display:none', 'visibility:hidden')
UNSHOWN_ELEMENTS = ('script', 'style', 'head', 'title')
BLOCK_ELEMENTS = ('p', 'div', 'li', 'tr', 'blockquote')
# what the parser that cuts the quoted email is given: its time grows with the square of a long line or a run of
# blank lines. Twice the 4,000 characters of TutorCruncher's longest chat message: a reply longer than a chat
# message is refused whatever follows it
REPLY_PARSE_LIMIT = 8000
BLANK_LINES = re.compile(r'\n\s*\n(?:\s*\n)+')


def _is_hidden(element: Tag) -> bool:
    """Whether a mail app shows nothing of the element: it is not page content, or is marked or styled hidden."""
    return (
        element.name in UNSHOWN_ELEMENTS
        or element.has_attr('hidden')
        or any(style in ''.join(str(element.get('style') or '').lower().split()) for style in HIDDEN_STYLES)
    )


def _visible_text(html: str) -> str:
    """
    The text of an HTML body that a mail app would show, with a new line at each line break and after each
    block. The parsed tree is read in one pass and never changed: taking an element out of it, or adding to one,
    takes time that grows with how many are beside or inside it, which an email written for that makes seconds.
    """
    with warnings.catch_warnings():
        # an HTML part that is only a link or a file's name is still what the sender wrote
        warnings.simplefilter('ignore', MarkupResemblesLocatorWarning)
        soup = BeautifulSoup(html, 'html.parser')
    text = []
    # the elements being read, outermost first, each with what is left of its contents
    open_elements: list[tuple[Tag, Iterator]] = [(soup, iter(soup.contents))]
    while open_elements:
        element, contents = open_elements[-1]
        node = next(contents, None)
        if node is None:
            open_elements.pop()
            if element.name in BLOCK_ELEMENTS:
                text.append('\n')
        elif isinstance(node, Tag):
            if _is_hidden(node):
                continue
            elif node.name == 'br':
                text.append('\n')
            else:
                open_elements.append((node, iter(node.contents)))
        elif type(node) is NavigableString:
            # not a comment, a doctype, a processing instruction or a CDATA section, none of which is shown
            text.append(str(node))
    lines = [' '.join(line.split()) for line in ''.join(text).splitlines()]
    return '\n'.join(lines).strip()


def body_text(message: EmailMessage) -> str:
    """The email's text: its plain text part, or else what its HTML part shows. Empty when it has neither."""
    if plain_part := message.get_body(preferencelist=('plain',)):
        return _part_text(plain_part)
    elif html_part := message.get_body(preferencelist=('html',)):
        return _visible_text(_part_text(html_part))
    else:
        return ''


def _part_text(part: MIMEPart) -> str:
    """
    The text of one part of the email, cut to `BODY_LIMIT`. A character set Python has no name for is read as UTF-8.
    """
    try:
        text = part.get_content()
    except LookupError:
        text = part.get_payload(decode=True).decode(errors='replace')  # ty:ignore[unresolved-attribute]
    return text[:BODY_LIMIT]


def latest_reply(text: str) -> str:
    """
    The text without the earlier email quoted under it, read from its first `REPLY_PARSE_LIMIT` characters once
    each run of blank lines has become one, in every language the parser knows, since a mail app writes the line
    above the quote in its own language ("Am … schrieb …"). A signature above the quote is kept. NULs are taken
    out, as Postgres cannot store one in text.
    """
    text = BLANK_LINES.sub('\n\n', text.replace('\x00', ''))[:REPLY_PARSE_LIMIT]
    return (EmailReplyParser(languages=list(MAIL_LANGUAGES)).read(text=text).latest_reply or '').strip()


def attached_file_names(message: EmailMessage) -> list[str]:
    """
    The names of the files attached to the email, whether a mail app marked them as attachments or, as Apple Mail
    does, as shown inline. A picture the email's HTML shows by its Content-ID, as in a signature, is not one.
    """
    return [
        part.get_filename() or 'file'
        for part in message.iter_attachments()
        if part.get_content_disposition() == 'attachment' or (part.get_filename() and not part['Content-ID'])
    ]
