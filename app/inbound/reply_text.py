"""What somebody wrote in an email they sent in reply to one of ours."""

import re
from email.message import EmailMessage, MIMEPart

from bs4 import BeautifulSoup
from mailparser_reply import EmailReplyParser
from mailparser_reply.constants import MAIL_LANGUAGES

# an email body is cut to this before anything parses it
BODY_LIMIT = 100_000
HIDDEN_STYLES = ('display:none', 'visibility:hidden')
# what the parser that cuts the quoted email is given: its time grows with the square of a long line or a run of
# blank lines. Twice the 4,000 characters of TutorCruncher's longest chat message: a reply longer than a chat
# message is refused whatever follows it
REPLY_PARSE_LIMIT = 8000
BLANK_LINES = re.compile(r'\n\s*\n(?:\s*\n)+')


def _visible_text(html: str) -> str:
    """The text of an HTML body that a mail app would show."""
    soup = BeautifulSoup(html, 'html.parser')
    for element in soup(['script', 'style', 'head', 'title']):
        element.decompose()
    for element in soup.find_all(hidden=True):
        element.decompose()
    for element in soup.find_all(style=True):
        # an element inside one already removed has gone with it
        if not element.decomposed and any(
            style in ''.join(str(element['style']).lower().split()) for style in HIDDEN_STYLES
        ):
            element.decompose()
    for line_break in soup.find_all('br'):
        line_break.replace_with('\n')
    for block in soup.find_all(['p', 'div', 'li', 'tr', 'blockquote']):
        block.append('\n')
    lines = [' '.join(line.split()) for line in soup.get_text().splitlines()]
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
