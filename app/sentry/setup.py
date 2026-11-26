from urllib.parse import urlsplit

import sentry_sdk

from app.core.config import settings

# The code that handles an email received at a chat reply address: the webhook Mandrill posts it to, the package
# that reads it and the two tasks. An event from any of it is sent without the request and without any frame's
# local variables, library frames included: both hold the email, and what its sender wrote in it. A request's
# form is attached to an event whatever `send_default_pii` says.
INBOUND_WEBHOOK_PATH = '/webhook/mandrill/inbound/'
INBOUND_MODULE = 'app.inbound'
INBOUND_FUNCTIONS = {
    'app.messages.api.webhooks': ('mandrill_inbound_view', '_queue_inbound_replies'),
    'app.messages.tasks': ('read_inbound_reply', 'deliver_chat_reply'),
}
# An exception's message is kept only when our own code raised it. A library that reads the email (`email`,
# `dkim`, `dns`, `bs4`, `mailparser_reply`, `json`, `html`) can quote it in its message, and so can any other
# the email passes through, such as a codec.
OWN_PACKAGE = 'app'
FILTERED = '[Filtered]'


def _is_inbound_frame(frame: dict) -> bool:
    module = frame.get('module') or ''
    return (
        module == INBOUND_MODULE
        or module.startswith(f'{INBOUND_MODULE}.')
        or frame.get('function') in INBOUND_FUNCTIONS.get(module, ())
    )


def before_send(event: dict, hint: dict) -> dict:
    """
    Take the email out of an event raised while one received at a chat reply address was handled (see
    `INBOUND_WEBHOOK_PATH`). The exception's message goes too unless our own code raised it: none of ours
    quotes the email, and a library's can.
    """
    path = urlsplit((event.get('request') or {}).get('url') or '').path
    exceptions = (event.get('exception') or {}).get('values') or []
    stacks = [(exception, (exception.get('stacktrace') or {}).get('frames') or []) for exception in exceptions]
    if path != INBOUND_WEBHOOK_PATH and not any(_is_inbound_frame(frame) for __, frames in stacks for frame in frames):
        return event
    event.pop('request', None)
    for exception, frames in stacks:
        for frame in frames:
            frame.pop('vars', None)
        if not frames or (frames[-1].get('module') or '').split('.')[0] != OWN_PACKAGE:
            exception['value'] = FILTERED
    return event


def init_sentry() -> None:
    if not settings.sentry_dsn:
        return
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        release=settings.release,
        environment='production' if not settings.dev_mode else 'development',
        # also keeps a Celery task's arguments, which for the inbound tasks are the email, out of its events
        send_default_pii=False,
        before_send=before_send,  # ty:ignore[invalid-argument-type]
    )
