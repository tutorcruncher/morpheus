import logging
import logging.config

from raven import Client
from raven_aiohttp import QueuedAioHttpTransport

from .settings import Settings


def setup_logging(settings: Settings):
    """
    setup logging config for morpheus by updating the arq logging config
    """
    client = None
    if settings.raven_dsn:
        client = Client(
            transport=QueuedAioHttpTransport,
            dsn=settings.raven_dsn,
            release=settings.commit,
            name=settings.deploy_name,
        )
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'morpheus.default': {
                'format': '%(levelname)s %(name)s: %(message)s',
            },
        },
        'handlers': {
            'morpheus.default': {
                'level': 'DEBUG',
                'class': 'arq.logs.ColourHandler',
                'formatter': 'morpheus.default',
            },
            'sentry': {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'client': client,
            } if client else {
                'level': 'WARNING',
                'class': 'logging.NullHandler',
            }
        },
        'loggers': {
            'morpheus': {
                'handlers': ['morpheus.default', 'sentry'],
                'level': settings.log_level,
            },
            'arq': {
                'handlers': ['morpheus.default', 'sentry'],
                'level': settings.log_level,
            },
        },
    }
    logging.config.dictConfig(config)
