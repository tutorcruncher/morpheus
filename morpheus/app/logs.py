import logging
import logging.config
from functools import partial

from raven_aiohttp import QueuedAioHttpTransport

from .settings import Settings


def setup_logging(settings: Settings):
    """
    setup logging config for morpheus by updating the arq logging config
    """
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
                'transport': partial(QueuedAioHttpTransport, workers=5, qsize=1000),
                'dsn': settings.raven_dsn,
                'release': settings.commit,
                'name': settings.deploy_name,
            },
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
