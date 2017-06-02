import logging
import logging.config

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
                'format': '%(levelname)s %(name)s %(message)s',
            },
        },
        'handlers': {
            'morpheus.default': {
                'level': settings.log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'morpheus.default',
            },
            'sentry': {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': settings.raven_dsn,
                'release': settings.commit,
                'name': settings.server_name,
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
