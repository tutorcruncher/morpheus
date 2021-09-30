import asyncio
import logging
from foxglove.db.main import prepare_database

from src.settings import Settings

settings = Settings()
logger = logging.getLogger('db')


def reset_database(settings):
    if not input('Confirm database reset? y/n ').lower() == 'y':
        print('cancelling')
    else:
        print('resetting database...')
        asyncio.run(prepare_database(settings, True))
        print('done.')
