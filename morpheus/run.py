import logging
import os

import click
import uvicorn
from app.logs import setup_logging
from app.main import app
from app.management import prepare_database
from app.settings import Settings
from app.worker import WorkerSettings
from arq import run_worker

logger = logging.getLogger('tc-hubspot')


@click.group()
@click.option('-v', '--verbose', is_flag=True)
def cli(verbose):
    """
    Run Morpheus
    """
    setup_logging(verbose)


@cli.command()
def reset_database():
    if input('Are you bloody sure you want to destroy the database? [y/n] ').lower() == 'y':
        prepare_database(True)


def web():
    """
    If the database doesn't already exist it will be created.
    """
    setup_logging()
    port = int(os.getenv('PORT', 8000))
    logger.info('preparing the database...')
    prepare_database(False)
    uvicorn.run(app, host='0.0.0.0', port=port)


def worker():
    """
    Run the worker
    """
    settings = Settings()
    run_worker(WorkerSettings, redis_settings=settings.redis_settings, ctx={'settings': settings})
    logger.info('worker is running')


@cli.command()
def auto():
    port_env = os.getenv('PORT')
    dyno_env = os.getenv('DYNO')
    if dyno_env:
        logger.info('using environment variable DYNO=%r to infer command', dyno_env)
        if dyno_env.lower().startswith('web'):
            web()
        else:
            worker()
    elif port_env and port_env.isdigit():
        logger.info('using environment variable PORT=%s to infer command as web', port_env)
        web()
    else:
        logger.info('no environment variable found to infer command, assuming worker')
        worker()


if __name__ == '__main__':
    cli()
