#!/usr/bin/env python3.6
import asyncio
import logging

import click

from morpheus.es import ElasticSearch
from morpheus.logs import setup_logging
from morpheus.settings import Settings

commands = []
logger = logging.getLogger('morpheus.main')


def command(func):
    commands.append(func)
    return func


@command
async def prepare_elasticsearch(*, settings, force):
    es = ElasticSearch(settings=settings)
    await es.create_indices(delete_existing=force)
    es.close()


@click.command()
@click.argument('command', type=click.Choice([c.__name__ for c in commands]))
@click.option('--force', is_flag=True)
def cli(*, command, force):
    """
    Run TutorCruncher socket
    """
    settings = Settings()
    setup_logging(settings)

    command_lookup = {c.__name__: c for c in commands}

    func = command_lookup[command]
    logger.info('running %s...', func.__name__)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(func(settings=settings, force=force))


if __name__ == '__main__':
    cli()
