#!/usr/bin/env python3.7
import asyncio
import logging
from time import sleep

import click
import uvloop
from aiohttp.web import run_app
from arq import RunWorkerProcess

from app.db import patches, reset_database, run_patch
from app.logs import setup_logging
from app.main import create_app
from app.settings import Settings

logger = logging.getLogger('morpheus.main')


async def _check_port_open(host, port, loop):
    steps, delay = 40, 0.5
    for i in range(steps):
        try:
            await loop.create_connection(lambda: asyncio.Protocol(), host=host, port=port)
        except OSError:
            logger.info('waiting for connection to %s:%s %d', host, port, i)
            await asyncio.sleep(delay, loop=loop)
        else:
            logger.info('Connected successfully to %s:%s after %0.2fs', host, port, delay * i)
            return
    raise RuntimeError(f'Unable to connect to {host}:{port} after {steps * delay}s')


def _check_services_ready(settings: Settings):
    loop = asyncio.get_event_loop()
    coros = [
        _check_port_open(settings.pg_host, settings.pg_port, loop),
        _check_port_open(settings.redis_host, settings.redis_port, loop),
    ]
    loop.run_until_complete(asyncio.gather(*coros, loop=loop))


@click.group()
@click.pass_context
def cli(ctx):
    """
    Run morpheus
    """
    pass


def _set_loop():
    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@cli.command()
@click.option('--wait/--no-wait', default=True)
def web(wait):
    """
    Serve the application
    If the database doesn't already exist it will be created.
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)

    logger.info('waiting for postgres and redis to come up...')

    _check_services_ready(settings)

    logger.info('starting server...')
    _set_loop()
    loop = asyncio.get_event_loop()
    app = create_app(loop, settings)
    run_app(app, port=8000, print=lambda v: None, access_log=None)


@cli.command()
@click.option('--wait/--no-wait', default=True)
def worker(wait):
    """
    Run the worker
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)

    _set_loop()
    logger.info('waiting for postgres and redis to come up...')
    wait and sleep(4)
    _check_services_ready(settings)
    # redis/the network occasionally hangs and gets itself in a mess if we try to connect too early,
    # even once it's "up", hence 2 second wait
    wait and sleep(2)
    RunWorkerProcess('app/worker.py', 'Worker')


@cli.command()
def postgres_reset_database():
    """
    reset the postgres database
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)

    logger.info('running reset_database...')
    reset_database(settings)


@cli.command()
@click.option('--live/--not-live')
@click.argument('patch', type=click.Choice([p.func.__name__ for p in patches]))
def postgres_patch(live, patch):
    """
    run a postgres patch
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)

    logger.info('running reset_database...')
    run_patch(settings, live, patch)


EXEC_LINES = [
    'import asyncio, os, re, sys',
    'from datetime import datetime, timedelta, timezone',
    'from pprint import pprint as pp',
    '',
    'from app.settings import Settings',
    '',
    'loop = asyncio.get_event_loop()',
    'await_ = loop.run_until_complete',
]
EXEC_LINES += (
    ['print("\\n    Python {v.major}.{v.minor}.{v.micro}\\n".format(v=sys.version_info))'] +
    [f'print("    {l}")' for l in EXEC_LINES]
)


@cli.command()
def shell():
    """
    Run an interactive python shell
    """
    from IPython import start_ipython
    from IPython.terminal.ipapp import load_default_config
    c = load_default_config()

    c.TerminalIPythonApp.display_banner = False
    c.TerminalInteractiveShell.confirm_exit = False
    c.InteractiveShellApp.exec_lines = EXEC_LINES
    start_ipython(argv=(), config=c)


if __name__ == '__main__':
    cli()
