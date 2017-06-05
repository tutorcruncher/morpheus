#!/usr/bin/env python3.6
import asyncio
import logging
from time import sleep

import click
import uvloop
from aiohttp.web import run_app
from arq import RunWorkerProcess

from app.es import ElasticSearch
from app.logs import setup_logging
from app.main import create_app
from app.settings import Settings

logger = logging.getLogger('morpheus.main')


async def _check_port_open(host, port, loop):
    steps, delay = 20, 0.5
    for i in range(steps):
        try:
            await loop.create_connection(lambda: asyncio.Protocol(), host=host, port=port)
        except OSError:
            await asyncio.sleep(delay, loop=loop)
        else:
            logger.info('Connected successfully to %s:%s after %0.2fs', host, port, delay * i)
            return
    raise RuntimeError(f'Unable to connect to {host}:{port} after {steps * delay}s')


def _check_services_ready(settings: Settings):
    loop = asyncio.get_event_loop()
    coros = [
        _check_port_open(settings.elastic_host, settings.elastic_port, loop),
        _check_port_open(settings.redis_host, settings.redis_port, loop),
    ]
    loop.run_until_complete(asyncio.gather(*coros, loop=loop))


@click.group()
@click.pass_context
def cli(ctx):
    """
    Run morpheus
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)
    ctx.obj['settings'] = settings


@cli.command()
@click.pass_context
def web(ctx):
    """
    Serve the application
    If the database doesn't already exist it will be created.
    """
    settings = ctx.obj['settings']
    logger.info('settings: %s', settings)
    logger.info('waiting for elasticsearch and redis to come up...')
    # give es a chance to come up fully, this just prevents lots of es errors, create_indices is itself lenient
    sleep(4)
    _check_services_ready(settings)

    _elasticsearch_setup(settings)
    logger.info('starting server...')
    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = create_app(loop, settings)
    run_app(app, port=8000, loop=loop, print=lambda v: None)


@cli.command()
@click.pass_context
def worker(ctx):
    """
    Run the worker
    """
    logger.info('waiting for elasticsearch and redis to come up...')
    sleep(4)
    _check_services_ready(ctx.obj['settings'])
    # redis/the network occasionally hangs and gets itself in a mess if we try to connect too early,
    # even once it's "up", hence 2 second wait
    sleep(2)
    RunWorkerProcess('app/worker.py', 'Worker')


def _elasticsearch_setup(settings, force=False):
    es = ElasticSearch(settings=settings)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(es.create_indices(delete_existing=force))
    es.close()


@cli.command()
@click.option('--force', is_flag=True)
@click.pass_context
def elasticsearch_setup(ctx, force):
    _elasticsearch_setup(ctx.obj['settings'], force)


EXEC_LINES = [
    'import asyncio, os, re, sys',
    'from datetime import datetime, timedelta, timezone',
    'from pprint import pprint as pp',
    '',
    'from app.settings import Settings',
    '',
    'loop = asyncio.get_event_loop()',
    'await_ = loop.run_until_complete',
    'settings = Settings()',
]
EXEC_LINES += (
    ['print("\\n    Python {v.major}.{v.minor}.{v.micro}\\n".format(v=sys.version_info))'] +
    [f'print("    {l}")' for l in EXEC_LINES]
)


@cli.command()
def shell():
    from IPython import start_ipython
    from IPython.terminal.ipapp import load_default_config
    c = load_default_config()

    c.TerminalIPythonApp.display_banner = False
    c.TerminalInteractiveShell.confirm_exit = False
    c.InteractiveShellApp.exec_lines = EXEC_LINES
    start_ipython(argv=(), config=c)


if __name__ == '__main__':
    cli(obj={})
