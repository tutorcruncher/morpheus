#!/usr/bin/env python3.6
import asyncio
import logging
import os
from time import sleep

import click
import uvloop
from arq import RunWorkerProcess
from gunicorn.app.base import BaseApplication

from app.es import ElasticSearch
from app.logs import setup_logging
from app.main import create_app
from app.settings import Settings

logger = logging.getLogger('morpheus.main')


@click.group()
@click.pass_context
def cli(ctx):
    """
    Run morpheus
    """
    settings = Settings(sender_cls='app.worker.Sender')
    setup_logging(settings)
    logger.info('settings: %s', settings)
    ctx.obj['settings'] = settings


@cli.command()
@click.pass_context
def web(ctx):
    """
    Serve the application
    If the database doesn't already exist it will be created.
    """
    sleep(10)  # TODO
    logger.info('waiting for elastic search and redis to come up...')
    _elasticsearch_setup(ctx.obj['settings'])

    config = dict(
        worker_class='aiohttp.worker.GunicornUVLoopWebWorker',
        bind=os.getenv('BIND', '127.0.0.1:8000'),
        max_requests=5000,
        max_requests_jitter=500,
    )

    class Application(BaseApplication):
        def load_config(self):
            for k, v in config.items():
                self.cfg.set(k, v)

        def load(self):
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            loop = asyncio.get_event_loop()
            return create_app(loop, ctx.obj['settings'])

    logger.info('starting gunicorn...')
    Application().run()


@cli.command()
def worker(**kwargs):
    """
    Run the worker
    """
    sleep(10)  # TODO
    logger.info('waiting for redis to come up...')
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


if __name__ == '__main__':
    cli(obj={})
