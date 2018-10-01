import asyncio
import logging
import os
from typing import Callable, NamedTuple, Union

import asyncpg
from async_timeout import timeout

from .models import MessageStatus, SendMethod
from .settings import Settings

logger = logging.getLogger('morpheus.db')
patches = []


class Patch(NamedTuple):
    func: Callable
    direct: bool = False


async def lenient_conn(settings: Settings, with_db=True):
    if with_db:
        dsn = settings.pg_dsn
    else:
        dsn, _ = settings.pg_dsn.rsplit('/', 1)

    for retry in range(8, -1, -1):
        try:
            async with timeout(2):
                conn = await asyncpg.connect(dsn=dsn)
        except (asyncpg.PostgresError, OSError) as e:
            if retry == 0:
                raise
            else:
                logger.warning('pg temporary connection error "%s", %d retries remaining...', e, retry)
                await asyncio.sleep(1)
        else:
            log = logger.debug if retry == 8 else logger.info
            log('pg connection successful, version: %s', await conn.fetchval('SELECT version()'))
            return conn


DROP_CONNECTIONS = """
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = $1 AND pid <> pg_backend_pid();
"""


async def prepare_database(settings: Settings, overwrite_existing: Union[bool, Callable]) -> bool:
    """
    (Re)create a fresh database and run migrations.
    :param settings: settings to use for db connection
    :param overwrite_existing: whether or not to drop an existing database if it exists
    :return: whether or not a database has been (re)created
    """
    conn = await lenient_conn(settings, with_db=False)
    try:
        await conn.execute(DROP_CONNECTIONS, settings.pg_name)
        logger.debug('attempting to create database "%s"...', settings.pg_name)
        try:
            await conn.execute('CREATE DATABASE {}'.format(settings.pg_name))
        except (asyncpg.DuplicateDatabaseError, asyncpg.UniqueViolationError):
            if callable(overwrite_existing):
                overwrite_existing = overwrite_existing()

            if overwrite_existing:
                logger.debug('database already exists...')
            else:
                logger.debug('database already exists, skipping table setup')
                return False
        else:
            logger.debug('database did not exist, now created')

        logger.debug('settings db timezone to utc...')
        await conn.execute(f"ALTER DATABASE {settings.pg_name} SET TIMEZONE TO 'UTC';")
    finally:
        await conn.close()

    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        logger.debug('creating tables from model definition...')
        async with conn.transaction():
            await conn.execute(settings.models_sql + '\n' + settings.logic_sql)
    finally:
        await conn.close()
    logger.info('database successfully setup ✓')
    return True


class SimplePgPool:
    def __init__(self, conn):
        self.conn = conn
        self._lock = asyncio.Lock(loop=self.conn._loop)

    def acquire(self):
        return self

    async def __aenter__(self):
        await self._lock.acquire()
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    async def close(self):
        pass


def overwrite_existing_check():
    return input('Confirm database reset? [yN] ') == 'y'


def reset_database(settings: Settings):
    logger.info('resetting database...')
    loop = asyncio.get_event_loop()
    overwrite_existing = os.getenv('CONFIRM_DATABASE_RESET') or overwrite_existing_check
    loop.run_until_complete(prepare_database(settings, overwrite_existing=overwrite_existing))
    logger.info('done.')


def run_patch(settings: Settings, live, patch_name):
    if patch_name is None:
        logger.info('available patches:\n{}'.format(
            '\n'.join('  {}: {}'.format(p.func.__name__, p.func.__doc__.strip('\n ')) for p in patches)
        ))
        return
    patch_lookup = {p.func.__name__: p for p in patches}
    try:
        patch = patch_lookup[patch_name]
    except KeyError as e:
        raise RuntimeError(f'patch "{patch_name}" not found in patches: {[p.func.__name__ for p in patches]}') from e

    if patch.direct:
        if not live:
            raise RuntimeError('direct patches must be called with "--live"')
        logger.info(f'running patch {patch_name} direct')
    else:
        logger.info(f'running patch {patch_name} live {live}')
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_run_patch(settings, live, patch))


async def _run_patch(settings, live, patch: Patch):
    conn = await lenient_conn(settings)
    tr = None
    if not patch.direct:
        tr = conn.transaction()
        await tr.start()
    logger.info('=' * 40)
    try:
        await patch.func(conn, settings=settings, live=live)
    except BaseException:
        logger.info('=' * 40)
        logger.exception('Error running %s patch', patch.func.__name__)
        if not patch.direct:
            await tr.rollback()
        return 1
    else:
        logger.info('=' * 40)
        if patch.direct:
            logger.info('committed patch')
        else:
            if live:
                logger.info('live, committed patch')
                await tr.commit()
            else:
                logger.info('not live, rolling back')
                await tr.rollback()
    finally:
        await conn.close()


def patch(*args, direct=False):
    if args:
        assert len(args) == 1, 'wrong arguments to patch'
        func = args[0]
        patches.append(Patch(func=func))
        return func
    else:
        def wrapper(func):
            patches.append(Patch(func=func, direct=direct))
            return func

        return wrapper


@patch
async def run_logic_sql(conn, settings, **kwargs):
    """
    run logic.sql code.
    """
    await conn.execute(settings.logic_sql)


@patch(direct=True)
async def update_enums(conn, settings, **kwargs):
    """
    update sql enums (must be run direct)
    """
    for t in SendMethod:
        await conn.execute(f"ALTER TYPE SEND_METHODS ADD VALUE IF NOT EXISTS '{t.value}'")
    for t in MessageStatus:
        await conn.execute(f"ALTER TYPE MESSAGE_STATUSES ADD VALUE IF NOT EXISTS '{t.value}'")
