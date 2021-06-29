import logging
from contextlib import contextmanager
from time import sleep

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .settings import Settings

settings = Settings()
logger = logging.getLogger('tc-hubspot')


engine = create_engine(settings.pg_dsn)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
Base.metadata.create_all(bind=engine)


def lenient_connection(settings: Settings, retries=5):
    try:
        return psycopg2.connect(dsn=settings.pg_dsn, password=settings.pg_password)
    except psycopg2.Error as e:
        if retries <= 0:
            raise
        else:
            logger.warning('%s: %s (%d retries remaining)', e.__class__.__name__, e, retries)
            sleep(1)
            return lenient_connection(settings, retries=retries - 1)


@contextmanager
def psycopg2_cursor(settings):
    conn = lenient_connection(settings)
    conn.autocommit = True
    cur = conn.cursor()

    yield cur

    cur.close()
    conn.close()


def populate_db(engine):
    Base.metadata.create_all(engine)


DROP_CONNECTIONS = """
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();
"""


def prepare_database(delete_existing: bool) -> bool:
    """
    (Re)create a fresh database and run migrations.
    :param delete_existing: whether or not to drop an existing database if it exists
    :return: whether or not a database as (re)created
    """
    with psycopg2_cursor(settings) as cur:
        cur.execute('SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname=%s)', (settings.pg_name,))
        already_exists = bool(cur.fetchone()[0])
        if already_exists:
            if not delete_existing:
                print(f'database "{settings.pg_name}" already exists, not recreating it')
                return False
            else:
                print(f'dropping existing connections to "{settings.pg_name}"...')
                cur.execute(DROP_CONNECTIONS, (settings.pg_name,))

                logger.debug('dropping and re-creating the schema...')
                cur.execute('drop schema public cascade;\ncreate schema public;')
        else:
            print(f'database "{settings.pg_name}" does not yet exist, creating')
            cur.execute(f'CREATE DATABASE {settings.pg_name}')

    engine = create_engine(settings.pg_dsn)
    print('creating tables from model definition...')
    populate_db(engine)
    engine.dispose()
    print('db and tables creation finished.')
    return True
