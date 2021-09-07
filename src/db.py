import arq
import asyncio
import logging
from foxglove.db.main import prepare_database as fox_prepare_database
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from src.models import Base
from src.settings import Settings

settings = Settings()
logger = logging.getLogger('db')


def populate_db(engine):
    try:
        Base.metadata.create_all(bind=engine)
    except ProgrammingError as e:
        if 'access method "gin"' in e.args[0]:
            engine.execute('create extension btree_gin;')
            Base.metadata.create_all(bind=engine)
        else:
            raise e


def get_session():
    engine = create_engine(settings.pg_dsn)
    session = sessionmaker(bind=engine)

    populate_db(engine)
    engine.dispose()
    return session


SessionLocal = get_session()


MESSAGE_VECTOR_TRIGGER = """
CREATE OR REPLACE FUNCTION set_message_vector() RETURNS trigger AS $$
  BEGIN
    NEW.vector := setweight(to_tsvector(coalesce(NEW.external_id, '')), 'A') ||
                  setweight(to_tsvector(coalesce(NEW.to_first_name, '')), 'A') ||
                  setweight(to_tsvector(coalesce(NEW.to_last_name, '')), 'A') ||
                  setweight(to_tsvector(coalesce(NEW.to_address, '')), 'A') ||
                  setweight(to_tsvector(coalesce(NEW.subject, '')), 'B') ||
                  setweight(to_tsvector(coalesce(array_to_string(NEW.tags, ' '), '')), 'B') ||
                  setweight(to_tsvector(coalesce(array_to_string(NEW.attachments, ' '), '')), 'C') ||
                  setweight(to_tsvector(coalesce(NEW.body, '')), 'D');
    return NEW;
  END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS create_tsvector ON messages;
CREATE TRIGGER create_tsvector BEFORE INSERT ON messages FOR EACH ROW EXECUTE PROCEDURE set_message_vector();
"""

AGGREGATION_VIEW = """
DROP materialized view IF EXISTS message_aggregation;
CREATE materialized view message_aggregation AS (
  SELECT company_id, method, status, date::date, COUNT(*)
  FROM (
    SELECT company_id, method, status, date_trunc('day', send_ts) AS date
    FROM messages
    WHERE send_ts > current_timestamp::date - '90 days'::interval
  ) AS t
  GROUP BY company_id, method, status, date
  ORDER BY company_id, method, status, date DESC
);

create index if not exists message_aggregation_method_company on message_aggregation using btree (method, company_id);
"""


async def prepare_database(settings: Settings, delete_existing: bool):
    """
    (Re)create a fresh database and run migrations.
    :param delete_existing: whether or not to drop an existing database if it exists
    :return: whether or not a database as (re)created
    """
    await fox_prepare_database(settings, delete_existing)
    _engine = create_engine(settings.pg_dsn)
    if delete_existing:
        redis = await arq.create_pool(settings.redis_settings)
        await redis.flushdb()

    populate_db(_engine)
    _engine.execute(MESSAGE_VECTOR_TRIGGER)
    _engine.execute(AGGREGATION_VIEW)
    _engine.dispose()


def reset_database(settings):
    if not input('Confirm database reset? y/n ').lower() == 'y':
        print('cancelling')
    else:
        print('resetting database...')
        asyncio.run(prepare_database(settings, True))
        print('done.')
