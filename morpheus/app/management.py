import logging
from foxglove.db.main import prepare_database as fox_prepare_database, lenient_conn
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .settings import Settings

settings = Settings()
logger = logging.getLogger('tc-hubspot')


async def prepare_database(settings: Settings, delete_existing: bool):
    """
    (Re)create a fresh database and run migrations.
    :param delete_existing: whether or not to drop an existing database if it exists
    :return: whether or not a database as (re)created
    """
    await fox_prepare_database(settings, delete_existing)
    async with lenient_conn(settings, with_db=True) as conn:
        await conn.execute(UPDATE_MESSAGE_TRIGGER)
        await conn.execute(MESSAGE_VECTOR_TRIGGER)
        await conn.execute(DT_FUNCTIONS)
        await conn.execute(AGGREGATION_VIEW)


prepare_database(settings, False)

engine = create_engine(settings.pg_dsn)
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()
Base.metadata.create_all(bind=engine)


UPDATE_MESSAGE_TRIGGER = """
CREATE OR REPLACE FUNCTION update_message() RETURNS trigger AS $$
  DECLARE
    current_update_ts timestamptz;
  BEGIN
    select update_ts into current_update_ts from messages where id=new.message_id;
    if new.ts > current_update_ts then
      update messages set update_ts=new.ts, status=new.status where id=new.message_id;
    end if;
    return null;
  END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_message ON events;
CREATE TRIGGER update_message AFTER INSERT ON events FOR EACH ROW EXECUTE PROCEDURE update_message();
"""


MESSAGE_VECTOR_TRIGGER = """
CREATE OR REPLACE FUNCTION set_message_vector() RETURNS trigger AS $$
  BEGIN
    RAISE NOTICE '%', NEW.external_id;
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


DT_FUNCTIONS = """
CREATE OR REPLACE FUNCTION iso_ts(v TIMESTAMPTZ, tz VARCHAR(63)) RETURNS VARCHAR(63) AS $$
  DECLARE
  BEGIN
    PERFORM set_config('timezone', tz, true);
    return to_char(v, 'YYYY-MM-DD"T"HH24:MI:SSOF');
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pretty_ts(v TIMESTAMPTZ, tz VARCHAR(63)) RETURNS VARCHAR(63) AS $$
  DECLARE
  BEGIN
    PERFORM set_config('timezone', tz, true);
    return to_char(v, 'Dy YYYY-MM-DD HH24:MI TZ');
  END;
$$ LANGUAGE plpgsql;
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
