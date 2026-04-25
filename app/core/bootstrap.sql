-- Pre-table SQL: extensions, enums, plpgsql functions.
-- Runs before SQLModel.metadata.create_all. Idempotent.

DO $do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'send_methods') THEN
    CREATE TYPE SEND_METHODS AS ENUM (
      'email-mandrill', 'email-ses', 'email-test', 'sms-messagebird', 'sms-test'
    );
  END IF;
END
$do$;

DO $do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'message_statuses') THEN
    CREATE TYPE MESSAGE_STATUSES AS ENUM (
      'render_failed', 'send_request_failed', 'send', 'deferral', 'hard_bounce', 'soft_bounce',
      'open', 'click', 'spam', 'unsub', 'reject', 'scheduled', 'buffered', 'delivered', 'expired',
      'delivery_failed'
    );
  END IF;
END
$do$;

CREATE OR REPLACE FUNCTION update_message() RETURNS trigger AS $update_message$
  DECLARE
    current_update_ts timestamptz;
  BEGIN
    select update_ts into current_update_ts from messages where id=new.message_id;
    if new.ts > current_update_ts then
      update messages set update_ts=new.ts, status=new.status where id=new.message_id;
    end if;
    return null;
  END;
$update_message$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_message_vector() RETURNS trigger AS $set_message_vector$
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
$set_message_vector$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION iso_ts(v TIMESTAMPTZ, tz VARCHAR(63)) RETURNS VARCHAR(63) AS $iso_ts$
  DECLARE
  BEGIN
    PERFORM set_config('timezone', tz, true);
    return to_char(v, 'YYYY-MM-DD"T"HH24:MI:SSOF');
  END;
$iso_ts$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pretty_ts(v TIMESTAMPTZ, tz VARCHAR(63)) RETURNS VARCHAR(63) AS $pretty_ts$
  DECLARE
  BEGIN
    PERFORM set_config('timezone', tz, true);
    return to_char(v, 'Dy YYYY-MM-DD HH24:MI TZ');
  END;
$pretty_ts$ LANGUAGE plpgsql;
