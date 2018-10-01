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

-- set client_min_messages to 'NOTICE';


CREATE FUNCTION set_message_vector() RETURNS trigger AS $$
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
    -- '%a %Y-%m-%d %H:%M'
    return to_char(v, 'Dy YYYY-MM-DD HH24:MI TZ');
  END;
$$ LANGUAGE plpgsql;
