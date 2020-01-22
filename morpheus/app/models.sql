create extension btree_gin;

-- should match SendMethod
CREATE TYPE SEND_METHODS AS ENUM ('email-mandrill', 'email-ses', 'email-test', 'sms-messagebird', 'sms-test');

-- { companies
CREATE TABLE companies (
  id SERIAL PRIMARY KEY,
  name VARCHAR(63) NOT NULL UNIQUE  -- TODO rename to code
);
-- } companies

CREATE TABLE message_groups (
  id SERIAL PRIMARY KEY,
  uuid UUID NOT NULL,
  company_id INT NOT NULL REFERENCES companies ON DELETE CASCADE,
  message_method SEND_METHODS NOT NULL,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  from_email VARCHAR(255),
  from_name VARCHAR(255)
);
CREATE UNIQUE INDEX message_group_uuid ON message_groups USING btree (uuid);
CREATE INDEX message_group_company_method ON message_groups USING btree (company_id, message_method);
CREATE INDEX message_group_method ON message_groups USING btree (message_method);
CREATE INDEX message_group_created_ts ON message_groups USING btree (created_ts);
CREATE INDEX message_group_company_id ON message_groups USING btree (company_id);


-- should match MessageStatus
CREATE TYPE MESSAGE_STATUSES AS ENUM (
  'render_failed', 'send_request_failed', 'send', 'deferral', 'hard_bounce', 'soft_bounce', 'open', 'click', 'spam',
  'unsub', 'reject', 'scheduled', 'buffered', 'delivered', 'expired', 'delivery_failed'
);

CREATE TABLE messages (
  id SERIAL PRIMARY KEY,
  external_id VARCHAR(255),
  group_id INT NOT NULL REFERENCES message_groups ON DELETE CASCADE,
  company_id INT NOT NULL REFERENCES companies ON DELETE CASCADE,
  method SEND_METHODS NOT NULL,
  send_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status MESSAGE_STATUSES NOT NULL DEFAULT 'send',
  to_first_name VARCHAR(255),
  to_last_name VARCHAR(255),
  to_user_link VARCHAR(255),
  to_address VARCHAR(255),
  tags VARCHAR(255)[],
  subject TEXT,
  body TEXT,
  attachments VARCHAR(255)[],
  cost FLOAT,
  extra JSONB,
  vector tsvector NOT NULL
);
CREATE INDEX message_company_id ON messages USING btree (company_id);
CREATE INDEX message_group_id_send_ts ON messages USING btree (group_id, send_ts);
CREATE INDEX message_external_id ON messages USING btree (external_id);
CREATE INDEX message_send_ts ON messages USING btree (send_ts desc);
CREATE INDEX message_update_ts ON messages USING btree (update_ts desc);
CREATE INDEX message_tags ON messages USING gin (tags, method, company_id);
CREATE INDEX message_vector ON messages USING gin (vector, method, company_id);
CREATE INDEX message_company_method ON messages USING btree (method, company_id, id);


CREATE TABLE events (
  id SERIAL PRIMARY KEY,
  message_id INT NOT NULL REFERENCES messages ON DELETE CASCADE,
  status MESSAGE_STATUSES NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  extra JSONB
);
CREATE INDEX event_message_id ON events USING btree (message_id);


CREATE TABLE links (
  id SERIAL PRIMARY KEY,
  message_id INT NOT NULL REFERENCES messages ON DELETE CASCADE,
  token VARCHAR(31),
  url TEXT
);
-- CREATE INDEX link_message_id ON links USING btree (message_id);  removed 2020-01-16 - unused
CREATE INDEX link_token ON links USING btree (token);

-- { logic
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
-- } logic
