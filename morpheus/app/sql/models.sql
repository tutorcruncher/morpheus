DROP SCHEMA public CASCADE;
CREATE SCHEMA public;


-- should match SendMethod
CREATE TYPE SEND_METHODS AS ENUM ('email-mandrill', 'email-ses', 'email-test', 'sms-messagebird', 'sms-test');

CREATE TABLE message_groups (
  id SERIAL PRIMARY KEY,
  uuid UUID NOT NULL,
  company VARCHAR(63) NOT NULL,
  method SEND_METHODS NOT NULL,
  created_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  from_email VARCHAR(255),
  from_name VARCHAR(255)
);
CREATE UNIQUE INDEX message_group_uuid ON message_groups USING btree (uuid);
CREATE INDEX message_group_company_method ON message_groups USING btree (company, method);
CREATE INDEX message_group_method ON message_groups USING btree (method);
CREATE INDEX message_group_created_ts ON message_groups USING btree (created_ts);


-- should match MessageStatus
CREATE TYPE MESSAGE_STATUSES AS ENUM (
  'render_failed', 'send_request_failed', 'send', 'deferral', 'hard_bounce', 'soft_bounce', 'open', 'click', 'spam',
  'unsub', 'reject', 'scheduled', 'buffered', 'delivered', 'expired', 'delivery_failed'
);

CREATE TABLE messages (
  id SERIAL PRIMARY KEY,
  external_id VARCHAR(255),
  group_id INT NOT NULL REFERENCES message_groups ON DELETE CASCADE,
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
CREATE INDEX message_group_id_send_ts ON messages USING btree (group_id, send_ts);
CREATE INDEX message_external_id ON messages USING btree (external_id);
CREATE INDEX message_status ON messages USING btree (status);
CREATE INDEX message_send_ts ON messages USING btree (send_ts);
CREATE INDEX message_update_ts ON messages USING btree (update_ts);
CREATE INDEX message_tags ON messages USING gin (tags);
CREATE INDEX message_vector ON messages USING gin (vector);


CREATE TABLE events (
  id SERIAL PRIMARY KEY,
  message_id INT NOT NULL REFERENCES messages ON DELETE CASCADE,
  status MESSAGE_STATUSES NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  extra JSONB
);
CREATE INDEX event_message_id ON events USING btree (message_id);
CREATE INDEX event_ts ON events USING btree (ts);


CREATE TABLE links (
  id SERIAL PRIMARY KEY,
  message_id INT NOT NULL REFERENCES messages ON DELETE CASCADE,
  token VARCHAR(31),
  url TEXT
);
CREATE INDEX link_message_id ON links USING btree (message_id);
CREATE INDEX link_token ON links USING btree (token);
