-- Post-table SQL: triggers and materialized views that depend on tables existing.
-- Runs after SQLModel.metadata.create_all. Idempotent.

DROP TRIGGER IF EXISTS update_message ON events;
CREATE TRIGGER update_message AFTER INSERT ON events
FOR EACH ROW EXECUTE PROCEDURE update_message();

DROP TRIGGER IF EXISTS create_tsvector ON messages;
CREATE TRIGGER create_tsvector BEFORE INSERT ON messages
FOR EACH ROW EXECUTE PROCEDURE set_message_vector();

-- The materialized view caches 90 days of per-company/method/status/day counts; it backs
-- /messages/{method}/aggregation/. Refreshed hourly by the update_aggregation_view celery beat
-- task. We use IF NOT EXISTS so dyno restarts do NOT wipe the cache; if the view definition
-- needs to change, drop it manually as part of a deploy.
CREATE MATERIALIZED VIEW IF NOT EXISTS message_aggregation AS (
  SELECT company_id, method, status, date::date, count(*)
  FROM (
    SELECT company_id, method, status, date_trunc('day', send_ts) AS date
    FROM messages
    WHERE send_ts > current_timestamp::date - '90 days'::interval
  ) AS t
  GROUP BY company_id, method, status, date
  ORDER BY company_id, method, status, date DESC
);

CREATE INDEX IF NOT EXISTS message_aggregation_method_company
ON message_aggregation USING btree (method, company_id);
