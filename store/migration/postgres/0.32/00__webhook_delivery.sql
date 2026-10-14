-- Persistent outbound webhook delivery queue.
-- Rows survive restarts; idempotency_key deduplicates repeated event triggers.
-- Workers atomically claim due PENDING rows and stale PROCESSING rows left
-- behind by crashed workers, then persist the attempt outcome.
-- IF NOT EXISTS keeps the migration re-runnable against databases already
-- initialized from LATEST.sql.
CREATE TABLE IF NOT EXISTS webhook_delivery (
  id BIGSERIAL PRIMARY KEY,
  created_ts BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
  updated_ts BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
  idempotency_key TEXT NOT NULL,
  webhook_id TEXT NOT NULL DEFAULT '',
  user_id INTEGER NOT NULL DEFAULT 0,
  url TEXT NOT NULL,
  label TEXT NOT NULL DEFAULT '',
  signing_secret TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL,
  message_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_attempt_ts BIGINT NOT NULL DEFAULT 0,
  claimed_by TEXT NOT NULL DEFAULT '',
  last_status_code INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  succeeded_ts BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_delivery_idempotency_key ON webhook_delivery(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_webhook_delivery_claim ON webhook_delivery(status, next_attempt_ts, updated_ts);
CREATE INDEX IF NOT EXISTS idx_webhook_delivery_webhook_id ON webhook_delivery(webhook_id);
