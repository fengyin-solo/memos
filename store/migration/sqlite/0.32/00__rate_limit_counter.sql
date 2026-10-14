-- rate_limit_counter backs the shared, database-supported rate limiter.
-- One row per (scope, key) holds the current fixed window and the previous
-- one, mirroring the in-memory limiter's entry. Rows older than two windows
-- are reaped by the limiter's cleanup loop. IF NOT EXISTS keeps the file a
-- no-op when replayed against a fresh install that already carries it.
CREATE TABLE IF NOT EXISTS rate_limit_counter (
  scope           TEXT    NOT NULL,
  counter_key     TEXT    NOT NULL,
  window_start_ts BIGINT  NOT NULL,
  current_count   INTEGER NOT NULL DEFAULT 0 CHECK (current_count >= 0),
  previous_count  INTEGER NOT NULL DEFAULT 0 CHECK (previous_count >= 0),
  updated_ts      BIGINT  NOT NULL DEFAULT (strftime('%s', 'now')),
  PRIMARY KEY (scope, counter_key)
);

CREATE INDEX IF NOT EXISTS idx_rate_limit_counter_window_start_ts ON rate_limit_counter(window_start_ts);
