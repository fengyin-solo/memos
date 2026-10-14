-- rate_limit_counter backs the shared, database-supported rate limiter.
-- One row per (scope, key) holds the current fixed window and the previous
-- one, mirroring the in-memory limiter's entry. Rows older than two windows
-- are reaped by the limiter's cleanup loop. IF NOT EXISTS keeps the file a
-- no-op when replayed against a fresh install that already carries it.
CREATE TABLE IF NOT EXISTS `rate_limit_counter` (
  `scope`           VARCHAR(64)  NOT NULL,
  `counter_key`     VARCHAR(255) NOT NULL,
  `window_start_ts` BIGINT       NOT NULL,
  `current_count`   INT          NOT NULL DEFAULT 0 CHECK (`current_count` >= 0),
  `previous_count`  INT          NOT NULL DEFAULT 0 CHECK (`previous_count` >= 0),
  `updated_ts`      BIGINT       NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  PRIMARY KEY (`scope`, `counter_key`)
);

-- MySQL has no CREATE INDEX IF NOT EXISTS; guard through a prepared statement
-- so that re-running this file against a database that already carries the
-- index (a fresh install whose schema version was rewound) is a no-op.
SET @idx_rate_limit_counter_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 'rate_limit_counter' AND index_name = 'idx_rate_limit_counter_window_start_ts'
);
SET @idx_rate_limit_counter_stmt = IF(
  @idx_rate_limit_counter_exists = 0,
  'CREATE INDEX `idx_rate_limit_counter_window_start_ts` ON `rate_limit_counter` (`window_start_ts`)',
  'SELECT 1'
);
PREPARE idx_rate_limit_counter_prepared FROM @idx_rate_limit_counter_stmt;
EXECUTE idx_rate_limit_counter_prepared;
DEALLOCATE PREPARE idx_rate_limit_counter_prepared;
