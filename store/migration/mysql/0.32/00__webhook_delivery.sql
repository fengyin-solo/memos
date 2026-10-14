-- Persistent outbound webhook delivery queue.
-- Rows survive restarts; idempotency_key deduplicates repeated event triggers.
-- Workers atomically claim due PENDING rows and stale PROCESSING rows left
-- behind by crashed workers, then persist the attempt outcome.
-- IF NOT EXISTS keeps the migration re-runnable against databases already
-- initialized from LATEST.sql; MySQL has no CREATE INDEX IF NOT EXISTS, so the
-- indexes are created together with the table.
CREATE TABLE IF NOT EXISTS `webhook_delivery` (
  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `created_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `updated_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `idempotency_key` VARCHAR(128) NOT NULL,
  `webhook_id` VARCHAR(64) NOT NULL DEFAULT '',
  `user_id` INT NOT NULL DEFAULT 0,
  `url` TEXT NOT NULL,
  `label` VARCHAR(128) NOT NULL DEFAULT '',
  `signing_secret` TEXT NOT NULL,
  `payload` MEDIUMTEXT NOT NULL,
  `message_id` VARCHAR(128) NOT NULL DEFAULT '',
  `status` VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  `attempts` INT NOT NULL DEFAULT 0,
  `max_attempts` INT NOT NULL DEFAULT 5,
  `next_attempt_ts` BIGINT NOT NULL DEFAULT 0,
  `claimed_by` VARCHAR(128) NOT NULL DEFAULT '',
  `last_status_code` INT NOT NULL DEFAULT 0,
  `last_error` TEXT NOT NULL,
  `succeeded_ts` BIGINT NOT NULL DEFAULT 0,
  UNIQUE INDEX `idx_webhook_delivery_idempotency_key` (`idempotency_key`),
  INDEX `idx_webhook_delivery_claim` (`status`, `next_attempt_ts`, `updated_ts`),
  INDEX `idx_webhook_delivery_webhook_id` (`webhook_id`)
);
