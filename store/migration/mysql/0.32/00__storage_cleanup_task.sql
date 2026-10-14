-- storage_cleanup_task records external attachment objects (local files and
-- S3 objects) whose database rows were deleted by a committed business
-- transaction. A background worker claims the rows by lease and deletes the
-- objects with backoff retry. Rows are kept in their terminal state
-- (SUCCEEDED/FAILED) for reconciliation.
CREATE TABLE `storage_cleanup_task` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `source`           VARCHAR(32)  NOT NULL,
  `storage_type`     VARCHAR(64)  NOT NULL,
  `storage_id`       VARCHAR(128) NOT NULL DEFAULT '',
  `key_hash`         CHAR(64)     NOT NULL,
  `storage_key`      TEXT         NOT NULL,
  `payload`          TEXT         NOT NULL,
  `attachment_uid`   VARCHAR(256) NOT NULL DEFAULT '',
  `status`           VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
  `attempts`         INT          NOT NULL DEFAULT 0,
  `leased_by`        VARCHAR(64)  NOT NULL DEFAULT '',
  `created_ts`       BIGINT       NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `updated_ts`       BIGINT       NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `next_run_ts`      BIGINT       NOT NULL DEFAULT 0,
  `lease_expires_ts` BIGINT       NOT NULL DEFAULT 0,
  `completed_ts`     BIGINT       NOT NULL DEFAULT 0,
  `last_error`       TEXT         NOT NULL,
  UNIQUE (`storage_type`, `storage_id`, `key_hash`)
);

-- Serves the worker claim scan: due PENDING rows or PROCESSING rows whose
-- lease expired after a crash.
CREATE INDEX `idx_storage_cleanup_claim`
  ON `storage_cleanup_task` (`status`, `next_run_ts`, `lease_expires_ts`, `id`);
