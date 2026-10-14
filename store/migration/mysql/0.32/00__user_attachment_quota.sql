-- Per-user attachment byte quota accounting.
-- used_size is maintained atomically by attachment create/delete transactions;
-- limit_size -1 means the instance default quota applies, >= 0 is an override.
CREATE TABLE IF NOT EXISTS `user_attachment_quota` (
  `user_id` INT NOT NULL PRIMARY KEY,
  `used_size` BIGINT NOT NULL DEFAULT 0,
  `limit_size` BIGINT NOT NULL DEFAULT -1
);

-- MySQL has no CREATE INDEX IF NOT EXISTS; guard with a prepared statement so
-- replaying this migration against a LATEST-initialized database is a no-op.
SET @idx_attachment_creator_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 'attachment' AND index_name = 'idx_attachment_creator_id'
);
SET @idx_attachment_creator_stmt = IF(
  @idx_attachment_creator_exists = 0,
  'CREATE INDEX `idx_attachment_creator_id` ON `attachment` (`creator_id`)',
  'SELECT 1'
);
PREPARE idx_attachment_creator_prepared FROM @idx_attachment_creator_stmt;
EXECUTE idx_attachment_creator_prepared;
DEALLOCATE PREPARE idx_attachment_creator_prepared;

-- Backfill one row for every user, seeded with the authoritative attachment
-- sum. Pre-creating the rows keeps attachment creation down to a single
-- conditional UPDATE with no insert race.
INSERT IGNORE INTO `user_attachment_quota` (`user_id`, `used_size`, `limit_size`)
SELECT `user`.`id`,
       (SELECT COALESCE(SUM(`attachment`.`size`), 0) FROM `attachment` WHERE `attachment`.`creator_id` = `user`.`id`),
       -1
FROM `user`;

-- One-time reconciliation of missing or inconsistent counters. Safe to re-run.
UPDATE `user_attachment_quota`
SET `used_size` = (
  SELECT COALESCE(SUM(`size`), 0)
  FROM `attachment`
  WHERE `attachment`.`creator_id` = `user_attachment_quota`.`user_id`
);
