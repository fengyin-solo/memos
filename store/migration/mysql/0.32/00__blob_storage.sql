-- Content-addressed blob storage. A blob is one physical object identified by
-- (storage_id, sha256); attachments hold reference to it and reference_count
-- tracks how many attachment rows share it.
CREATE TABLE `blob` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `storage_id` VARCHAR(256) NOT NULL,
  `sha256` CHAR(64) NOT NULL,
  `size` BIGINT NOT NULL DEFAULT 0,
  `reference_count` INT NOT NULL DEFAULT 0,
  -- ACTIVE: referenced by attachments; PENDING_DELETE: unreferenced, inside the
  -- grace period; CORRUPT: bytes failed verification, never auto-deleted.
  `status` VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
  -- content holds the bytes for DATABASE storage blobs; NULL otherwise.
  `content` MEDIUMBLOB DEFAULT NULL,
  -- location is the local relative path or S3 object key; empty for DATABASE.
  `location` TEXT NOT NULL,
  `created_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `updated_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `orphaned_ts` BIGINT DEFAULT NULL,
  UNIQUE (`storage_id`, `sha256`),
  KEY `idx_blob_status_orphaned` (`status`, `orphaned_ts`)
);

-- Append-only audit trail of blob lifecycle transitions and GC actions.
CREATE TABLE `blob_event` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `blob_id` INT DEFAULT NULL,
  `storage_id` VARCHAR(256) NOT NULL DEFAULT '',
  `sha256` CHAR(64) NOT NULL DEFAULT '',
  `type` VARCHAR(64) NOT NULL,
  `detail` TEXT NOT NULL,
  `created_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  KEY `idx_blob_event_blob_id` (`blob_id`),
  KEY `idx_blob_event_type_created` (`type`, `created_ts`)
);

-- Physical objects that no blob row should reference: duplicate locations
-- discovered while backfilling existing attachments, objects of attachments deleted
-- before backfill, and abandoned uploads.
CREATE TABLE `blob_orphan` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `storage_id` VARCHAR(256) NOT NULL DEFAULT '',
  `storage_type` VARCHAR(32) NOT NULL DEFAULT '',
  `location` TEXT NOT NULL,
  `payload` TEXT NOT NULL,
  `reason` VARCHAR(64) NOT NULL,
  -- PENDING: awaiting the sweep; DELETED: object removed; MISSING: already gone.
  `status` VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  `created_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  `updated_ts` BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
  KEY `idx_blob_orphan_status_created` (`status`, `created_ts`)
);

ALTER TABLE `attachment`
  ADD COLUMN `blob_id` INT DEFAULT NULL AFTER `payload`,
  ADD COLUMN `blob_backfill_status` VARCHAR(32) NOT NULL DEFAULT '' AFTER `blob_id`;

CREATE INDEX `idx_attachment_blob_id` ON `attachment`(`blob_id`);
