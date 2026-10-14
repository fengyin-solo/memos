-- Content-addressed blob storage. A blob is one physical object identified by
-- (storage_id, sha256); attachments hold references to it and reference_count
-- tracks how many attachment rows share it.
CREATE TABLE blob (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  storage_id TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size BIGINT NOT NULL DEFAULT 0,
  reference_count INTEGER NOT NULL DEFAULT 0,
  -- ACTIVE: referenced by attachments; PENDING_DELETE: unreferenced, inside the
  -- grace period; CORRUPT: bytes failed verification, never auto-deleted.
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  -- content holds the bytes for DATABASE storage blobs; NULL otherwise.
  content BLOB DEFAULT NULL,
  -- location is the local relative path or S3 object key; empty for DATABASE.
  location TEXT NOT NULL DEFAULT '',
  created_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now')),
  updated_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now')),
  orphaned_ts BIGINT DEFAULT NULL,
  UNIQUE(storage_id, sha256)
);

CREATE INDEX idx_blob_status_orphaned ON blob(status, orphaned_ts);

-- Append-only audit trail of blob lifecycle transitions and GC actions.
CREATE TABLE blob_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  blob_id INTEGER DEFAULT NULL,
  storage_id TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL DEFAULT '',
  type TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '{}',
  created_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX idx_blob_event_blob_id ON blob_event(blob_id);
CREATE INDEX idx_blob_event_type_created ON blob_event(type, created_ts);

-- Physical objects that no blob row should reference: duplicate locations
-- discovered while backfilling existing attachments, objects of attachments deleted
-- before backfill, and abandoned uploads.
CREATE TABLE blob_orphan (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  storage_id TEXT NOT NULL DEFAULT '',
  storage_type TEXT NOT NULL DEFAULT '',
  location TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL DEFAULT '{}',
  reason TEXT NOT NULL,
  -- PENDING: awaiting the sweep; DELETED: object removed; MISSING: already gone.
  status TEXT NOT NULL DEFAULT 'PENDING',
  created_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now')),
  updated_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX idx_blob_orphan_status_created ON blob_orphan(status, created_ts);

ALTER TABLE attachment ADD COLUMN blob_id INTEGER DEFAULT NULL;
ALTER TABLE attachment ADD COLUMN blob_backfill_status TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_attachment_blob_id ON attachment(blob_id);
