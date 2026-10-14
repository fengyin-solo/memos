-- Per-user attachment byte quota accounting.
-- used_size is maintained atomically by attachment create/delete transactions;
-- limit_size -1 means the instance default quota applies, >= 0 is an override.
-- Statements are idempotent: test harnesses may replay this migration against
-- a database already initialized from LATEST.sql.
CREATE TABLE IF NOT EXISTS user_attachment_quota (
  user_id INTEGER PRIMARY KEY,
  used_size BIGINT NOT NULL DEFAULT 0,
  limit_size BIGINT NOT NULL DEFAULT -1
);

CREATE INDEX IF NOT EXISTS idx_attachment_creator_id ON attachment(creator_id);

-- Backfill one row for every user, seeded with the authoritative attachment
-- sum. Pre-creating the rows keeps attachment creation down to a single
-- conditional UPDATE with no insert race.
INSERT OR IGNORE INTO user_attachment_quota (user_id, used_size, limit_size)
SELECT user.id,
       (SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = user.id),
       -1
FROM user;

-- One-time reconciliation of missing or inconsistent counters. Safe to re-run.
UPDATE user_attachment_quota
SET used_size = (
  SELECT COALESCE(SUM(size), 0)
  FROM attachment
  WHERE attachment.creator_id = user_attachment_quota.user_id
);
