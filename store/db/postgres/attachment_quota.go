package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

// chargePostgresAttachmentQuotaTx maintains the creator usage counter and
// enforces the effective quota atomically inside the attachment insert
// transaction. A nil limit only maintains the counter; a zero limit rejects
// every write. The hot path is one UPDATE of the row every user gets at
// creation: row locks queue concurrent writers, which re-evaluate the
// predicate against the latest committed version. An insert only repairs a
// row missing on a legacy or partially migrated database.
func chargePostgresAttachmentQuotaTx(ctx context.Context, tx *sql.Tx, creatorID int32, size int64, limit *int64) error {
	// UPDATE-first: every user normally already has a quota row (created with
	// the account, or backfilled by the 0.32 migration). The predicate update
	// is the authoritative concurrency gate; the row lock queues concurrent
	// writers which re-evaluate against the latest committed version.
	result, err := tx.ExecContext(ctx, `
		UPDATE user_attachment_quota
		SET used_size = used_size + $1
		WHERE user_id = $2
		  AND ($3::BIGINT IS NULL OR ($4::BIGINT > 0 AND used_size + $5 <= $6))`,
		size, creatorID, limit, limit, size, limit)
	if err != nil {
		return errors.Wrap(err, "failed to charge attachment quota")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to count charged attachment quota")
	}
	if rows == 1 {
		return nil
	}

	// Zero affected rows means a rejected write, PostgreSQL's
	// changed-vs-matched rule on a zero-byte no-op, or a row missing on a
	// legacy/partly migrated database. The locking read distinguishes them.
	var usedSize int64
	err = tx.QueryRowContext(ctx,
		`SELECT used_size FROM user_attachment_quota WHERE user_id = $1 FOR UPDATE`,
		creatorID).Scan(&usedSize)
	switch {
	case err == nil:
		if !quotaLimitAllows(limit, usedSize, size) {
			return store.ErrAttachmentQuotaExceeded
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return errors.Wrap(err, "failed to read attachment quota after conditional update")
	}

	// Recovery path (single writer in practice): seed the missing row from
	// the live attachment sum and retry the gate.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO user_attachment_quota (user_id, used_size, limit_size)
		VALUES ($1, (SELECT COALESCE(SUM(size), 0) FROM attachment WHERE creator_id = $2), -1)
		ON CONFLICT (user_id) DO NOTHING`,
		creatorID, creatorID); err != nil {
		return errors.Wrap(err, "failed to initialize attachment quota row")
	}
	retry, err := tx.ExecContext(ctx, `
		UPDATE user_attachment_quota
		SET used_size = used_size + $1
		WHERE user_id = $2
		  AND ($3::BIGINT IS NULL OR ($4::BIGINT > 0 AND used_size + $5 <= $6))`,
		size, creatorID, limit, limit, size, limit)
	if err != nil {
		return errors.Wrap(err, "failed to retry charging attachment quota")
	}
	retryRows, err := retry.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to count retried attachment quota")
	}
	if retryRows == 1 {
		return nil
	}
	usedSize, scanErr := readPostgresQuotaUsedSizeForUpdate(ctx, tx, creatorID)
	if scanErr != nil {
		return errors.Wrap(scanErr, "failed to read attachment quota after repair")
	}
	if !quotaLimitAllows(limit, usedSize, size) {
		return store.ErrAttachmentQuotaExceeded
	}
	return nil
}

func readPostgresQuotaUsedSizeForUpdate(ctx context.Context, tx *sql.Tx, creatorID int32) (int64, error) {
	var usedSize int64
	err := tx.QueryRowContext(ctx,
		`SELECT used_size FROM user_attachment_quota WHERE user_id = $1 FOR UPDATE`,
		creatorID).Scan(&usedSize)
	return usedSize, err
}

// quotaLimitAllows decides whether size bytes may be added at usedSize under
// limit. nil disables enforcement; zero blocks every write.
func quotaLimitAllows(limit *int64, usedSize, size int64) bool {
	return limit == nil || (*limit > 0 && usedSize+size <= *limit)
}

// ensurePostgresUserAttachmentQuotaRow seeds a new user's quota row alongside
// account creation so attachment uploads never race to create it. A missing
// table (undefined_table, current binary briefly opening a pre-migration
// database) is tolerated through a savepoint: in PostgreSQL any error dooms
// the transaction, so the ignored statement must be nested.
func ensurePostgresUserAttachmentQuotaRow(ctx context.Context, tx *sql.Tx, userID int32) error {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT user_attachment_quota_seed"); err != nil {
		return errors.Wrap(err, "failed to open attachment quota savepoint")
	}
	_, err := tx.ExecContext(ctx,
		"INSERT INTO user_attachment_quota (user_id, used_size, limit_size) VALUES ($1, 0, -1) ON CONFLICT (user_id) DO NOTHING",
		userID)
	if err == nil {
		if _, releaseErr := tx.ExecContext(ctx, "RELEASE SAVEPOINT user_attachment_quota_seed"); releaseErr != nil {
			return errors.Wrap(releaseErr, "failed to release attachment quota savepoint")
		}
		return nil
	}
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "42P01" {
		return err
	}
	if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT user_attachment_quota_seed"); rollbackErr != nil {
		return errors.Wrap(rollbackErr, "failed to roll back attachment quota savepoint")
	}
	if _, releaseErr := tx.ExecContext(ctx, "RELEASE SAVEPOINT user_attachment_quota_seed"); releaseErr != nil {
		return errors.Wrap(releaseErr, "failed to release attachment quota savepoint")
	}
	return nil
}

// decrementPostgresAttachmentQuotaTx deducts the bytes of attachments about to be deleted in the same transaction, grouped by creator and clamped at
// zero.
func decrementPostgresAttachmentQuotaTx(ctx context.Context, tx *sql.Tx, attachmentIDs []int32) error {
	for _, batch := range deleteUserBatches(attachmentIDs, deleteUserBatchSize) {
		// Placeholder numbering restarts per Exec, since each statement gets
		// its own argument list.
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, id := range batch {
			placeholders[i] = placeholder(i + 1)
			args[i] = id
		}
		stmt := `
			WITH deleted_attachment(creator_id, size) AS (
				SELECT creator_id, size FROM attachment WHERE id IN (` + strings.Join(placeholders, ",") + `)
			),
			deleted_total(creator_id, total_size) AS (
				SELECT creator_id, SUM(size) FROM deleted_attachment GROUP BY creator_id
			)
			UPDATE user_attachment_quota q
			SET used_size = GREATEST(0, q.used_size - t.total_size)
			FROM deleted_total t
			WHERE q.user_id = t.creator_id`
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return errors.Wrap(err, "failed to deduct attachment quota")
		}
	}
	return nil
}

// GetUserAttachmentQuotas reads quota states from the user table; missing
// quota rows report live attachment usage with no override.
func (d *DB) GetUserAttachmentQuotas(ctx context.Context, find *store.FindUserAttachmentQuota) ([]*store.UserAttachmentQuota, error) {
	where, args := []string{"1 = 1"}, []any{}
	if len(find.UserIDList) > 0 {
		placeholders := make([]string, len(find.UserIDList))
		for i, id := range find.UserIDList {
			placeholders[i] = placeholder(len(args) + 1)
			args = append(args, id)
		}
		where = append(where, `"user".id IN (`+strings.Join(placeholders, ",")+`)`)
	}

	stmt := `
		SELECT
			"user".id,
			COALESCE(user_attachment_quota.used_size,
				(SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = "user".id)) AS used_size,
			user_attachment_quota.limit_size
		FROM "user"
		LEFT JOIN user_attachment_quota ON user_attachment_quota.user_id = "user".id
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY "user".id`
	if find.Limit != nil {
		stmt += fmt.Sprintf(" LIMIT %d", *find.Limit)
		if find.Offset != nil {
			stmt += fmt.Sprintf(" OFFSET %d", *find.Offset)
		}
	}

	rows, err := d.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list attachment quotas")
	}
	defer rows.Close()

	list := make([]*store.UserAttachmentQuota, 0)
	for rows.Next() {
		quota := &store.UserAttachmentQuota{}
		var limit sql.NullInt64
		if err := rows.Scan(&quota.UserID, &quota.UsedSize, &limit); err != nil {
			return nil, errors.Wrap(err, "failed to scan attachment quota")
		}
		if limit.Valid && limit.Int64 >= 0 {
			value := limit.Int64
			quota.LimitSize = &value
		}
		list = append(list, quota)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to iterate attachment quotas")
	}
	return list, nil
}

// UpsertUserAttachmentQuotaLimit sets or clears (limitSize -1) the per-user
// override; it affects no rows when the user does not exist.
func (d *DB) UpsertUserAttachmentQuotaLimit(ctx context.Context, userID int32, limit *int64) error {
	limitValue := int64(-1)
	if limit != nil {
		limitValue = *limit
	}
	result, err := d.db.ExecContext(ctx, `
		INSERT INTO user_attachment_quota (user_id, used_size, limit_size)
		SELECT "user".id,
		       (SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = "user".id),
		       $2
		FROM "user"
		WHERE "user".id = $1
		ON CONFLICT (user_id) DO UPDATE SET limit_size = EXCLUDED.limit_size`,
		userID, limitValue)
	if err != nil {
		return errors.Wrap(err, "failed to upsert attachment quota override")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to count attachment quota override")
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}
