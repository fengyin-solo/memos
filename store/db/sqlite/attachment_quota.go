package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

// chargeSQLiteAttachmentQuotaTx maintains the creator usage counter and
// enforces the effective quota atomically. It runs inside the attachment
// insert transaction: a rejection returns ErrAttachmentQuotaExceeded and the
// caller rolls the attachment insert back, so failed writes never inflate
// usage. A nil limit maintains the counter without enforcing a cap.
//
// The hot path is a single UPDATE of the quota row, which every user receives
// at creation; SQLite serializes writers and re-evaluates the predicate
// against the latest committed value. An insert only repairs a row that a
// legacy or partially migrated database is missing.
func chargeSQLiteAttachmentQuotaTx(ctx context.Context, tx dbExecutor, creatorID int32, size int64, limit *int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE user_attachment_quota
		SET used_size = used_size + ?
		WHERE user_id = ?
		  AND (? IS NULL OR ? > 0 AND used_size + ? <= ?)`,
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

	usedSize, err := readSQLiteQuotaUsedSize(ctx, tx, creatorID)
	if errors.Is(err, sql.ErrNoRows) {
		// Recovery path: seed the missing row from the attachment sum and
		// retry the gate once.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO user_attachment_quota (user_id, used_size)
			VALUES (?, (SELECT COALESCE(SUM(size), 0) FROM attachment WHERE creator_id = ?))
			ON CONFLICT(user_id) DO NOTHING`, creatorID, creatorID); err != nil {
			return errors.Wrap(err, "failed to initialize attachment quota row")
		}
		retry, err := tx.ExecContext(ctx, `
			UPDATE user_attachment_quota
			SET used_size = used_size + ?
			WHERE user_id = ?
			  AND (? IS NULL OR ? > 0 AND used_size + ? <= ?)`,
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
		usedSize, err = readSQLiteQuotaUsedSize(ctx, tx, creatorID)
		if err != nil {
			return errors.Wrap(err, "failed to read attachment quota after repair")
		}
	} else if err != nil {
		return errors.Wrap(err, "failed to read attachment quota after conditional update")
	}
	if !quotaLimitAllows(limit, usedSize, size) {
		return store.ErrAttachmentQuotaExceeded
	}
	return nil
}

func readSQLiteQuotaUsedSize(ctx context.Context, tx dbExecutor, creatorID int32) (int64, error) {
	var usedSize int64
	err := tx.QueryRowContext(ctx, "SELECT used_size FROM user_attachment_quota WHERE user_id = ?", creatorID).Scan(&usedSize)
	return usedSize, err
}

// quotaLimitAllows decides whether size bytes may be added at usedSize under
// limit. nil disables enforcement; zero blocks every write.
func quotaLimitAllows(limit *int64, usedSize, size int64) bool {
	return limit == nil || (*limit > 0 && usedSize+size <= *limit)
}

// ensureSQLiteUserAttachmentQuotaRow seeds a new user's quota row alongside
// account creation so attachment uploads never race to create it. A missing
// table is ignored: when a current binary briefly opens a pre-migration
// database (the upgrade test path), startup migration adds the table before
// traffic is served.
func ensureSQLiteUserAttachmentQuotaRow(ctx context.Context, tx dbExecutor, userID int32) error {
	_, err := tx.ExecContext(ctx,
		"INSERT INTO user_attachment_quota (user_id, used_size, limit_size) VALUES (?, 0, -1) ON CONFLICT(user_id) DO NOTHING",
		userID)
	if err != nil && strings.Contains(err.Error(), "no such table") {
		return nil
	}
	return err
}

// decrementSQLiteAttachmentQuotaTx deducts the bytes of attachments that are
// about to be deleted in the same transaction. It sums the still-visible
// target rows per creator (so retries and missing rows cannot double-deduct)
// and clamps the counter at zero.
func decrementSQLiteAttachmentQuotaTx(ctx context.Context, tx dbExecutor, attachmentIDs []int32) error {
	for _, batch := range deleteUserBatches(attachmentIDs, deleteUserBatchSize) {
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, id := range batch {
			placeholders[i] = "?"
			args[i] = id
		}
		stmt := `
			WITH deleted_attachment(creator_id, size) AS (
				SELECT creator_id, size FROM attachment WHERE id IN (` + strings.Join(placeholders, ",") + `)
			),
			deleted_total(creator_id, total_size) AS (
				SELECT creator_id, SUM(size) FROM deleted_attachment GROUP BY creator_id
			)
			UPDATE user_attachment_quota
			SET used_size = MAX(0, used_size - COALESCE((
				SELECT total_size FROM deleted_total WHERE creator_id = user_attachment_quota.user_id
			), 0))
			WHERE user_id IN (SELECT creator_id FROM deleted_total)`
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return errors.Wrap(err, "failed to deduct attachment quota")
		}
	}
	return nil
}

// GetUserAttachmentQuotas reads quota states from the user table so users
// without a quota row are reported too; their usage is synthesized from the
// live attachment sum.
func (d *DB) GetUserAttachmentQuotas(ctx context.Context, find *store.FindUserAttachmentQuota) ([]*store.UserAttachmentQuota, error) {
	where, args := []string{"1 = 1"}, []any{}
	if len(find.UserIDList) > 0 {
		placeholders := make([]string, len(find.UserIDList))
		for i, id := range find.UserIDList {
			placeholders[i] = "?"
			args = append(args, id)
		}
		where = append(where, "`user`.`id` IN ("+strings.Join(placeholders, ",")+")")
	}

	stmt := `
		SELECT
			` + "`user`.`id`" + `,
			COALESCE(user_attachment_quota.used_size,
				(SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = ` + "`user`.`id`" + `)) AS used_size,
			user_attachment_quota.limit_size
		FROM ` + "`user`" + `
		LEFT JOIN user_attachment_quota ON user_attachment_quota.user_id = ` + "`user`.`id`" + `
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY ` + "`user`.`id`"
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
// override. A missing user affects no rows. When the quota row is missing it is
// created with usage initialized from the live attachment sum.
func (d *DB) UpsertUserAttachmentQuotaLimit(ctx context.Context, userID int32, limit *int64) error {
	limitValue := int64(-1)
	if limit != nil {
		limitValue = *limit
	}
	result, err := d.db.ExecContext(ctx, `
		INSERT INTO user_attachment_quota (user_id, used_size, limit_size)
		SELECT ` + "`user`.`id`" + `,
		       (SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = ` + "`user`.`id`" + `),
		       ?
		FROM ` + "`user`" + `
		WHERE ` + "`user`.`id`" + ` = ?
		ON CONFLICT(user_id) DO UPDATE SET limit_size = excluded.limit_size`,
		limitValue, userID)
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
