package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

// chargeMySQLAttachmentQuotaTx maintains the creator usage counter and
// enforces the effective quota atomically inside the attachment insert
// transaction. A nil limit only maintains the counter; a zero limit rejects
// every write. The hot path is one UPDATE of the row every user gets at
// creation: InnoDB queues concurrent writers on that row lock and they
// re-evaluate the predicate against committed usage. An insert only repairs a
// row missing on a legacy or partially migrated database.
func chargeMySQLAttachmentQuotaTx(ctx context.Context, tx *sql.Tx, creatorID int32, size int64, limit *int64) error {
	// UPDATE-first: every user normally already has a quota row (created with
	// the account, or backfilled by the 0.32 migration). The predicate update
	// is the authoritative concurrency gate; InnoDB queues writers on the row
	// X lock and they re-evaluate against committed usage.
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

	// Zero affected rows means a rejected write, MySQL's changed-vs-matched
	// rule on a zero-byte no-op, or a row missing on a legacy/partly migrated
	// database. The locking read distinguishes all three.
	var usedSize int64
	err = tx.QueryRowContext(ctx,
		`SELECT used_size FROM user_attachment_quota WHERE user_id = ? FOR UPDATE`,
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
		INSERT IGNORE INTO user_attachment_quota (user_id, used_size, limit_size)
		VALUES (?, (SELECT COALESCE(SUM(size), 0) FROM attachment WHERE creator_id = ?), -1)`,
		creatorID, creatorID); err != nil {
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
	usedSize, scanErr := readMySQLQuotaUsedSizeForUpdate(ctx, tx, creatorID)
	if scanErr != nil {
		return errors.Wrap(scanErr, "failed to read attachment quota after repair")
	}
	if !quotaLimitAllows(limit, usedSize, size) {
		return store.ErrAttachmentQuotaExceeded
	}
	return nil
}

func readMySQLQuotaUsedSizeForUpdate(ctx context.Context, tx *sql.Tx, creatorID int32) (int64, error) {
	var usedSize int64
	err := tx.QueryRowContext(ctx,
		`SELECT used_size FROM user_attachment_quota WHERE user_id = ? FOR UPDATE`,
		creatorID).Scan(&usedSize)
	return usedSize, err
}

// quotaLimitAllows decides whether size bytes may be added at usedSize under
// limit. nil disables enforcement; zero blocks every write.
func quotaLimitAllows(limit *int64, usedSize, size int64) bool {
	return limit == nil || (*limit > 0 && usedSize+size <= *limit)
}

// ensureMySQLUserAttachmentQuotaRow seeds a new user's quota row alongside
// account creation so attachment uploads never race to create it. A missing
// table (ER_NO_SUCH_TABLE) is ignored: when a current binary briefly opens a
// pre-migration database, startup migration adds the table before traffic is
// served.
func ensureMySQLUserAttachmentQuotaRow(ctx context.Context, tx *sql.Tx, userID int32) error {
	_, err := tx.ExecContext(ctx,
		"INSERT IGNORE INTO user_attachment_quota (user_id, used_size, limit_size) VALUES (?, 0, -1)",
		userID)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
			return nil
		}
		return err
	}
	return nil
}

// decrementMySQLAttachmentQuotaTx deducts the bytes of attachments about to
// be deleted in the same transaction, grouped by creator and clamped at zero.
func decrementMySQLAttachmentQuotaTx(ctx context.Context, tx memoUpdateExecer, attachmentIDs []int32) error {
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
			SET used_size = GREATEST(0, used_size - COALESCE((
				SELECT total_size FROM deleted_total WHERE creator_id = user_attachment_quota.user_id
			), 0))
			WHERE user_id IN (SELECT creator_id FROM deleted_total)`
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
// override; it affects no rows when the user does not exist.
func (d *DB) UpsertUserAttachmentQuotaLimit(ctx context.Context, userID int32, limit *int64) error {
	limitValue := int64(-1)
	if limit != nil {
		limitValue = *limit
	}
	result, err := d.db.ExecContext(ctx, `
		INSERT INTO user_attachment_quota (user_id, used_size, limit_size)
		SELECT `+"`user`.`id`"+`,
		       (SELECT COALESCE(SUM(attachment.size), 0) FROM attachment WHERE attachment.creator_id = `+"`user`.`id`"+`),
		       ?
		FROM `+"`user`"+`
		WHERE `+"`user`.`id`"+` = ?
		ON DUPLICATE KEY UPDATE limit_size = VALUES(limit_size)`,
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
