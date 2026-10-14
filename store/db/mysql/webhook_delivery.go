package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

// webhookDeliveryColumns is the full column list in scan order. All timestamp
// columns are epoch seconds, so every driver scans them identically.
const webhookDeliveryColumns = "`id`, `created_ts`, `updated_ts`, `idempotency_key`, `webhook_id`, `user_id`, `url`, `label`, `signing_secret`, `payload`, `message_id`, `status`, `attempts`, `max_attempts`, `next_attempt_ts`, `claimed_by`, `last_status_code`, `last_error`, `succeeded_ts`"

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanWebhookDelivery(scanner rowScanner) (*store.WebhookDelivery, error) {
	delivery := &store.WebhookDelivery{}
	var status string
	if err := scanner.Scan(
		&delivery.ID,
		&delivery.CreatedTs,
		&delivery.UpdatedTs,
		&delivery.IdempotencyKey,
		&delivery.WebhookID,
		&delivery.UserID,
		&delivery.URL,
		&delivery.Label,
		&delivery.SigningSecret,
		&delivery.Payload,
		&delivery.MessageID,
		&status,
		&delivery.Attempts,
		&delivery.MaxAttempts,
		&delivery.NextAttemptTs,
		&delivery.ClaimedBy,
		&delivery.LastStatusCode,
		&delivery.LastError,
		&delivery.SucceededTs,
	); err != nil {
		return nil, err
	}
	delivery.Status = store.WebhookDeliveryStatus(status)
	return delivery, nil
}

func (d *DB) CreateWebhookDelivery(ctx context.Context, create *store.CreateWebhookDelivery) (*store.WebhookDelivery, bool, error) {
	// INSERT IGNORE turns a duplicate idempotency key into a no-op; the
	// subsequent SELECT returns the previously stored row either way, so a
	// duplicate event trigger never produces a second delivery.
	fields := []string{"`idempotency_key`", "`webhook_id`", "`user_id`", "`url`", "`label`", "`signing_secret`", "`payload`", "`message_id`", "`max_attempts`"}
	placeholders := []string{"?", "?", "?", "?", "?", "?", "?", "?", "?"}
	args := []any{create.IdempotencyKey, create.WebhookID, create.UserID, create.URL, create.Label, create.SigningSecret, string(create.Payload), create.MessageID, create.MaxAttempts}

	stmt := "INSERT IGNORE INTO `webhook_delivery` (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	result, err := d.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to create webhook delivery")
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}

	existing, err := d.getWebhookDeliveryByIdempotencyKey(ctx, create.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	return existing, inserted == 1, nil
}

func (d *DB) getWebhookDeliveryByIdempotencyKey(ctx context.Context, key string) (*store.WebhookDelivery, error) {
	query := "SELECT " + webhookDeliveryColumns + " FROM `webhook_delivery` WHERE `idempotency_key` = ?"
	delivery, err := scanWebhookDelivery(d.db.QueryRowContext(ctx, query, key))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get webhook delivery by idempotency key")
	}
	return delivery, nil
}

func (d *DB) ListWebhookDeliveries(ctx context.Context, find *store.FindWebhookDelivery) ([]*store.WebhookDelivery, error) {
	where, args := []string{"1 = 1"}, []any{}

	if find.ID != nil {
		where, args = append(where, "`id` = ?"), append(args, *find.ID)
	}
	if len(find.IDs) > 0 {
		where, args = append(where, "`id` IN ("+questionMarks(len(find.IDs))+")"), append(args, int64SliceToAny(find.IDs)...)
	}
	if find.IdempotencyKey != nil {
		where, args = append(where, "`idempotency_key` = ?"), append(args, *find.IdempotencyKey)
	}
	if find.WebhookID != nil {
		where, args = append(where, "`webhook_id` = ?"), append(args, *find.WebhookID)
	}
	if find.UserID != nil {
		where, args = append(where, "`user_id` = ?"), append(args, *find.UserID)
	}
	if find.Status != nil {
		where, args = append(where, "`status` = ?"), append(args, string(*find.Status))
	}

	query := "SELECT " + webhookDeliveryColumns + " FROM `webhook_delivery` WHERE " + strings.Join(where, " AND ") + " ORDER BY `id` ASC"
	if find.Limit != nil {
		query = fmt.Sprintf("%s LIMIT %d", query, *find.Limit)
		if find.Offset != nil {
			query = fmt.Sprintf("%s OFFSET %d", query, *find.Offset)
		}
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []*store.WebhookDelivery{}
	for rows.Next() {
		delivery, err := scanWebhookDelivery(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, delivery)
	}
	return list, rows.Err()
}

// selectClaimableIDsTx locks and returns the next batch of due or stale
// delivery ids inside the caller's transaction. SKIP LOCKED lets concurrent
// workers carve disjoint batches without waiting on each other.
func selectClaimableIDsTx(ctx context.Context, tx *sql.Tx, nowTs int64, staleBeforeTs int64, limit int) ([]int64, error) {
	selectStmt := "SELECT `id` FROM `webhook_delivery` WHERE (`status` = 'PENDING' AND `next_attempt_ts` <= ?) OR (`status` = 'PROCESSING' AND `updated_ts` <= ?) ORDER BY `id` LIMIT ? FOR UPDATE SKIP LOCKED"
	rows, err := tx.QueryContext(ctx, selectStmt, nowTs, staleBeforeTs, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to select claimable webhook deliveries")
	}
	defer rows.Close()

	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (d *DB) ClaimDueWebhookDeliveries(ctx context.Context, limit int, nowTs int64, staleBeforeTs int64, workerID string) ([]*store.WebhookDelivery, error) {
	// MySQL has no UPDATE ... RETURNING. Lock the candidate ids with
	// SKIP LOCKED so concurrent workers claim disjoint batches, mark them in
	// the same transaction, then read the completed rows back.
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin claim transaction")
	}
	defer tx.Rollback()

	ids, err := selectClaimableIDsTx(ctx, tx, nowTs, staleBeforeTs, limit)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []*store.WebhookDelivery{}, tx.Commit()
	}

	updateStmt := "UPDATE `webhook_delivery` SET `status` = 'PROCESSING', `claimed_by` = ?, `attempts` = `attempts` + 1, `updated_ts` = ? WHERE `id` IN (" + questionMarks(len(ids)) + ")"
	updateArgs := []any{workerID, nowTs}
	updateArgs = append(updateArgs, int64SliceToAny(ids)...)
	if _, err := tx.ExecContext(ctx, updateStmt, updateArgs...); err != nil {
		return nil, errors.Wrap(err, "failed to claim webhook deliveries")
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to commit claim transaction")
	}

	// Read the claimed rows outside the transaction; they stay PROCESSING until
	// the worker completes them, so a concurrent worker cannot pick them up
	// unless this process dies and the claim goes stale.
	claimed, err := d.ListWebhookDeliveries(ctx, &store.FindWebhookDelivery{IDs: ids})
	if err != nil {
		return nil, errors.Wrap(err, "failed to load claimed webhook deliveries")
	}
	byID := make(map[int64]*store.WebhookDelivery, len(claimed))
	for _, delivery := range claimed {
		byID[delivery.ID] = delivery
	}
	ordered := make([]*store.WebhookDelivery, 0, len(ids))
	for _, id := range ids {
		if delivery, ok := byID[id]; ok {
			ordered = append(ordered, delivery)
		}
	}
	return ordered, nil
}

func (d *DB) CompleteWebhookDelivery(ctx context.Context, complete *store.CompleteWebhookDelivery) error {
	stmt := "UPDATE `webhook_delivery` SET `status` = ?, `attempts` = ?, `next_attempt_ts` = ?, `last_status_code` = ?, `last_error` = ?, `succeeded_ts` = ?, `claimed_by` = '', `updated_ts` = UNIX_TIMESTAMP() WHERE `id` = ?"
	result, err := d.db.ExecContext(ctx, stmt,
		string(complete.Status),
		complete.Attempts,
		complete.NextAttemptTs,
		complete.LastStatusCode,
		complete.LastError,
		complete.SucceededTs,
		complete.ID,
	)
	if err != nil {
		return errors.Wrap(err, "failed to complete webhook delivery")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.Errorf("webhook delivery %d not found", complete.ID)
	}
	return nil
}

func (d *DB) CountWebhookDeliveriesGrouped(ctx context.Context, webhookID *string) ([]*store.WebhookDeliveryCount, error) {
	where, args := []string{"1 = 1"}, []any{}
	if webhookID != nil {
		where, args = append(where, "`webhook_id` = ?"), append(args, *webhookID)
	}
	query := "SELECT `webhook_id`, `user_id`, `status`, COUNT(*) FROM `webhook_delivery` WHERE " + strings.Join(where, " AND ") + " GROUP BY `webhook_id`, `user_id`, `status` ORDER BY `webhook_id`"
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []*store.WebhookDeliveryCount{}
	for rows.Next() {
		count := &store.WebhookDeliveryCount{}
		var status string
		if err := rows.Scan(&count.WebhookID, &count.UserID, &status, &count.Count); err != nil {
			return nil, err
		}
		count.Status = store.WebhookDeliveryStatus(status)
		list = append(list, count)
	}
	return list, rows.Err()
}

func questionMarks(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}

func int64SliceToAny(values []int64) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}
