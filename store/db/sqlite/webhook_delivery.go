package sqlite

import (
	"context"
	"database/sql"
	stderrors "errors"
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
	fields := []string{"`idempotency_key`", "`webhook_id`", "`user_id`", "`url`", "`label`", "`signing_secret`", "`payload`", "`message_id`", "`max_attempts`"}
	placeholders := []string{"?", "?", "?", "?", "?", "?", "?", "?", "?"}
	args := []any{create.IdempotencyKey, create.WebhookID, create.UserID, create.URL, create.Label, create.SigningSecret, string(create.Payload), create.MessageID, create.MaxAttempts}

	stmt := "INSERT INTO `webhook_delivery` (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ") ON CONFLICT (`idempotency_key`) DO NOTHING RETURNING " + webhookDeliveryColumns
	delivery, err := scanWebhookDelivery(d.db.QueryRowContext(ctx, stmt, args...))
	if err == nil {
		return delivery, true, nil
	}
	if !stderrors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}

	// The idempotency key already exists: return the stored row so duplicate
	// event triggers never produce a second delivery.
	existing, err := d.getWebhookDeliveryByIdempotencyKey(ctx, create.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
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

func (d *DB) ClaimDueWebhookDeliveries(ctx context.Context, limit int, nowTs int64, staleBeforeTs int64, workerID string) ([]*store.WebhookDelivery, error) {
	// The IN-subquery is evaluated before the UPDATE, so reading the same
	// table being updated is legal. The single UPDATE is atomic; concurrent
	// workers serialize on SQLite's write lock and claim disjoint batches.
	stmt := "UPDATE `webhook_delivery` SET `status` = 'PROCESSING', `claimed_by` = ?, `attempts` = `attempts` + 1, `updated_ts` = ? " +
		"WHERE `id` IN (SELECT `id` FROM `webhook_delivery` WHERE (`status` = 'PENDING' AND `next_attempt_ts` <= ?) OR (`status` = 'PROCESSING' AND `updated_ts` <= ?) ORDER BY `id` LIMIT ?) " +
		"RETURNING " + webhookDeliveryColumns
	rows, err := d.db.QueryContext(ctx, stmt, workerID, nowTs, nowTs, staleBeforeTs, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to claim webhook deliveries")
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

func (d *DB) CompleteWebhookDelivery(ctx context.Context, complete *store.CompleteWebhookDelivery) error {
	stmt := "UPDATE `webhook_delivery` SET `status` = ?, `attempts` = ?, `next_attempt_ts` = ?, `last_status_code` = ?, `last_error` = ?, `succeeded_ts` = ?, `claimed_by` = '', `updated_ts` = CAST(strftime('%s', 'now') AS INTEGER) WHERE `id` = ?"
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
