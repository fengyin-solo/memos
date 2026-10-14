package postgres

import (
	"context"
	"database/sql"
	stderrors "errors"
	"strings"

	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

// webhookDeliveryColumns is the full column list in scan order. All timestamp
// columns are epoch seconds, so every driver scans them identically.
const webhookDeliveryColumns = "id, created_ts, updated_ts, idempotency_key, webhook_id, user_id, url, label, signing_secret, payload, message_id, status, attempts, max_attempts, next_attempt_ts, claimed_by, last_status_code, last_error, succeeded_ts"

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
	fields := []string{"idempotency_key", "webhook_id", "user_id", "url", "label", "signing_secret", "payload", "message_id", "max_attempts"}
	args := []any{create.IdempotencyKey, create.WebhookID, create.UserID, create.URL, create.Label, create.SigningSecret, string(create.Payload), create.MessageID, create.MaxAttempts}

	stmt := "INSERT INTO webhook_delivery (" + strings.Join(fields, ", ") + ") VALUES (" + placeholders(len(args)) + ") ON CONFLICT (idempotency_key) DO NOTHING RETURNING " + webhookDeliveryColumns
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
	query := "SELECT " + webhookDeliveryColumns + " FROM webhook_delivery WHERE idempotency_key = $1"
	delivery, err := scanWebhookDelivery(d.db.QueryRowContext(ctx, query, key))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get webhook delivery by idempotency key")
	}
	return delivery, nil
}

func (d *DB) ListWebhookDeliveries(ctx context.Context, find *store.FindWebhookDelivery) ([]*store.WebhookDelivery, error) {
	where, args := []string{"1 = 1"}, []any{}

	if find.ID != nil {
		where = append(where, "id = "+placeholder(len(args)+1))
		args = append(args, *find.ID)
	}
	if len(find.IDs) > 0 {
		idPlaceholders := make([]string, len(find.IDs))
		for i, id := range find.IDs {
			idPlaceholders[i] = placeholder(len(args) + 1)
			args = append(args, id)
		}
		where = append(where, "id IN ("+strings.Join(idPlaceholders, ", ")+")")
	}
	if find.IdempotencyKey != nil {
		where = append(where, "idempotency_key = "+placeholder(len(args)+1))
		args = append(args, *find.IdempotencyKey)
	}
	if find.WebhookID != nil {
		where = append(where, "webhook_id = "+placeholder(len(args)+1))
		args = append(args, *find.WebhookID)
	}
	if find.UserID != nil {
		where = append(where, "user_id = "+placeholder(len(args)+1))
		args = append(args, *find.UserID)
	}
	if find.Status != nil {
		where = append(where, "status = "+placeholder(len(args)+1))
		args = append(args, string(*find.Status))
	}

	query := "SELECT " + webhookDeliveryColumns + " FROM webhook_delivery WHERE " + strings.Join(where, " AND ") + " ORDER BY id ASC"
	if find.Limit != nil {
		query += " LIMIT " + placeholder(len(args)+1)
		args = append(args, *find.Limit)
		if find.Offset != nil {
			query += " OFFSET " + placeholder(len(args)+1)
			args = append(args, *find.Offset)
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
	// FOR UPDATE SKIP LOCKED lets concurrent workers carve disjoint batches out
	// of the same queue without blocking each other. Rows already PROCESSING
	// with a fresh claim are skipped by the WHERE clause; stale claims left by
	// crashed workers are reclaimed.
	stmt := "UPDATE webhook_delivery SET status = 'PROCESSING', claimed_by = $1, attempts = attempts + 1, updated_ts = $2 " +
		"WHERE id IN (SELECT id FROM webhook_delivery WHERE (status = 'PENDING' AND next_attempt_ts <= $3) OR (status = 'PROCESSING' AND updated_ts <= $4) ORDER BY id LIMIT $5 FOR UPDATE SKIP LOCKED) " +
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
	stmt := "UPDATE webhook_delivery SET status = $1, attempts = $2, next_attempt_ts = $3, last_status_code = $4, last_error = $5, succeeded_ts = $6, claimed_by = '', updated_ts = EXTRACT(EPOCH FROM NOW()) WHERE id = $7"
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
		where = append(where, "webhook_id = "+placeholder(len(args)+1))
		args = append(args, *webhookID)
	}
	query := "SELECT webhook_id, user_id, status, COUNT(*) FROM webhook_delivery WHERE " + strings.Join(where, " AND ") + " GROUP BY webhook_id, user_id, status ORDER BY webhook_id"
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
