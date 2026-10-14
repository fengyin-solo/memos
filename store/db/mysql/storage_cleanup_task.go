package mysql

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

const storageCleanupTaskPageSize = 500

// insertMySQLStorageCleanupTasksTx enqueues cleanup tasks inside a business
// delete transaction. Duplicate objects are ignored so a re-enqueued key
// never aborts the deletion.
func insertMySQLStorageCleanupTasksTx(ctx context.Context, tx *sql.Tx, tasks []*store.StorageCleanupTask) error {
	for _, task := range tasks {
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO storage_cleanup_task
			(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, created_ts, updated_ts)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(task.Source), task.StorageType.String(), task.StorageID, task.KeyHash, task.StorageKey, task.Payload, task.AttachmentUID,
			string(store.StorageCleanupStatusPending), 0, task.CreatedTs, task.UpdatedTs,
		); err != nil {
			return errors.Wrap(err, "failed to enqueue storage cleanup task")
		}
	}
	return nil
}

// InsertStorageCleanupTasks enqueues cleanup tasks outside a transaction.
func (d *DB) InsertStorageCleanupTasks(ctx context.Context, tasks []*store.StorageCleanupTask) error {
	for _, task := range tasks {
		if _, err := d.db.ExecContext(ctx, `INSERT IGNORE INTO storage_cleanup_task
			(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, created_ts, updated_ts)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(task.Source), task.StorageType.String(), task.StorageID, task.KeyHash, task.StorageKey, task.Payload, task.AttachmentUID,
			string(store.StorageCleanupStatusPending), 0, task.CreatedTs, task.UpdatedTs,
		); err != nil {
			return errors.Wrap(err, "failed to enqueue storage cleanup task")
		}
	}
	return nil
}

// ClaimStorageCleanupTasks atomically claims due tasks. Selection and the
// conditional update are separate statements; the WHERE clause on the update
// guarantees each row is claimed by exactly one caller.
func (d *DB) ClaimStorageCleanupTasks(ctx context.Context, nowTs, leaseExpiresTs int64, workerID string, batchSize int) ([]*store.StorageCleanupTask, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid,
		status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error
		FROM storage_cleanup_task
		WHERE (status = ? AND next_run_ts <= ?)
		   OR (status = ? AND lease_expires_ts > 0 AND lease_expires_ts < ?)
		ORDER BY id LIMIT ?`,
		string(store.StorageCleanupStatusPending), nowTs,
		string(store.StorageCleanupStatusProcessing), nowTs,
		batchSize,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list claimable storage cleanup tasks")
	}
	candidates := make([]*store.StorageCleanupTask, 0, batchSize)
	for rows.Next() {
		task, err := scanMySQLStorageCleanupTask(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, task)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to scan claimable storage cleanup tasks")
	}

	claimed := make([]*store.StorageCleanupTask, 0, len(candidates))
	for _, task := range candidates {
		result, err := d.db.ExecContext(ctx, `UPDATE storage_cleanup_task
			SET status = ?, leased_by = ?, attempts = attempts + 1, lease_expires_ts = ?, updated_ts = ?
			WHERE id = ?
			  AND ((status = ? AND next_run_ts <= ?)
			    OR (status = ? AND lease_expires_ts > 0 AND lease_expires_ts < ?))`,
			string(store.StorageCleanupStatusProcessing), workerID, leaseExpiresTs, nowTs, task.ID,
			string(store.StorageCleanupStatusPending), nowTs,
			string(store.StorageCleanupStatusProcessing), nowTs,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to claim storage cleanup task")
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, errors.Wrap(err, "failed to count claimed storage cleanup task")
		}
		if affected != 1 {
			continue
		}
		claimedTask, err := d.getMySQLStorageCleanupTask(ctx, task.ID)
		if err != nil {
			return nil, err
		}
		if claimedTask != nil {
			claimed = append(claimed, claimedTask)
		}
	}
	return claimed, nil
}

func (d *DB) getMySQLStorageCleanupTask(ctx context.Context, id int64) (*store.StorageCleanupTask, error) {
	row := d.db.QueryRowContext(ctx, `SELECT id, source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid,
		status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error
		FROM storage_cleanup_task WHERE id = ?`, id)
	task, err := scanMySQLStorageCleanupTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return task, nil
}

type mysqlStorageCleanupTaskScanner interface {
	Scan(dest ...any) error
}

func scanMySQLStorageCleanupTask(scanner mysqlStorageCleanupTaskScanner) (*store.StorageCleanupTask, error) {
	var (
		task        store.StorageCleanupTask
		source      string
		storageType string
		status      string
	)
	if err := scanner.Scan(
		&task.ID, &source, &storageType, &task.StorageID, &task.KeyHash, &task.StorageKey, &task.Payload, &task.AttachmentUID,
		&status, &task.Attempts, &task.LeasedBy, &task.CreatedTs, &task.UpdatedTs, &task.NextRunTs, &task.LeaseExpiresTs, &task.CompletedTs, &task.LastError,
	); err != nil {
		return nil, errors.Wrap(err, "failed to scan storage cleanup task")
	}
	task.Source = store.StorageCleanupSource(source)
	task.StorageType = storepb.AttachmentStorageType(storepb.AttachmentStorageType_value[storageType])
	task.Status = store.StorageCleanupStatus(status)
	return &task, nil
}

// FinishStorageCleanupTask writes the processing result if the caller still
// holds the lease.
func (d *DB) FinishStorageCleanupTask(ctx context.Context, update *store.StorageCleanupTaskUpdate) (bool, error) {
	var (
		query string
		args  []any
	)
	switch update.Status {
	case store.StorageCleanupStatusSucceeded:
		query = `UPDATE storage_cleanup_task
			SET status = ?, last_error = '', next_run_ts = 0, lease_expires_ts = 0, leased_by = '', completed_ts = ?, updated_ts = ?
			WHERE id = ? AND status = ? AND leased_by = ?`
		args = []any{string(store.StorageCleanupStatusSucceeded), update.Now, update.Now, update.ID, string(store.StorageCleanupStatusProcessing), update.WorkerID}
	case store.StorageCleanupStatusFailed:
		query = `UPDATE storage_cleanup_task
			SET status = ?, last_error = ?, next_run_ts = 0, lease_expires_ts = 0, leased_by = '', completed_ts = ?, updated_ts = ?
			WHERE id = ? AND status = ? AND leased_by = ?`
		args = []any{string(store.StorageCleanupStatusFailed), store.TruncateStorageCleanupError(update.LastError), update.Now, update.Now, update.ID, string(store.StorageCleanupStatusProcessing), update.WorkerID}
	default:
		query = `UPDATE storage_cleanup_task
			SET status = ?, last_error = ?, next_run_ts = ?, lease_expires_ts = 0, leased_by = '', updated_ts = ?
			WHERE id = ? AND status = ? AND leased_by = ?`
		args = []any{string(store.StorageCleanupStatusPending), store.TruncateStorageCleanupError(update.LastError), update.NextRunTs, update.Now, update.ID, string(store.StorageCleanupStatusProcessing), update.WorkerID}
	}
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, errors.Wrap(err, "failed to finish storage cleanup task")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "failed to count finished storage cleanup task")
	}
	return affected == 1, nil
}

// GetStorageCleanupStats groups task counts by source and status.
func (d *DB) GetStorageCleanupStats(ctx context.Context) ([]*store.StorageCleanupSourceStats, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT source, status, COUNT(*) FROM storage_cleanup_task GROUP BY source, status`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query storage cleanup task stats")
	}
	defer rows.Close()
	stats := make([]*store.StorageCleanupSourceStats, 0)
	for rows.Next() {
		var source, status string
		var stat store.StorageCleanupSourceStats
		if err := rows.Scan(&source, &status, &stat.Count); err != nil {
			return nil, errors.Wrap(err, "failed to scan storage cleanup task stats")
		}
		stat.Source = store.StorageCleanupSource(source)
		stat.Status = store.StorageCleanupStatus(status)
		stats = append(stats, &stat)
	}
	return stats, rows.Err()
}

// ListStorageCleanupTasks keyset-paginates all task rows for reconciliation.
func (d *DB) ListStorageCleanupTasks(ctx context.Context, afterID int64, limit int) ([]*store.StorageCleanupTask, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid,
		status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error
		FROM storage_cleanup_task WHERE id > ? ORDER BY id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list storage cleanup tasks")
	}
	defer rows.Close()
	tasks := make([]*store.StorageCleanupTask, 0, limit)
	for rows.Next() {
		task, err := scanMySQLStorageCleanupTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// ListAttachmentStorageRefs lists external object keys referenced by live
// attachments. It pages internally so reconciliation cannot load every blob
// row at once.
func (d *DB) ListAttachmentStorageRefs(ctx context.Context) ([]*store.StorageObjectRef, error) {
	refs := make([]*store.StorageObjectRef, 0)
	var lastID int64
	for {
		rows, err := d.db.QueryContext(ctx, `SELECT id, storage_type, reference, payload FROM attachment
			WHERE id > ? AND storage_type IN (?, ?) ORDER BY id LIMIT ?`,
			lastID, storepb.AttachmentStorageType_LOCAL.String(), storepb.AttachmentStorageType_S3.String(), storageCleanupTaskPageSize)
		if err != nil {
			return nil, errors.Wrap(err, "failed to list attachment storage refs")
		}
		pageCount := 0
		for rows.Next() {
			var (
				id          int64
				storageType string
				reference   string
				payloadRaw  []byte
			)
			if err := rows.Scan(&id, &storageType, &reference, &payloadRaw); err != nil {
				rows.Close()
				return nil, errors.Wrap(err, "failed to scan attachment storage ref")
			}
			pageCount++
			lastID = id
			ref := buildMySQLAttachmentStorageRef(storageType, reference, payloadRaw)
			if ref != nil {
				refs = append(refs, ref)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, errors.Wrap(err, "failed to scan attachment storage refs")
		}
		if pageCount < storageCleanupTaskPageSize {
			return refs, nil
		}
	}
}

func buildMySQLAttachmentStorageRef(storageType, reference string, payloadRaw []byte) *store.StorageObjectRef {
	switch storepb.AttachmentStorageType(storepb.AttachmentStorageType_value[storageType]) {
	case storepb.AttachmentStorageType_LOCAL:
		if reference == "" {
			return nil
		}
		return &store.StorageObjectRef{StorageType: storepb.AttachmentStorageType_LOCAL, StorageID: store.LocalStorageID, StorageKey: reference}
	case storepb.AttachmentStorageType_S3:
		payload := &storepb.AttachmentPayload{}
		if len(payloadRaw) > 0 {
			if err := protojsonUnmarshaler.Unmarshal(payloadRaw, payload); err != nil {
				return nil
			}
		}
		s3Object := payload.GetS3Object()
		if s3Object == nil || s3Object.Key == "" {
			return nil
		}
		return &store.StorageObjectRef{StorageType: storepb.AttachmentStorageType_S3, StorageID: s3Object.StorageId, StorageKey: s3Object.Key}
	default:
		return nil
	}
}
