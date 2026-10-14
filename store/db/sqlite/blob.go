package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// acquireSQLiteBlob inserts or increments the blob row bound by a new
// attachment inside its insert transaction, returning the blob id. Writers are
// serialized by BEGIN IMMEDIATE, so the read-modify-write sequence cannot race.
func acquireSQLiteBlob(ctx context.Context, exec dbExecutor, acquire *store.BlobAcquire) (int64, error) {
	if acquire == nil {
		return 0, errors.New("blob acquire is required")
	}
	now := time.Now().Unix()
	var (
		id     int64
		status string
		count  int32
	)
	err := exec.QueryRowContext(ctx,
		"SELECT id, status, reference_count FROM blob WHERE storage_id = ? AND sha256 = ?",
		acquire.StorageID, acquire.Sha256,
	).Scan(&id, &status, &count)
	if errors.Is(err, sql.ErrNoRows) {
		err = exec.QueryRowContext(ctx,
			`INSERT INTO blob (storage_id, sha256, size, reference_count, status, content, location, created_ts, updated_ts)
			 VALUES (?, ?, ?, 1, 'ACTIVE', ?, ?, ?, ?) RETURNING id`,
			acquire.StorageID, acquire.Sha256, acquire.Size, acquire.Content, acquire.Location, now, now,
		).Scan(&id)
		return id, errors.Wrap(err, "failed to insert blob")
	}
	if err != nil {
		return 0, errors.Wrap(err, "failed to load blob")
	}
	if status == store.BlobStatusDeleting {
		return 0, errors.New("blob is being garbage collected")
	}
	if status == store.BlobStatusCorrupt {
		if count > 0 {
			return 0, errors.New("blob is corrupt")
		}
		// An unreferenced corrupt row is replaced by the freshly verified upload.
		if _, err := exec.ExecContext(ctx,
			`UPDATE blob SET size = ?, reference_count = 1, status = 'ACTIVE', content = ?, location = ?, orphaned_ts = NULL, updated_ts = ? WHERE id = ?`,
			acquire.Size, acquire.Content, acquire.Location, now, id,
		); err != nil {
			return 0, errors.Wrap(err, "failed to replace corrupt blob")
		}
		return id, nil
	}
	resurrected := status == store.BlobStatusPendingDelete
	if _, err := exec.ExecContext(ctx,
		`UPDATE blob SET reference_count = reference_count + 1, status = 'ACTIVE', orphaned_ts = NULL, updated_ts = ? WHERE id = ?`,
		now, id,
	); err != nil {
		return 0, errors.Wrap(err, "failed to acquire blob reference")
	}
	if resurrected {
		if err := insertSQLiteBlobEvent(ctx, exec, &store.BlobEvent{
			BlobID: &id, StorageID: acquire.StorageID, Sha256: acquire.Sha256,
			Type:   store.BlobEventResurrect,
			Detail: blobEventDetail(map[string]any{"reference_count": count + 1}),
		}); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// releaseSQLiteAttachments decrements blob reference counts for every deleted
// attachment inside the caller's transaction. Blobs reaching zero enter the
// grace period and get a GRACE_START audit event. Managed attachments
// without a blob row (deleted before backfill completed) register their
// physical object as a pending orphan instead.
func releaseSQLiteAttachments(ctx context.Context, exec dbExecutor, attachments []*store.Attachment) error {
	now := time.Now().Unix()
	for _, attachment := range attachments {
		if attachment == nil {
			continue
		}
		if attachment.BlobID != nil {
			if err := releaseSQLiteBlobRef(ctx, exec, *attachment.BlobID, now); err != nil {
				return err
			}
			continue
		}
		if err := registerSQLiteLegacyOrphan(ctx, exec, attachment, now); err != nil {
			return err
		}
	}
	return nil
}

func releaseSQLiteBlobRef(ctx context.Context, exec dbExecutor, blobID int64, now int64) error {
	var (
		count     int32
		storageID string
		sha       string
	)
	err := exec.QueryRowContext(ctx,
		`UPDATE blob SET reference_count = reference_count - 1, updated_ts = ?
		 WHERE id = ? AND reference_count > 0
		 RETURNING reference_count, storage_id, sha256`,
		now, blobID,
	).Scan(&count, &storageID, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		// Already zero/unknown; nothing to release.
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to release blob reference")
	}
	if count > 0 {
		return nil
	}
	if _, err := exec.ExecContext(ctx,
		`UPDATE blob SET status = 'PENDING_DELETE', orphaned_ts = ?, updated_ts = ? WHERE id = ?`,
		now, now, blobID,
	); err != nil {
		return errors.Wrap(err, "failed to mark blob pending delete")
	}
	return insertSQLiteBlobEvent(ctx, exec, &store.BlobEvent{
		BlobID: &blobID, StorageID: storageID, Sha256: sha,
		Type:   store.BlobEventGraceStart,
		Detail: blobEventDetail(map[string]any{"orphaned_ts": now}),
	})
}

func registerSQLiteLegacyOrphan(ctx context.Context, exec dbExecutor, attachment *store.Attachment, now int64) error {
	var storageID, location, payload string
	switch attachment.StorageType {
	case storepb.AttachmentStorageType_LOCAL:
		storageID = store.BlobStorageLocalID
		location = attachment.Reference
	case storepb.AttachmentStorageType_S3:
		s3Object := attachment.Payload.GetS3Object()
		if s3Object == nil {
			return nil
		}
		storageID = s3Object.StorageId
		location = s3Object.Key
		if attachment.Payload != nil {
			payloadBytes, err := protojson.Marshal(attachment.Payload)
			if err != nil {
				return errors.Wrap(err, "failed to marshal orphan payload")
			}
			payload = string(payloadBytes)
		}
	default:
		// External attachments own no managed object; database bytes die with the row.
		return nil
	}
	if location == "" {
		return nil
	}
	if _, err := exec.ExecContext(ctx,
		`INSERT INTO blob_orphan (storage_id, storage_type, location, payload, reason, status, created_ts, updated_ts)
		 VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)`,
		storageID, attachment.StorageType.String(), location, payload, store.BlobOrphanLegacyDelete, now, now,
	); err != nil {
		return errors.Wrap(err, "failed to register legacy attachment orphan")
	}
	return nil
}

func insertSQLiteBlobEvent(ctx context.Context, exec dbExecutor, event *store.BlobEvent) error {
	if event.CreatedTs == 0 {
		event.CreatedTs = time.Now().Unix()
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO blob_event (blob_id, storage_id, sha256, type, detail, created_ts)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		event.BlobID, event.StorageID, event.Sha256, event.Type, event.Detail, event.CreatedTs,
	)
	return errors.Wrap(err, "failed to insert blob event")
}

func blobPayloadJSON(payload *storepb.AttachmentPayload) string {
	if payload == nil {
		return "{}"
	}
	data, err := protojson.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func blobEventDetail(values map[string]any) string {
	if len(values) == 0 {
		return "{}"
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// ListBlobs lists blob rows.
func (d *DB) ListBlobs(ctx context.Context, find *store.FindBlob) ([]*store.Blob, error) {
	where, args := []string{"1 = 1"}, []any{}
	if v := find.ID; v != nil {
		where, args = append(where, "id = ?"), append(args, *v)
	}
	if v := find.StorageID; v != nil {
		where, args = append(where, "storage_id = ?"), append(args, *v)
	}
	if v := find.Sha256; v != nil {
		where, args = append(where, "sha256 = ?"), append(args, *v)
	}
	if v := find.Status; v != nil {
		where, args = append(where, "status = ?"), append(args, *v)
	}
	if len(find.StatusIn) > 0 {
		where = append(where, "status IN ("+strings.Repeat("?,", len(find.StatusIn)-1)+"?)")
		for _, status := range find.StatusIn {
			args = append(args, status)
		}
	}
	if v := find.OrphanedBeforeTs; v != nil {
		where = append(where, "status = 'PENDING_DELETE' AND orphaned_ts IS NOT NULL AND orphaned_ts <= ?")
		args = append(args, *v)
	}
	contentColumn := "NULL AS content"
	if find.IncludeContent {
		contentColumn = "content"
	}
	query := "SELECT id, storage_id, sha256, size, reference_count, status, " + contentColumn + ", location, created_ts, updated_ts, orphaned_ts FROM blob WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *find.Limit)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blobs")
	}
	defer rows.Close()
	return scanSQLiteBlobs(rows)
}

func scanSQLiteBlobs(rows *sql.Rows) ([]*store.Blob, error) {
	list := make([]*store.Blob, 0)
	for rows.Next() {
		blob := &store.Blob{}
		var orphaned sql.NullInt64
		if err := rows.Scan(
			&blob.ID, &blob.StorageID, &blob.Sha256, &blob.Size, &blob.ReferenceCount,
			&blob.Status, &blob.Content, &blob.Location, &blob.CreatedTs, &blob.UpdatedTs, &orphaned,
		); err != nil {
			return nil, errors.Wrap(err, "failed to scan blob")
		}
		if orphaned.Valid {
			blob.OrphanedTs = &orphaned.Int64
		}
		list = append(list, blob)
	}
	return list, rows.Err()
}

// ListBlobEvents lists blob audit events.
func (d *DB) ListBlobEvents(ctx context.Context, find *store.FindBlobEvent) ([]*store.BlobEvent, error) {
	where, args := []string{"1 = 1"}, []any{}
	if v := find.BlobID; v != nil {
		where, args = append(where, "blob_id = ?"), append(args, *v)
	}
	if v := find.Type; v != nil {
		where, args = append(where, "type = ?"), append(args, *v)
	}
	query := "SELECT id, blob_id, storage_id, sha256, type, detail, created_ts FROM blob_event WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *find.Limit)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blob events")
	}
	defer rows.Close()
	list := make([]*store.BlobEvent, 0)
	for rows.Next() {
		event := &store.BlobEvent{}
		var blobID sql.NullInt64
		if err := rows.Scan(&event.ID, &blobID, &event.StorageID, &event.Sha256, &event.Type, &event.Detail, &event.CreatedTs); err != nil {
			return nil, errors.Wrap(err, "failed to scan blob event")
		}
		if blobID.Valid {
			event.BlobID = &blobID.Int64
		}
		list = append(list, event)
	}
	return list, rows.Err()
}

// ListBlobOrphans lists registered orphan objects.
func (d *DB) ListBlobOrphans(ctx context.Context, find *store.FindBlobOrphan) ([]*store.BlobOrphan, error) {
	where, args := []string{"1 = 1"}, []any{}
	if v := find.ID; v != nil {
		where, args = append(where, "id = ?"), append(args, *v)
	}
	if v := find.Status; v != nil {
		where, args = append(where, "status = ?"), append(args, *v)
	}
	if v := find.Reason; v != nil {
		where, args = append(where, "reason = ?"), append(args, *v)
	}
	query := "SELECT id, storage_id, storage_type, location, payload, reason, status, created_ts, updated_ts FROM blob_orphan WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *find.Limit)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blob orphans")
	}
	defer rows.Close()
	list := make([]*store.BlobOrphan, 0)
	for rows.Next() {
		orphan := &store.BlobOrphan{}
		if err := rows.Scan(&orphan.ID, &orphan.StorageID, &orphan.StorageType, &orphan.Location, &orphan.Payload, &orphan.Reason, &orphan.Status, &orphan.CreatedTs, &orphan.UpdatedTs); err != nil {
			return nil, errors.Wrap(err, "failed to scan blob orphan")
		}
		list = append(list, orphan)
	}
	return list, rows.Err()
}

// AppendBlobEvent writes one audit event.
func (d *DB) AppendBlobEvent(ctx context.Context, event *store.BlobEvent) error {
	return insertSQLiteBlobEvent(ctx, d.db, event)
}

// ListBackfillAttachments lists legacy attachments waiting for blob backfill.
func (d *DB) ListBackfillAttachments(ctx context.Context, limit int) ([]*store.Attachment, error) {
	return d.ListAttachments(ctx, &store.FindAttachment{
		BackfillPending: true,
		GetBlob:          true,
		Limit:            &limit,
	})
}

// BackfillAttachmentBlob binds one legacy attachment to its content-addressed
// blob in a single transaction.
func (d *DB) BackfillAttachmentBlob(ctx context.Context, attachment *store.Attachment, acquire *store.BlobAcquire, legacyLocation string) (bool, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()

	blobID, err := acquireSQLiteBlob(ctx, tx, acquire)
	if err != nil {
		return false, err
	}
	duplicate := false
	if legacyLocation != "" && legacyLocation != acquire.Location {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blob_orphan (storage_id, storage_type, location, payload, reason, status, created_ts, updated_ts)
			 VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)`,
			acquire.StorageID, attachment.StorageType.String(), legacyLocation, blobPayloadJSON(attachment.Payload),
			store.BlobOrphanMigrationDup, now, now,
		); err != nil {
			return false, errors.Wrap(err, "failed to register duplicate backfill orphan")
		}
		duplicate = true
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE attachment SET blob_id = ?, blob_backfill_status = ?, blob = NULL, updated_ts = ? WHERE id = ?`,
		blobID, store.AttachmentBlobBackfillDone, now, attachment.ID,
	); err != nil {
		return false, errors.Wrap(err, "failed to bind backfilled blob")
	}
	return duplicate, tx.Commit()
}

// MarkAttachmentBackfillStatus records FAILED/SKIPPED backfill state.
func (d *DB) MarkAttachmentBackfillStatus(ctx context.Context, attachmentID int32, status string) error {
	_, err := d.db.ExecContext(ctx,
		`UPDATE attachment SET blob_backfill_status = ? WHERE id = ?`, status, attachmentID)
	return errors.Wrap(err, "failed to mark attachment backfill status")
}

// ClaimDueBlob marks a due blob as DELETING so concurrent uploads
// cannot resurrect it while its physical object is removed.
func (d *DB) ClaimDueBlob(ctx context.Context, blobID int64) (*store.Blob, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()
	blob := &store.Blob{}
	var orphaned sql.NullInt64
	err = tx.QueryRowContext(ctx,
		`UPDATE blob SET status = 'DELETING', updated_ts = ?
		 WHERE id = ? AND reference_count = 0 AND status IN ('PENDING_DELETE', 'DELETING')
		 RETURNING id, storage_id, sha256, size, reference_count, status, NULL AS content, location, created_ts, updated_ts, orphaned_ts`,
		now, blobID,
	).Scan(&blob.ID, &blob.StorageID, &blob.Sha256, &blob.Size, &blob.ReferenceCount,
		&blob.Status, &blob.Content, &blob.Location, &blob.CreatedTs, &blob.UpdatedTs, &orphaned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, tx.Commit()
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to claim blob")
	}
	if orphaned.Valid {
		blob.OrphanedTs = &orphaned.Int64
	}
	return blob, tx.Commit()
}

// FinishBlobDelete removes the blob row and writes its completion event.
func (d *DB) FinishBlobDelete(ctx context.Context, blob *store.Blob) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM blob WHERE id = ? AND status IN ('PENDING_DELETE', 'DELETING') AND reference_count = 0`, blob.ID); err != nil {
		return errors.Wrap(err, "failed to delete blob row")
	}
	if err := insertSQLiteBlobEvent(ctx, tx, &store.BlobEvent{
		BlobID: &blob.ID, StorageID: blob.StorageID, Sha256: blob.Sha256,
		Type: store.BlobEventDeleteCompleted,
		Detail: blobEventDetail(map[string]any{"size": blob.Size, "location": blob.Location}),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkBlobCorrupt flags a verified-bad blob and records the detection.
func (d *DB) MarkBlobCorrupt(ctx context.Context, blob *store.Blob, detail string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()
	if _, err := tx.ExecContext(ctx,
		`UPDATE blob SET status = 'CORRUPT', updated_ts = ? WHERE id = ?`, now, blob.ID,
	); err != nil {
		return errors.Wrap(err, "failed to mark blob corrupt")
	}
	if err := insertSQLiteBlobEvent(ctx, tx, &store.BlobEvent{
		BlobID: &blob.ID, StorageID: blob.StorageID, Sha256: blob.Sha256,
		Type: store.BlobEventCorruptDetected, Detail: detail,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// ListDueBlobOrphans lists pending orphans registered before beforeTs.
func (d *DB) ListDueBlobOrphans(ctx context.Context, beforeTs int64, limit int) ([]*store.BlobOrphan, error) {
	rows, err := d.db.QueryContext(ctx,
		`SELECT id, storage_id, storage_type, location, payload, reason, status, created_ts, updated_ts
		 FROM blob_orphan
		 WHERE status = 'PENDING' AND created_ts <= ?
		 ORDER BY id LIMIT ?`, beforeTs, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list due blob orphans")
	}
	defer rows.Close()
	list := make([]*store.BlobOrphan, 0)
	for rows.Next() {
		orphan := &store.BlobOrphan{}
		if err := rows.Scan(&orphan.ID, &orphan.StorageID, &orphan.StorageType, &orphan.Location,
			&orphan.Payload, &orphan.Reason, &orphan.Status, &orphan.CreatedTs, &orphan.UpdatedTs); err != nil {
			return nil, errors.Wrap(err, "failed to scan blob orphan")
		}
		list = append(list, orphan)
	}
	return list, rows.Err()
}

// DeleteBlobOrphan removes one processed orphan row.
func (d *DB) DeleteBlobOrphan(ctx context.Context, orphanID int64) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM blob_orphan WHERE id = ?`, orphanID)
	return errors.Wrap(err, "failed to delete blob orphan")
}

// BlobLocationReferenced reports whether any blob row uses location.
func (d *DB) BlobLocationReferenced(ctx context.Context, storageID, location string) (bool, error) {
	var count int
	if err := d.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM blob WHERE storage_id = ? AND location = ?`, storageID, location,
	).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// LegacyAttachmentAtLocation reports whether a not-backfilled managed
// attachment still occupies the local path or S3 key.
func (d *DB) LegacyAttachmentAtLocation(ctx context.Context, storageType, location string) (bool, error) {
	var count int
	switch storageType {
	case storepb.AttachmentStorageType_S3.String():
		if err := d.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM attachment
			 WHERE blob_id IS NULL AND storage_type = ? AND payload LIKE '%' || ? || '%'`,
			storageType, location,
		).Scan(&count); err != nil {
			return false, err
		}
	default:
		if err := d.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM attachment
			 WHERE blob_id IS NULL AND storage_type = ? AND reference = ?`,
			storageType, location,
		).Scan(&count); err != nil {
			return false, err
		}
	}
	return count > 0, nil
}

// CountAttachmentRefsByBlobs returns attachment reference counts keyed by blob id.
func (d *DB) CountAttachmentRefsByBlobs(ctx context.Context, blobIDs []int64) (map[int64]int32, error) {
	counts := make(map[int64]int32, len(blobIDs))
	for _, blobID := range blobIDs {
		var count int32
		if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM attachment WHERE blob_id = ?", blobID).Scan(&count); err != nil {
			return nil, errors.Wrap(err, "failed to count attachment blob refs")
		}
		counts[blobID] = count
	}
	return counts, nil
}
