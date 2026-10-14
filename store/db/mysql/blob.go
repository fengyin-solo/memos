package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// acquireMySQLBlob inserts or increments the blob row bound by a new
// attachment inside its insert transaction, returning the blob id.
//
// MySQL has no equivalent of SQLite's BEGIN IMMEDIATE: a SELECT ... FOR
// UPDATE against a missing unique key only takes gap locks, so two
// concurrent inserters would deadlock when their INSERTs request insert
// intention locks. The insert therefore runs first as an idempotent upsert
// (ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)); only when the row
// already exists do we take a FOR UPDATE record lock and run the
// read-modify-write status transitions.
func acquireMySQLBlob(ctx context.Context, exec memoUpdateExecer, acquire *store.BlobAcquire) (int64, error) {
	if acquire == nil {
		return 0, errors.New("blob acquire is required")
	}
	result, err := exec.ExecContext(ctx,
		"INSERT INTO `blob` (`storage_id`, `sha256`, `size`, `reference_count`, `status`, `content`, `location`, `created_ts`, `updated_ts`)"+
			" VALUES (?, ?, ?, 1, 'ACTIVE', ?, ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())"+
			" ON DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`)",
		acquire.StorageID, acquire.Sha256, acquire.Size, acquire.Content, acquire.Location,
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to insert blob")
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to read blob insert result")
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, errors.Wrap(err, "failed to read blob id")
	}
	// RowsAffected is 1 for a new row and 0 when the duplicate key kept the
	// existing row untouched.
	if inserted == 1 {
		return id, nil
	}

	var (
		status string
		count  int32
	)
	if err := exec.QueryRowContext(ctx,
		"SELECT `status`, `reference_count` FROM `blob` WHERE `id` = ? FOR UPDATE",
		id,
	).Scan(&status, &count); err != nil {
		return 0, errors.Wrap(err, "failed to lock blob")
	}
	if status == store.BlobStatusCorrupt {
		if count > 0 {
			return 0, errors.New("blob is corrupt")
		}
		// An unreferenced corrupt row is replaced by the freshly verified upload.
		if _, err := exec.ExecContext(ctx,
			"UPDATE `blob` SET `size` = ?, `reference_count` = 1, `status` = 'ACTIVE', `content` = ?, `location` = ?, `orphaned_ts` = NULL, `updated_ts` = UNIX_TIMESTAMP() WHERE `id` = ?",
			acquire.Size, acquire.Content, acquire.Location, id,
		); err != nil {
			return 0, errors.Wrap(err, "failed to replace corrupt blob")
		}
		return id, nil
	}
	resurrected := status == store.BlobStatusPendingDelete
	if _, err := exec.ExecContext(ctx,
		"UPDATE `blob` SET `reference_count` = `reference_count` + 1, `status` = 'ACTIVE', `orphaned_ts` = NULL, `updated_ts` = UNIX_TIMESTAMP() WHERE `id` = ?",
		id,
	); err != nil {
		return 0, errors.Wrap(err, "failed to acquire blob reference")
	}
	if resurrected {
		if err := insertMySQLBlobEvent(ctx, exec, &store.BlobEvent{
			BlobID: &id, StorageID: acquire.StorageID, Sha256: acquire.Sha256,
			Type:   store.BlobEventResurrect,
			Detail: blobEventDetail(map[string]any{"reference_count": count + 1}),
		}); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// releaseMySQLAttachments decrements blob reference counts for every deleted
// attachment inside the caller's transaction. Blobs reaching zero enter the
// grace period and get a GRACE_START audit event. Managed attachments
// without a blob row (deleted before backfill completed) register their
// physical object as a pending orphan instead.
func releaseMySQLAttachments(ctx context.Context, exec memoUpdateExecer, attachments []*store.Attachment) error {
	var now int64
	if err := exec.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()").Scan(&now); err != nil {
		return errors.Wrap(err, "failed to read current timestamp")
	}
	for _, attachment := range attachments {
		if attachment == nil {
			continue
		}
		if attachment.BlobID != nil {
			if err := releaseMySQLBlobRef(ctx, exec, *attachment.BlobID, now); err != nil {
				return err
			}
			continue
		}
		if err := registerMySQLLegacyOrphan(ctx, exec, attachment, now); err != nil {
			return err
		}
	}
	return nil
}

func releaseMySQLBlobRef(ctx context.Context, exec memoUpdateExecer, blobID int64, now int64) error {
	var (
		count     int32
		storageID string
		sha       string
	)
	err := exec.QueryRowContext(ctx,
		"SELECT `id`, `reference_count`, `storage_id`, `sha256` FROM `blob` WHERE `id` = ? FOR UPDATE",
		blobID,
	).Scan(&blobID, &count, &storageID, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		// Unknown; nothing to release.
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to lock blob for release")
	}
	result, err := exec.ExecContext(ctx,
		"UPDATE `blob` SET `reference_count` = `reference_count` - 1, `updated_ts` = ? WHERE `id` = ? AND `reference_count` > 0",
		now, blobID,
	)
	if err != nil {
		return errors.Wrap(err, "failed to release blob reference")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to count released blob reference")
	}
	if affected == 0 || count-1 > 0 {
		return nil
	}
	if _, err := exec.ExecContext(ctx,
		"UPDATE `blob` SET `status` = 'PENDING_DELETE', `orphaned_ts` = ?, `updated_ts` = ? WHERE `id` = ?",
		now, now, blobID,
	); err != nil {
		return errors.Wrap(err, "failed to mark blob pending delete")
	}
	return insertMySQLBlobEvent(ctx, exec, &store.BlobEvent{
		BlobID: &blobID, StorageID: storageID, Sha256: sha,
		Type:   store.BlobEventGraceStart,
		Detail: blobEventDetail(map[string]any{"orphaned_ts": now}),
	})
}

func registerMySQLLegacyOrphan(ctx context.Context, exec memoUpdateExecer, attachment *store.Attachment, now int64) error {
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
		"INSERT INTO `blob_orphan` (`storage_id`, `storage_type`, `location`, `payload`, `reason`, `status`, `created_ts`, `updated_ts`)"+
			" VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)",
		storageID, attachment.StorageType.String(), location, payload, store.BlobOrphanLegacyDelete, now, now,
	); err != nil {
		return errors.Wrap(err, "failed to register legacy attachment orphan")
	}
	return nil
}

func insertMySQLBlobEvent(ctx context.Context, exec memoUpdateExecer, event *store.BlobEvent) error {
	_, err := exec.ExecContext(ctx,
		"INSERT INTO `blob_event` (`blob_id`, `storage_id`, `sha256`, `type`, `detail`, `created_ts`)"+
			" VALUES (?, ?, ?, ?, ?, UNIX_TIMESTAMP())",
		event.BlobID, event.StorageID, event.Sha256, event.Type, event.Detail,
	)
	return errors.Wrap(err, "failed to insert blob event")
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
		where, args = append(where, "`id` = ?"), append(args, *v)
	}
	if v := find.StorageID; v != nil {
		where, args = append(where, "`storage_id` = ?"), append(args, *v)
	}
	if v := find.Sha256; v != nil {
		where, args = append(where, "`sha256` = ?"), append(args, *v)
	}
	if v := find.Status; v != nil {
		where, args = append(where, "`status` = ?"), append(args, *v)
	}
	if len(find.StatusIn) > 0 {
		where = append(where, "`status` IN ("+strings.Repeat("?,", len(find.StatusIn)-1)+"?)")
		for _, status := range find.StatusIn {
			args = append(args, status)
		}
	}
	if v := find.OrphanedBeforeTs; v != nil {
		where = append(where, "`status` = 'PENDING_DELETE' AND `orphaned_ts` IS NOT NULL AND `orphaned_ts` <= ?")
		args = append(args, *v)
	}
	contentColumn := "NULL AS `content`"
	if find.IncludeContent {
		contentColumn = "`content`"
	}
	query := "SELECT `id`, `storage_id`, `sha256`, `size`, `reference_count`, `status`, " + contentColumn + ", `location`, `created_ts`, `updated_ts`, `orphaned_ts` FROM `blob` WHERE " + strings.Join(where, " AND ") + " ORDER BY `id`"
	if find.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *find.Limit)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blobs")
	}
	defer rows.Close()
	return scanMySQLBlobs(rows)
}

func scanMySQLBlobs(rows *sql.Rows) ([]*store.Blob, error) {
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
		where, args = append(where, "`blob_id` = ?"), append(args, *v)
	}
	if v := find.Type; v != nil {
		where, args = append(where, "`type` = ?"), append(args, *v)
	}
	query := "SELECT `id`, `blob_id`, `storage_id`, `sha256`, `type`, `detail`, `created_ts` FROM `blob_event` WHERE " + strings.Join(where, " AND ") + " ORDER BY `id`"
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
		where, args = append(where, "`id` = ?"), append(args, *v)
	}
	if v := find.Status; v != nil {
		where, args = append(where, "`status` = ?"), append(args, *v)
	}
	if v := find.Reason; v != nil {
		where, args = append(where, "`reason` = ?"), append(args, *v)
	}
	query := "SELECT `id`, `storage_id`, `storage_type`, `location`, `payload`, `reason`, `status`, `created_ts`, `updated_ts` FROM `blob_orphan` WHERE " + strings.Join(where, " AND ") + " ORDER BY `id`"
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

// CountAttachmentRefsByBlobs returns attachment reference counts keyed by blob id.
func (d *DB) CountAttachmentRefsByBlobs(ctx context.Context, blobIDs []int64) (map[int64]int32, error) {
	counts := make(map[int64]int32, len(blobIDs))
	for _, blobID := range blobIDs {
		var count int32
		if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `attachment` WHERE `blob_id` = ?", blobID).Scan(&count); err != nil {
			return nil, errors.Wrap(err, "failed to count attachment blob refs")
		}
		counts[blobID] = count
	}
	return counts, nil
}

var errMySQLBlobGCNotImplemented = errors.New("blob GC/backfill not yet implemented for mysql")

func (d *DB) AppendBlobEvent(context.Context, *store.BlobEvent) error { return errMySQLBlobGCNotImplemented }
func (d *DB) ListBackfillAttachments(context.Context, int) ([]*store.Attachment, error) {
	return nil, errMySQLBlobGCNotImplemented
}
func (d *DB) BackfillAttachmentBlob(context.Context, *store.Attachment, *store.BlobAcquire, string) (bool, error) {
	return false, errMySQLBlobGCNotImplemented
}
func (d *DB) MarkAttachmentBackfillStatus(context.Context, int32, string) error {
	return errMySQLBlobGCNotImplemented
}
func (d *DB) ClaimDueBlob(context.Context, int64) (*store.Blob, error) { return nil, errMySQLBlobGCNotImplemented }
func (d *DB) FinishBlobDelete(context.Context, *store.Blob) error { return errMySQLBlobGCNotImplemented }
func (d *DB) MarkBlobCorrupt(context.Context, *store.Blob, string) error { return errMySQLBlobGCNotImplemented }
func (d *DB) ListDueBlobOrphans(context.Context, int64, int) ([]*store.BlobOrphan, error) {
	return nil, errMySQLBlobGCNotImplemented
}
func (d *DB) DeleteBlobOrphan(context.Context, int64) error { return errMySQLBlobGCNotImplemented }
func (d *DB) BlobLocationReferenced(context.Context, string, string) (bool, error) {
	return false, errMySQLBlobGCNotImplemented
}
func (d *DB) LegacyAttachmentAtLocation(context.Context, string, string) (bool, error) {
	return false, errMySQLBlobGCNotImplemented
}
