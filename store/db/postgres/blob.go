package postgres

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

// postgresBlobExecutor is satisfied by both *sql.DB and *sql.Tx so blob
// read-modify-write sequences run inside the caller's transaction.
type postgresBlobExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// acquirePostgresBlob inserts or increments the blob row bound by a new
// attachment inside its insert transaction, returning the blob id. The blob
// row is taken with FOR UPDATE, and INSERT ... ON CONFLICT DO NOTHING plus a
// re-lock resolves the race where two transactions find no row concurrently.
func acquirePostgresBlob(ctx context.Context, exec postgresBlobExecutor, acquire *store.BlobAcquire) (int64, error) {
	if acquire == nil {
		return 0, errors.New("blob acquire is required")
	}
	id, status, count, found, err := lockPostgresBlob(ctx, exec, acquire.StorageID, acquire.Sha256)
	if err != nil {
		return 0, err
	}
	if !found {
		inserted := true
		id, err = insertPostgresBlob(ctx, exec, acquire)
		if errors.Is(err, sql.ErrNoRows) {
			// A concurrent transaction won the unique insert race; its row is
			// now committed (or this one aborted), so lock and acquire it.
			inserted = false
		} else if err != nil {
			return 0, err
		}
		if inserted {
			return id, nil
		}
		id, status, count, found, err = lockPostgresBlob(ctx, exec, acquire.StorageID, acquire.Sha256)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, errors.New("failed to lock blob after insert conflict")
		}
	}
	if status == store.BlobStatusCorrupt {
		if count > 0 {
			return 0, errors.New("blob is corrupt")
		}
		// An unreferenced corrupt row is replaced by the freshly verified upload.
		if _, err := exec.ExecContext(ctx,
			`UPDATE blob SET size = $1, reference_count = 1, status = 'ACTIVE', content = $2, location = $3, orphaned_ts = NULL, updated_ts = EXTRACT(EPOCH FROM NOW()) WHERE id = $4`,
			acquire.Size, acquire.Content, acquire.Location, id,
		); err != nil {
			return 0, errors.Wrap(err, "failed to replace corrupt blob")
		}
		return id, nil
	}
	resurrected := status == store.BlobStatusPendingDelete
	if _, err := exec.ExecContext(ctx,
		`UPDATE blob SET reference_count = reference_count + 1, status = 'ACTIVE', orphaned_ts = NULL, updated_ts = EXTRACT(EPOCH FROM NOW()) WHERE id = $1`,
		id,
	); err != nil {
		return 0, errors.Wrap(err, "failed to acquire blob reference")
	}
	if resurrected {
		if err := insertPostgresBlobEvent(ctx, exec, &store.BlobEvent{
			BlobID: &id, StorageID: acquire.StorageID, Sha256: acquire.Sha256,
			Type:   store.BlobEventResurrect,
			Detail: blobEventDetail(map[string]any{"reference_count": count + 1}),
		}); err != nil {
			return 0, err
		}
	}
	return id, nil
}

// lockPostgresBlob locks and returns the blob row for the given content key.
// found is false when no row exists yet.
func lockPostgresBlob(ctx context.Context, exec postgresBlobExecutor, storageID, sha256 string) (id int64, status string, count int32, found bool, err error) {
	err = exec.QueryRowContext(ctx,
		"SELECT id, status, reference_count FROM blob WHERE storage_id = $1 AND sha256 = $2 FOR UPDATE",
		storageID, sha256,
	).Scan(&id, &status, &count)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", 0, false, nil
	}
	if err != nil {
		return 0, "", 0, false, errors.Wrap(err, "failed to load blob")
	}
	return id, status, count, true, nil
}

// insertPostgresBlob inserts a fresh ACTIVE blob with reference_count 1.
// sql.ErrNoRows means the insert was skipped due to a unique conflict and the
// caller must lock the winner instead.
func insertPostgresBlob(ctx context.Context, exec postgresBlobExecutor, acquire *store.BlobAcquire) (int64, error) {
	var id int64
	err := exec.QueryRowContext(ctx,
		`INSERT INTO blob (storage_id, sha256, size, reference_count, status, content, location, created_ts, updated_ts)
		 VALUES ($1, $2, $3, 1, 'ACTIVE', $4, $5, EXTRACT(EPOCH FROM NOW()), EXTRACT(EPOCH FROM NOW()))
		 ON CONFLICT (storage_id, sha256) DO NOTHING
		 RETURNING id`,
		acquire.StorageID, acquire.Sha256, acquire.Size, acquire.Content, acquire.Location,
	).Scan(&id)
	return id, errors.Wrap(err, "failed to insert blob")
}

// releasePostgresAttachments decrements blob reference counts for every deleted
// attachment inside the caller's transaction. Blobs reaching zero enter the
// grace period and get a GRACE_START audit event. Managed attachments
// without a blob row (deleted before backfill completed) register their
// physical object as a pending orphan instead.
func releasePostgresAttachments(ctx context.Context, exec postgresBlobExecutor, attachments []*store.Attachment) error {
	now := time.Now().Unix()
	for _, attachment := range attachments {
		if attachment == nil {
			continue
		}
		if attachment.BlobID != nil {
			if err := releasePostgresBlobRef(ctx, exec, *attachment.BlobID, now); err != nil {
				return err
			}
			continue
		}
		if err := registerPostgresLegacyOrphan(ctx, exec, attachment, now); err != nil {
			return err
		}
	}
	return nil
}

func releasePostgresBlobRef(ctx context.Context, exec postgresBlobExecutor, blobID int64, now int64) error {
	var (
		count     int32
		storageID string
		sha       string
	)
	err := exec.QueryRowContext(ctx,
		`SELECT reference_count, storage_id, sha256 FROM blob WHERE id = $1 FOR UPDATE`,
		blobID,
	).Scan(&count, &storageID, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		// Unknown; nothing to release.
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to release blob reference")
	}
	if count <= 0 {
		// Already zero; nothing to release.
		return nil
	}
	if _, err := exec.ExecContext(ctx,
		`UPDATE blob SET reference_count = $1, updated_ts = EXTRACT(EPOCH FROM NOW()) WHERE id = $2`,
		count-1, blobID,
	); err != nil {
		return errors.Wrap(err, "failed to release blob reference")
	}
	if count > 1 {
		return nil
	}
	if _, err := exec.ExecContext(ctx,
		`UPDATE blob SET status = 'PENDING_DELETE', orphaned_ts = EXTRACT(EPOCH FROM NOW()), updated_ts = EXTRACT(EPOCH FROM NOW()) WHERE id = $1`,
		blobID,
	); err != nil {
		return errors.Wrap(err, "failed to mark blob pending delete")
	}
	return insertPostgresBlobEvent(ctx, exec, &store.BlobEvent{
		BlobID: &blobID, StorageID: storageID, Sha256: sha,
		Type:   store.BlobEventGraceStart,
		Detail: blobEventDetail(map[string]any{"orphaned_ts": now}),
	})
}

func registerPostgresLegacyOrphan(ctx context.Context, exec postgresBlobExecutor, attachment *store.Attachment, now int64) error {
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
		 VALUES ($1, $2, $3, $4, $5, 'PENDING', EXTRACT(EPOCH FROM NOW()), EXTRACT(EPOCH FROM NOW()))`,
		storageID, attachment.StorageType.String(), location, payload, store.BlobOrphanLegacyDelete,
	); err != nil {
		return errors.Wrap(err, "failed to register legacy attachment orphan")
	}
	return nil
}

func insertPostgresBlobEvent(ctx context.Context, exec postgresBlobExecutor, event *store.BlobEvent) error {
	if event.CreatedTs == 0 {
		event.CreatedTs = time.Now().Unix()
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO blob_event (blob_id, storage_id, sha256, type, detail, created_ts)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		event.BlobID, event.StorageID, event.Sha256, event.Type, event.Detail, event.CreatedTs,
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
		where = append(where, "id = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.StorageID; v != nil {
		where = append(where, "storage_id = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.Sha256; v != nil {
		where = append(where, "sha256 = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.Status; v != nil {
		where = append(where, "status = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if len(find.StatusIn) > 0 {
		holders := make([]string, 0, len(find.StatusIn))
		for _, status := range find.StatusIn {
			holders = append(holders, placeholder(len(args)+1))
			args = append(args, status)
		}
		where = append(where, "status IN ("+strings.Join(holders, ", ")+")")
	}
	if v := find.OrphanedBeforeTs; v != nil {
		where = append(where, "status = 'PENDING_DELETE' AND orphaned_ts IS NOT NULL AND orphaned_ts <= "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	contentColumn := "NULL::bytea AS content"
	if find.IncludeContent {
		contentColumn = "content"
	}
	query := "SELECT id, storage_id, sha256, size, reference_count, status, " + contentColumn + ", location, created_ts, updated_ts, orphaned_ts FROM blob WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT " + placeholder(len(args)+1)
		args = append(args, *find.Limit)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blobs")
	}
	defer rows.Close()

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
		where = append(where, "blob_id = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.Type; v != nil {
		where = append(where, "type = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	query := "SELECT id, blob_id, storage_id, sha256, type, detail, created_ts FROM blob_event WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT " + placeholder(len(args)+1)
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
		where = append(where, "id = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.Status; v != nil {
		where = append(where, "status = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	if v := find.Reason; v != nil {
		where = append(where, "reason = "+placeholder(len(args)+1))
		args = append(args, *v)
	}
	query := "SELECT id, storage_id, storage_type, location, payload, reason, status, created_ts, updated_ts FROM blob_orphan WHERE " + strings.Join(where, " AND ") + " ORDER BY id"
	if find.Limit != nil {
		query += " LIMIT " + placeholder(len(args)+1)
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
		if err := d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM attachment WHERE blob_id = $1", blobID).Scan(&count); err != nil {
			return nil, errors.Wrap(err, "failed to count attachment blob refs")
		}
		counts[blobID] = count
	}
	return counts, nil
}

var errPostgresBlobGCNotImplemented = errors.New("blob GC/backfill not yet implemented for postgres")

func (d *DB) AppendBlobEvent(context.Context, *store.BlobEvent) error { return errPostgresBlobGCNotImplemented }
func (d *DB) ListBackfillAttachments(context.Context, int) ([]*store.Attachment, error) {
	return nil, errPostgresBlobGCNotImplemented
}
func (d *DB) BackfillAttachmentBlob(context.Context, *store.Attachment, *store.BlobAcquire, string) (bool, error) {
	return false, errPostgresBlobGCNotImplemented
}
func (d *DB) MarkAttachmentBackfillStatus(context.Context, int32, string) error {
	return errPostgresBlobGCNotImplemented
}
func (d *DB) ClaimDueBlob(context.Context, int64) (*store.Blob, error) { return nil, errPostgresBlobGCNotImplemented }
func (d *DB) FinishBlobDelete(context.Context, *store.Blob) error { return errPostgresBlobGCNotImplemented }
func (d *DB) MarkBlobCorrupt(context.Context, *store.Blob, string) error { return errPostgresBlobGCNotImplemented }
func (d *DB) ListDueBlobOrphans(context.Context, int64, int) ([]*store.BlobOrphan, error) {
	return nil, errPostgresBlobGCNotImplemented
}
func (d *DB) DeleteBlobOrphan(context.Context, int64) error { return errPostgresBlobGCNotImplemented }
func (d *DB) BlobLocationReferenced(context.Context, string, string) (bool, error) {
	return false, errPostgresBlobGCNotImplemented
}
func (d *DB) LegacyAttachmentAtLocation(context.Context, string, string) (bool, error) {
	return false, errPostgresBlobGCNotImplemented
}
