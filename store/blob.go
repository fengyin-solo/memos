package store

import (
	"context"
)

// Blob lifecycle statuses.
const (
	// BlobStatusActive means the blob is referenced by at least one attachment.
	BlobStatusActive = "ACTIVE"
	// BlobStatusPendingDelete means the reference count dropped to zero and the
	// blob is retained until its grace period elapses.
	BlobStatusPendingDelete = "PENDING_DELETE"
	// BlobStatusCorrupt means the physical object failed content verification; it is
	// never deleted automatically.
	BlobStatusCorrupt = "CORRUPT"
	// BlobStatusDeleting marks a claimed GC deletion while the physical
	// object is being verified/removed. New identical uploads do not
	// acquire it; a crashed GC reclaims it on the next run.
	BlobStatusDeleting = "DELETING"
)

// Blob audit event types.
const (
	// BlobEventGraceStart is appended when reference_count reaches zero.
	BlobEventGraceStart = "GRACE_START"
	// BlobEventResurrect is appended when a pending-delete blob gets a new reference.
	BlobEventResurrect = "RESURRECT"
	// BlobEventDeleteCompleted is appended after the physical object is deleted.
	BlobEventDeleteCompleted = "DELETE_COMPLETED"
	// BlobEventCorruptDetected is appended when object verification fails.
	BlobEventCorruptDetected = "CORRUPT_DETECTED"
	// BlobEventOrphanDelete is appended when an unreferenced object is removed.
	BlobEventOrphanDelete = "ORPHAN_DELETE"
)

// Blob orphan registration reasons.
const (
	// BlobOrphanLegacyDelete marks the object of a managed attachment deleted
	// before its blob backfill completed.
	BlobOrphanLegacyDelete = "LEGACY_DELETE"
	// BlobOrphanMigrationDup marks a physical location made redundant when
	// duplicate legacy attachments were joined onto one blob during backfill.
	BlobOrphanMigrationDup = "MIGRATION_DUP"
	// BlobOrphanAbandonedUpload marks an object the orphan sweep found in the
	// blob prefix with no blob row.
	BlobOrphanAbandonedUpload = "ABANDONED_UPLOAD"
)

// Blob orphan processing statuses.
const (
	BlobOrphanStatusPending = "PENDING"
	BlobOrphanStatusDeleted = "DELETED"
	BlobOrphanStatusMissing = "MISSING"
)

// Attachment blob backfill statuses, stored in attachment.blob_backfill_status.
const (
	// AttachmentBlobBackfillPending means the attachment has not been processed yet.
	AttachmentBlobBackfillPending = ""
	AttachmentBlobBackfillDone    = "DONE"
	AttachmentBlobBackfillFailed  = "FAILED"
	AttachmentBlobBackfillSkipped = "SKIPPED"
)

// Builtin logical storage IDs for content-addressed blobs. Named S3
// storages use their configured storage id.
const (
	BlobStorageLocalID    = "local"
	BlobStorageDatabaseID = "database"
)

// BlobLocalDirName is the data-directory subfolder holding
// content-addressed local blob objects.
const BlobLocalDirName = ".blobs"

// BlobS3KeyPrefix is the object key prefix for content-addressed
// objects in configured S3 storages.
const BlobS3KeyPrefix = "blobs/"

// Blob is one content-addressed physical object shared by attachments.
type Blob struct {
	ID int64
	// StorageID identifies the logical storage: builtin "local"/"database" or a
	// configured S3 storage id.
	StorageID string
	// Sha256 is the lowercase hex digest of the full object bytes.
	Sha256 string
	// Size is the object length in bytes.
	Size int64

	ReferenceCount int32
	Status         string

	// Content holds the bytes for DATABASE storage blobs; nil otherwise.
	Content []byte
	// Location is the local internal path or S3 object key; empty for DATABASE.
	Location string

	CreatedTs  int64
	UpdatedTs  int64
	OrphanedTs *int64
}

// BlobAcquire is the content-addressed object an attachment binds to when it
// is inserted. The blob row is created or incremented in the same transaction
// as the attachment row.
type BlobAcquire struct {
	StorageID string
	Sha256    string
	Size      int64
	Location  string
	// Content carries the bytes for DATABASE storage.
	Content []byte
}

// BlobEvent is one append-only blob lifecycle audit record.
type BlobEvent struct {
	ID        int64
	BlobID    *int64
	StorageID string
	Sha256    string
	Type      string
	// Detail is a JSON object with event-specific context.
	Detail    string
	CreatedTs int64
}

// BlobOrphan describes a physical object that no blob row should reference.
type BlobOrphan struct {
	ID int64
	// StorageID may be empty when the legacy attachment payload cannot resolve it;
	// GC then relies on StorageType/Location/Payload.
	StorageID   string
	StorageType string
	Location    string
	// Payload keeps the legacy attachment payload (e.g. embedded S3 config) so
	// the sweeper can resolve the object just like attachment cleanup does.
	Payload string
	Reason  string
	Status  string

	CreatedTs int64
	UpdatedTs int64
}

// FindBlob selects blob rows.
type FindBlob struct {
	ID        *int64
	StorageID *string
	Sha256    *string
	Status    *string
	StatusIn  []string
	// OrphanedBeforeTs filters PENDING_DELETE blob whose grace period may have
	// elapsed: orphaned_ts <= value (unix seconds).
	OrphanedBeforeTs *int64
	// IncludeContent selects the byte column (DATABASE storage); listings that
	// do not need bytes leave it false.
	IncludeContent bool
	Limit         *int
}

// FindBlobEvent selects audit events.
type FindBlobEvent struct {
	BlobID *int64
	Type   *string
	Limit  *int
}

// FindBlobOrphan selects registered orphan objects.
type FindBlobOrphan struct {
	ID     *int64
	Status *string
	Reason *string
	Limit  *int
}

// GetAttachmentBlob loads the blob backing a content-addressed
// attachment, including its inline bytes for DATABASE storage.
func (s *Store) GetAttachmentBlob(ctx context.Context, attachment *Attachment) (*Blob, error) {
	if attachment == nil || attachment.BlobID == nil {
		return nil, nil
	}
	return s.GetBlob(ctx, &FindBlob{ID: attachment.BlobID, IncludeContent: true})
}

// ListBlobs lists blob rows matching find.
func (s *Store) ListBlobs(ctx context.Context, find *FindBlob) ([]*Blob, error) {
	return s.driver.ListBlobs(ctx, find)
}

// GetBlob returns the first matching blob, or nil when none exists.
func (s *Store) GetBlob(ctx context.Context, find *FindBlob) (*Blob, error) {
	blobs, err := s.ListBlobs(ctx, find)
	if err != nil || len(blobs) == 0 {
		return nil, err
	}
	return blobs[0], nil
}

// ListBlobEvents lists blob audit events matching find.
func (s *Store) ListBlobEvents(ctx context.Context, find *FindBlobEvent) ([]*BlobEvent, error) {
	return s.driver.ListBlobEvents(ctx, find)
}

// ListBlobOrphans lists registered orphan objects matching find.
func (s *Store) ListBlobOrphans(ctx context.Context, find *FindBlobOrphan) ([]*BlobOrphan, error) {
	return s.driver.ListBlobOrphans(ctx, find)
}

// CountAttachmentRefsByBlobs returns how many attachment rows reference each of
// the given blob ids. It is the authoritative reconciliation used by tests and
// the garbage collector.
func (s *Store) CountAttachmentRefsByBlobs(ctx context.Context, blobIDs []int64) (map[int64]int32, error) {
	counts := make(map[int64]int32, len(blobIDs))
	if len(blobIDs) == 0 {
		return counts, nil
	}
	return s.driver.CountAttachmentRefsByBlobs(ctx, blobIDs)
}
