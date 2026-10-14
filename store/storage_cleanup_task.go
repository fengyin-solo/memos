package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	storepb "github.com/usememos/memos/proto/gen/store"
)

// StorageCleanupSource names the business deletion that enqueued a cleanup
// task. It is the reconciliation grouping key.
type StorageCleanupSource string

const (
	// StorageCleanupSourceAttachment is a direct attachment deletion or an
	// attachment removed while editing a memo.
	StorageCleanupSourceAttachment StorageCleanupSource = "ATTACHMENT"
	// StorageCleanupSourceMemo is a memo deletion.
	StorageCleanupSourceMemo StorageCleanupSource = "MEMO"
	// StorageCleanupSourceSpace is a Space deletion.
	StorageCleanupSourceSpace StorageCleanupSource = "SPACE"
	// StorageCleanupSourceUser is a user deletion whose transaction cascades
	// to the user's memos and attachments.
	StorageCleanupSourceUser StorageCleanupSource = "USER"
)

// StorageCleanupStatus is the processing state of a cleanup task.
type StorageCleanupStatus string

const (
	// StorageCleanupStatusPending waits for a worker claim; next_run_ts gates
	// the first and every retry attempt.
	StorageCleanupStatusPending StorageCleanupStatus = "PENDING"
	// StorageCleanupStatusProcessing is claimed by leased_by until
	// lease_expires_ts; an expired lease makes the row reclaimable.
	StorageCleanupStatusProcessing StorageCleanupStatus = "PROCESSING"
	// StorageCleanupStatusSucceeded is the terminal state after the object
	// was deleted.
	StorageCleanupStatusSucceeded StorageCleanupStatus = "SUCCEEDED"
	// StorageCleanupStatusFailed is the terminal state after an unrecoverable
	// deletion error.
	StorageCleanupStatusFailed StorageCleanupStatus = "FAILED"
)

// StorageCleanupTask is one external object awaiting or undergoing deletion.
type StorageCleanupTask struct {
	ID int64

	// Source is the business deletion that enqueued the task.
	Source StorageCleanupSource
	// StorageType is LOCAL or S3. DATABASE objects are never enqueued.
	StorageType storepb.AttachmentStorageType
	// StorageID is the named storage id for S3 resolution; empty for local
	// objects and legacy S3 attachments that carry their config in Payload.
	StorageID string
	// KeyHash is the sha256 hex of storage type, storage id and key, giving
	// every driver a short stable unique index independent of key length.
	KeyHash string
	// StorageKey is the local reference path or the S3 object key.
	StorageKey string
	// Payload is the attachment payload JSON, retaining legacy embedded S3
	// configuration so resolution still works if the registry changed.
	Payload string
	// AttachmentUID is the deleted attachment UID, kept for traceability and
	// derived cache cleanup.
	AttachmentUID string

	Status         StorageCleanupStatus
	Attempts       int32
	LeasedBy       string
	CreatedTs      int64
	UpdatedTs      int64
	NextRunTs      int64
	LeaseExpiresTs int64
	CompletedTs    int64
	LastError      string
}

// StorageCleanupTaskUpdate is the conditional result write-back a claimed task
// owner performs after processing. Only the row still held by WorkerID in
// PROCESSING state may be updated.
type StorageCleanupTaskUpdate struct {
	ID       int64
	WorkerID string
	// Status must be SUCCEEDED, FAILED or PENDING (retry).
	Status StorageCleanupStatus
	// NextRunTs is required for PENDING retries and ignored for terminals.
	NextRunTs int64
	// LastError is the truncated latest failure message; empty on success.
	LastError string
	// Now stamps updated_ts and completed_ts for terminal states.
	Now int64
}

// StorageCleanupSourceStats is one (source, status) count row.
type StorageCleanupSourceStats struct {
	Source StorageCleanupSource
	Status StorageCleanupStatus
	Count  int64
}

// StorageObjectRef identifies one object currently referenced by a live
// attachment or by a cleanup task. StorageType/StorageID/StorageKey match
// StorageCleanupTask fields.
type StorageObjectRef struct {
	StorageType storepb.AttachmentStorageType
	StorageID   string
	StorageKey  string
}

// LocalStorageID is the synthetic storage id under which local filesystem
// objects are grouped in task rows and reconciliation reports.
const LocalStorageID = "local"

// storageCleanupMaxErrorLen bounds last_error so a verbose storage response
// cannot grow the table without limit.
const storageCleanupMaxErrorLen = 1024

var storageCleanupPayloadMarshal = protojson.MarshalOptions{UseEnumNumbers: false}

// BuildStorageCleanupTasks converts attachment snapshots taken inside a
// delete transaction into cleanup tasks. LOCAL and S3 tasks delete the
// external object and the derived local caches; DATABASE blob attachments
// produce a cache-only task keyed by their UID so generated thumbnail and
// motion caches are not orphaned. Duplicate keys within the batch collapse to one task.
func BuildStorageCleanupTasks(attachments []*Attachment, source StorageCleanupSource) []*StorageCleanupTask {
	tasks := make([]*StorageCleanupTask, 0, len(attachments))
	seen := make(map[string]struct{}, len(attachments))
	now := time.Now().Unix()
	for _, attachment := range attachments {
		task, err := buildStorageCleanupTask(attachment, source, now)
		if err != nil || task == nil {
			continue
		}
		if _, exists := seen[task.KeyHash]; exists {
			continue
		}
		seen[task.KeyHash] = struct{}{}
		tasks = append(tasks, task)
	}
	return tasks
}

func buildStorageCleanupTask(attachment *Attachment, source StorageCleanupSource, now int64) (*StorageCleanupTask, error) {
	if attachment == nil {
		return nil, nil
	}
	task := &StorageCleanupTask{
		Source:        source,
		AttachmentUID: attachment.UID,
		Status:        StorageCleanupStatusPending,
		CreatedTs:     now,
		UpdatedTs:     now,
	}
	switch attachment.StorageType {
	case storepb.AttachmentStorageType_LOCAL:
		if strings.TrimSpace(attachment.Reference) == "" {
			return nil, nil
		}
		task.StorageType = storepb.AttachmentStorageType_LOCAL
		task.StorageID = LocalStorageID
		task.StorageKey = attachment.Reference
	case storepb.AttachmentStorageType_S3:
		s3Object := attachment.Payload.GetS3Object()
		if s3Object == nil || strings.TrimSpace(s3Object.Key) == "" {
			return nil, nil
		}
		payload, err := storageCleanupPayloadMarshal.Marshal(attachment.Payload)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal attachment payload")
		}
		task.StorageType = storepb.AttachmentStorageType_S3
		task.StorageID = s3Object.StorageId
		task.StorageKey = s3Object.Key
		task.Payload = string(payload)
	default:
		// Database blobs have no external object, but their locally cached
		// derivatives still need to be removed. The attachment UID is the
		// unique key for this cache-only task.
		if attachment.UID == "" {
			return nil, nil
		}
		task.StorageType = storepb.AttachmentStorageType_ATTACHMENT_STORAGE_TYPE_UNSPECIFIED
		task.StorageKey = attachment.UID
		task.Payload = "{}"
	}
	task.KeyHash = StorageCleanupKeyHash(task.StorageType, task.StorageID, task.StorageKey)
	return task, nil
}

// StorageCleanupKeyHash returns the stable identity of an object across
// task rows, live attachment refs and storage listings.
func StorageCleanupKeyHash(storageType storepb.AttachmentStorageType, storageID, key string) string {
	sum := sha256.Sum256([]byte(storageType.String() + "\x00" + storageID + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

// TruncateStorageCleanupError caps a worker error message for last_error.
func TruncateStorageCleanupError(message string) string {
	if len(message) > storageCleanupMaxErrorLen {
		return message[:storageCleanupMaxErrorLen]
	}
	return message
}

// InsertStorageCleanupTasks enqueues cleanup tasks outside a business delete
// transaction (used by store facade deletion paths). Duplicate objects are
// ignored silently.
func (s *Store) InsertStorageCleanupTasks(ctx context.Context, tasks []*StorageCleanupTask) error {
	if len(tasks) == 0 {
		return nil
	}
	return s.driver.InsertStorageCleanupTasks(ctx, tasks)
}

// ClaimStorageCleanupTasks atomically claims due tasks: PENDING rows whose
// next_run_ts is due and PROCESSING rows whose lease has expired. Each row is
// claimed by exactly one caller; the returned tasks carry the new lease.
func (s *Store) ClaimStorageCleanupTasks(ctx context.Context, nowTs, leaseExpiresTs int64, workerID string, batchSize int) ([]*StorageCleanupTask, error) {
	if workerID == "" {
		return nil, errors.New("worker id is required")
	}
	if batchSize <= 0 {
		return nil, errors.New("batch size must be positive")
	}
	return s.driver.ClaimStorageCleanupTasks(ctx, nowTs, leaseExpiresTs, workerID, batchSize)
}

// FinishStorageCleanupTask writes the processing result of a claimed task.
// It returns false when the caller no longer owns the lease (e.g. it expired
// and another worker reclaimed the row), in which case nothing is written.
func (s *Store) FinishStorageCleanupTask(ctx context.Context, update *StorageCleanupTaskUpdate) (bool, error) {
	if update == nil || update.ID <= 0 || update.WorkerID == "" {
		return false, errors.New("storage cleanup task finish requires id and worker id")
	}
	switch update.Status {
	case StorageCleanupStatusSucceeded, StorageCleanupStatusFailed, StorageCleanupStatusPending:
	default:
		return false, errors.Errorf("unsupported cleanup task result status %q", update.Status)
	}
	return s.driver.FinishStorageCleanupTask(ctx, update)
}

// GetStorageCleanupStats returns task counts grouped by source and status.
func (s *Store) GetStorageCleanupStats(ctx context.Context) ([]*StorageCleanupSourceStats, error) {
	return s.driver.GetStorageCleanupStats(ctx)
}

// ListStorageCleanupTasks returns tasks with ids strictly greater than afterID,
// ordered by id and capped at limit.
func (s *Store) ListStorageCleanupTasks(ctx context.Context, afterID int64, limit int) ([]*StorageCleanupTask, error) {
	if limit <= 0 {
		limit = 1000
	}
	return s.driver.ListStorageCleanupTasks(ctx, afterID, limit)
}

// ListAttachmentStorageRefs lists the external object keys referenced by
// live attachments, keyset-paginated inside the driver. DATABASE attachments
// are excluded because they have no external object.
func (s *Store) ListAttachmentStorageRefs(ctx context.Context) ([]*StorageObjectRef, error) {
	return s.driver.ListAttachmentStorageRefs(ctx)
}
