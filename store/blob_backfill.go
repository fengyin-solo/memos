package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"os"

	"github.com/pkg/errors"

	storepb "github.com/usememos/memos/proto/gen/store"
)

// BlobBackfillStats summarizes one legacy attachment backfill pass.
type BlobBackfillStats struct {
	Scanned    int `json:"scanned"`
	Done       int `json:"done"`
	Failed     int `json:"failed"`
	Skipped    int `json:"skipped"`
	Duplicates int `json:"duplicates"`
}

// RunAttachmentBackfill binds every legacy managed attachment to a
// content-addressed blob row, hashing the physical object. It is
// idempotent and safe to re-run; rows marked FAILED are retried.
func (s *Store) RunAttachmentBackfill(ctx context.Context, batchSize int) (*BlobBackfillStats, error) {
	if batchSize <= 0 {
		batchSize = defaultGCBatchSize
	}
	stats := &BlobBackfillStats{}
	for {
		attachments, err := s.driver.ListBackfillAttachments(ctx, batchSize)
		if err != nil {
			return stats, errors.Wrap(err, "failed to list attachments pending backfill")
		}
		if len(attachments) == 0 {
			return stats, nil
		}
		for _, attachment := range attachments {
			stats.Scanned++
			if err := s.backfillOneAttachment(ctx, attachment, stats); err != nil {
				slog.Warn("attachment backfill failed", slog.String("uid", attachment.UID), slog.Any("error", err))
			}
		}
		if len(attachments) < batchSize {
			return stats, nil
		}
	}
}

func (s *Store) backfillOneAttachment(ctx context.Context, attachment *Attachment, stats *BlobBackfillStats) error {
	if attachment.StorageType == storepb.AttachmentStorageType_EXTERNAL {
		if err := s.driver.MarkAttachmentBackfillStatus(ctx, attachment.ID, AttachmentBlobBackfillSkipped); err != nil {
			return err
		}
		stats.Skipped++
		return nil
	}
	acquire, err := s.buildBackfillAcquire(ctx, attachment)
	if err != nil {
		_ = s.driver.MarkAttachmentBackfillStatus(ctx, attachment.ID, AttachmentBlobBackfillFailed)
		stats.Failed++
		return err
	}
	duplicate, err := s.driver.BackfillAttachmentBlob(ctx, attachment, acquire, acquire.Location)
	if err != nil {
		_ = s.driver.MarkAttachmentBackfillStatus(ctx, attachment.ID, AttachmentBlobBackfillFailed)
		stats.Failed++
		return errors.Wrap(err, "failed to bind blob")
	}
	stats.Done++
	if duplicate {
		stats.Duplicates++
	}
	return nil
}

// buildBackfillAcquire reads and hashes the legacy object and returns
// the blob acquire descriptor. Location points at the object's existing
// physical location (no bytes are moved).
func (s *Store) buildBackfillAcquire(ctx context.Context, attachment *Attachment) (*BlobAcquire, error) {
	switch attachment.StorageType {
	case storepb.AttachmentStorageType_LOCAL:
		return s.backfillLocalBlob(attachment)
	case storepb.AttachmentStorageType_S3:
		return s.backfillS3Blob(ctx, attachment)
	default:
		return s.backfillDatabaseBlob(attachment)
	}
}

func (s *Store) backfillLocalBlob(attachment *Attachment) (*BlobAcquire, error) {
	path := s.localObjectPath(attachment.Reference)
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open legacy local attachment")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat legacy local attachment")
	}
	if attachment.Size > 0 && info.Size() != attachment.Size {
		return nil, errors.Errorf("legacy local attachment size mismatch: %d != %d", info.Size(), attachment.Size)
	}
	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return nil, errors.Wrap(err, "failed to hash legacy local attachment")
	}
	return &BlobAcquire{
		StorageID: BlobStorageLocalID,
		Sha256:    hex.EncodeToString(hasher.Sum(nil)),
		Size:      size,
		Location: attachment.Reference,
	}, nil
}

func (s *Store) backfillS3Blob(ctx context.Context, attachment *Attachment) (*BlobAcquire, error) {
	driver, s3Object, err := s.ResolveAttachmentS3Driver(ctx, attachment)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve legacy s3 attachment driver")
	}
	data, err := driver.GetObject(ctx, s3Object.Key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read legacy s3 attachment")
	}
	if attachment.Size > 0 && int64(len(data)) != attachment.Size {
		return nil, errors.Errorf("legacy s3 attachment size mismatch: %d != %d", len(data), attachment.Size)
	}
	sum := sha256.Sum256(data)
	storageID := s3Object.StorageId
	if storageID == "" {
		storageID = s.resolvedS3StorageID(ctx, attachment)
	}
	return &BlobAcquire{
		StorageID: storageID,
		Sha256:    hex.EncodeToString(sum[:]),
		Size:      int64(len(data)),
		Location: s3Object.Key,
	}, nil
}

func (s *Store) resolvedS3StorageID(ctx context.Context, attachment *Attachment) string {
	setting, err := s.GetInstanceStorageSetting(ctx)
	if err != nil {
		return ""
	}
	s3Object := attachment.Payload.GetS3Object()
	resolved, err := ResolveStorage(setting, "", s3Object.GetS3Config())
	if err != nil || resolved == nil {
		return ""
	}
	return resolved.Id
}

func (s *Store) backfillDatabaseBlob(attachment *Attachment) (*BlobAcquire, error) {
	data := attachment.Blob
	if len(data) == 0 {
		return nil, errors.New("legacy database attachment has no bytes")
	}
	if attachment.Size > 0 && int64(len(data)) != attachment.Size {
		return nil, errors.Errorf("legacy database attachment size mismatch: %d != %d", len(data), attachment.Size)
	}
	sum := sha256.Sum256(data)
	return &BlobAcquire{
		StorageID: BlobStorageDatabaseID,
		Sha256:    hex.EncodeToString(sum[:]),
		Size:      int64(len(data)),
		Content:   bytes.Clone(data),
	}, nil
}
