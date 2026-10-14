package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	providerstorage "github.com/usememos/memos/provider/storage"
	storepb "github.com/usememos/memos/proto/gen/store"
)

const defaultGCBatchSize = 100

// Skip reasons reported in GCReport.Skipped.
const (
	GCSkipDryRun           = "dry_run"
	GCSkipResurrected      = "resurrected"
	GCSkipStorageMissing   = "storage_unavailable"
	GCSkipVerifyError      = "verify_error"
	GCSkipReferenced       = "referenced"
	GCSkipTooYoung         = "too_young"
	GCSkipDeleteFailed     = "delete_failed"
)

// GCOptions controls one blob garbage collection run.
type GCOptions struct {
	Now            time.Time
	DryRun         bool
	GracePeriod    time.Duration
	OrphanSafetyAge time.Duration
	BatchSize      int
	// SweepAbandonedObjects enables the prefix scan for objects with no
	// blob row (LOCAL .blobs folder and configured S3 blobs/ prefixes).
	SweepAbandonedObjects bool
}

// GCReport summarizes one GC run.
type GCReport struct {
	RunID  string `json:"run_id"`
	DryRun bool   `json:"dry_run"`

	ScannedBlobs  int64 `json:"scanned_blobs"`
	DueCandidates int64 `json:"due_candidates"`
	DeletedBlobs  int64 `json:"deleted_blobs"`
	CorruptBlobs  int64 `json:"corrupt_blobs"`
	MissingBlobs  int64 `json:"missing_blobs"`
	Skipped       map[string]int64 `json:"skipped"`

	OrphansScanned int64 `json:"orphans_scanned"`
	OrphansDeleted int64 `json:"orphans_deleted"`

	AbandonedScanned int64 `json:"abandoned_scanned"`
	AbandonedDeleted int64 `json:"abandoned_deleted"`
}

func (o GCOptions) batchSize() int {
	if o.BatchSize <= 0 {
		return defaultGCBatchSize
	}
	return o.BatchSize
}

// RunBlobGC reclaims unreferenced blob objects that outlived the grace
// period, registered orphan objects, and (optionally) abandoned objects
// with no blob row.
func (s *Store) RunBlobGC(ctx context.Context, opts GCOptions) (*GCReport, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	report := &GCReport{RunID: fmt.Sprintf("gc-%d", now.UnixNano()), DryRun: opts.DryRun, Skipped: map[string]int64{}}
	graceCutoff := now.Add(-opts.GracePeriod).Unix()
	safetyCutoff := now.Add(-opts.OrphanSafetyAge).Unix()

	if err := s.collectDueBlobs(ctx, opts, graceCutoff, report); err != nil {
		return report, err
	}
	if err := s.collectRegisteredOrphans(ctx, opts, safetyCutoff, report); err != nil {
		return report, err
	}
	if opts.SweepAbandonedObjects {
		if err := s.sweepAbandonedObjects(ctx, opts, safetyCutoff, report); err != nil {
			return report, err
		}
	}
	slog.Info("blob garbage collection finished",
		slog.String("run_id", report.RunID),
		slog.Bool("dry_run", report.DryRun),
		slog.Int64("deleted_blobs", report.DeletedBlobs),
		slog.Int64("corrupt_blobs", report.CorruptBlobs),
		slog.Int64("orphans_deleted", report.OrphansDeleted),
		slog.Int64("abandoned_deleted", report.AbandonedDeleted),
	)
	return report, nil
}

func (s *Store) collectDueBlobs(ctx context.Context, opts GCOptions, cutoff int64, report *GCReport) error {
	for {
		blobs, err := s.driver.ListBlobs(ctx, &FindBlob{
			StatusIn:         []string{BlobStatusPendingDelete, BlobStatusDeleting},
			OrphanedBeforeTs: &cutoff,
			Limit:            intPtr(opts.batchSize()),
		})
		if err != nil {
			return errors.Wrap(err, "failed to list due blobs")
		}
		if len(blobs) == 0 {
			return nil
		}
		for _, blob := range blobs {
			report.ScannedBlobs++
			if err := s.processDueBlob(ctx, opts, blob, report); err != nil {
				return err
			}
		}
		if len(blobs) < opts.batchSize() {
			return nil
		}
	}
}

func (s *Store) processDueBlob(ctx context.Context, opts GCOptions, listed *Blob, report *GCReport) error {
	claimed, err := s.driver.ClaimDueBlob(ctx, listed.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to claim blob %d", listed.ID)
	}
	if claimed == nil {
		report.Skipped[GCSkipResurrected]++
		return nil
	}
	report.DueCandidates++
	if opts.DryRun {
		report.Skipped[GCSkipDryRun]++
		return nil
	}

	exists, reader, err := s.openBlobObjectForVerify(ctx, claimed)
	if err != nil {
		report.Skipped[GCSkipStorageMissing]++
		slog.Warn("blob object unavailable; leaving it pending", slog.Int64("blob_id", claimed.ID), slog.Any("error", err))
		return nil
	}
	if !exists {
		// Object already gone: finalize the row idempotently.
		if err := s.driver.FinishBlobDelete(ctx, claimed); err != nil {
			return errors.Wrap(err, "failed to finalize missing blob")
		}
		report.MissingBlobs++
		return nil
	}
	corrupt, detail, err := verifyBlobObject(reader, claimed)
	closeErr := reader.Close()
	if err != nil {
		report.Skipped[GCSkipVerifyError]++
		slog.Warn("failed to verify blob", slog.Int64("blob_id", claimed.ID), slog.Any("error", err))
		return nil
	}
	if closeErr != nil {
		slog.Warn("failed to close blob stream", slog.Int64("blob_id", claimed.ID), slog.Any("error", closeErr))
	}
	if corrupt {
		if err := s.driver.MarkBlobCorrupt(ctx, claimed, detail); err != nil {
			return errors.Wrap(err, "failed to mark corrupt blob")
		}
		report.CorruptBlobs++
		return nil
	}

	if err := s.deleteBlobPhysical(ctx, claimed); err != nil {
		report.Skipped[GCSkipDeleteFailed]++
		slog.Warn("failed to delete blob object", slog.Int64("blob_id", claimed.ID), slog.Any("error", err))
		return nil
	}
	if err := s.driver.FinishBlobDelete(ctx, claimed); err != nil {
		return errors.Wrap(err, "failed to finish blob delete")
	}
	report.DeletedBlobs++
	return nil
}

// openBlobObjectForVerify opens the physical object for hash verification.
// exists=false means the object is already gone.
func (s *Store) openBlobObjectForVerify(ctx context.Context, blob *Blob) (bool, io.ReadCloser, error) {
	switch blob.StorageID {
	case BlobStorageLocalID:
		path := s.localObjectPath(blob.Location)
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil, nil
			}
			return false, nil, errors.Wrap(err, "failed to open local blob")
		}
		return true, file, nil
	case BlobStorageDatabaseID:
		if blob.Content == nil {
			full, err := s.GetBlob(ctx, &FindBlob{ID: &blob.ID, IncludeContent: true})
			if err != nil || full == nil {
				return false, nil, errors.Wrap(err, "failed to load database blob content")
			}
			blob.Content = full.Content
		}
		if len(blob.Content) == 0 {
			return false, nil, nil
		}
		return true, io.NopCloser(bytes.NewReader(blob.Content)), nil
	default:
		driver, err := s.blobS3Driver(ctx, blob)
		if err != nil {
			return false, nil, err
		}
		exists, _, err := driver.ObjectExists(ctx, blob.Location)
		if err != nil {
			return false, nil, errors.Wrap(err, "failed to head s3 blob")
		}
		if !exists {
			return false, nil, nil
		}
		stream, err := driver.GetObjectStream(ctx, blob.Location, "")
		if err != nil {
			return false, nil, errors.Wrap(err, "failed to stream s3 blob")
		}
		return true, stream.Body, nil
	}
}

// blobS3Driver resolves the driver for an S3-backed blob, using a
// backing attachment's legacy payload when the blob row carries no
// storage id (pre-registry embedded configuration).
func (s *Store) blobS3Driver(ctx context.Context, blob *Blob) (providerstorage.Driver, error) {
	if blob.StorageID != "" {
		return s.StorageDriverByID(ctx, blob.StorageID)
	}
	setting, err := s.GetInstanceStorageSetting(ctx)
	if err != nil {
		return nil, err
	}
	legacyConfig, err := s.legacyS3ConfigForBlob(ctx, blob.ID)
	if err != nil {
		return nil, err
	}
	resolved, err := ResolveStorage(setting, "", legacyConfig)
	if err != nil {
		return nil, err
	}
	return s.StorageDriver(ctx, resolved)
}

// legacyS3ConfigForBlob finds the embedded S3 configuration of a
// pre-registry attachment sharing the blob.
func (s *Store) legacyS3ConfigForBlob(ctx context.Context, blobID int64) (*storepb.StorageS3Config, error) {
	list, err := s.driver.ListAttachments(ctx, &FindAttachment{BlobID: &blobID, SkipDefaultLimit: true, Limit: intPtr(50)})
	if err != nil {
		return nil, errors.Wrap(err, "failed to load backing attachments")
	}
	for _, attachment := range list {
		if s3Object := attachment.Payload.GetS3Object(); s3Object != nil && s3Object.S3Config != nil {
			return s3Object.S3Config, nil
		}
	}
	return nil, errors.New("legacy s3 configuration not found for blob")
}

func (s *Store) deleteBlobPhysical(ctx context.Context, blob *Blob) error {
	switch blob.StorageID {
	case BlobStorageLocalID:
		if err := os.Remove(s.localObjectPath(blob.Location)); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "failed to remove local blob object")
		}
		return nil
	case BlobStorageDatabaseID:
		return nil
	default:
		driver, err := s.blobS3Driver(ctx, blob)
		if err != nil {
			return err
		}
		return driver.DeleteObject(ctx, blob.Location)
	}
}

func (s *Store) localObjectPath(location string) string {
	p := filepath.FromSlash(location)
	if !filepath.IsAbs(p) {
		p = filepath.Join(s.profile.Data, p)
	}
	return p
}

func verifyBlobObject(reader io.Reader, blob *Blob) (bool, string, error) {
	hasher := sha256.New()
	size, err := io.Copy(hasher, reader)
	if err != nil {
		return false, "", errors.Wrap(err, "failed to hash blob object")
	}
	sha := hex.EncodeToString(hasher.Sum(nil))
	detail := fmt.Sprintf(`{"expected_sha256":%q,"actual_sha256":%q,"expected_size":%d,"actual_size":%d}`,
		blob.Sha256, sha, blob.Size, size)
	if size != blob.Size || sha != blob.Sha256 {
		return true, detail, nil
	}
	return false, "", nil
}

func (s *Store) collectRegisteredOrphans(ctx context.Context, opts GCOptions, cutoff int64, report *GCReport) error {
	for {
		orphans, err := s.driver.ListDueBlobOrphans(ctx, cutoff, opts.batchSize())
		if err != nil {
			return errors.Wrap(err, "failed to list due orphans")
		}
		if len(orphans) == 0 {
			return nil
		}
		for _, orphan := range orphans {
			report.OrphansScanned++
			if err := s.processOrphan(ctx, opts, orphan, report); err != nil {
				return err
			}
		}
		if len(orphans) < opts.batchSize() {
			return nil
		}
	}
}

func (s *Store) processOrphan(ctx context.Context, opts GCOptions, orphan *BlobOrphan, report *GCReport) error {
	storageID := orphan.StorageID
	if storageID == "" {
		storageID = s.guessLegacyOrphanStorageID(orphan.StorageType)
	}
	referenced, err := s.driver.BlobLocationReferenced(ctx, storageID, orphan.Location)
	if err != nil {
		return errors.Wrap(err, "failed to verify orphan references")
	}
	if !referenced {
		referenced, err = s.driver.LegacyAttachmentAtLocation(ctx, orphan.StorageType, orphan.Location)
		if err != nil {
			return errors.Wrap(err, "failed to verify legacy attachment references")
		}
	}
	if referenced {
		report.Skipped[GCSkipReferenced]++
		return nil
	}
	if opts.DryRun {
		report.Skipped[GCSkipDryRun]++
		return nil
	}
	missing, err := s.deleteOrphanObject(ctx, orphan)
	if err != nil {
		report.Skipped[GCSkipDeleteFailed]++
		slog.Warn("failed to delete orphan object", slog.Int64("orphan_id", orphan.ID), slog.Any("error", err))
		return nil
	}
	if err := s.driver.DeleteBlobOrphan(ctx, orphan.ID); err != nil {
		return errors.Wrap(err, "failed to remove orphan row")
	}
	event := &BlobEvent{
		StorageID: storageID,
		Sha256:   "",
		Type:     BlobEventOrphanDelete,
		Detail: fmtJSON(map[string]any{
			"reason": orphan.Reason, "location": orphan.Location, "missing": missing,
		}),
	}
	if err := s.driver.AppendBlobEvent(ctx, event); err != nil {
		slog.Warn("failed to append orphan delete event", slog.Any("error", err))
	}
	report.OrphansDeleted++
	return nil
}

func (s *Store) guessLegacyOrphanStorageID(storageType string) string {
	switch storageType {
	case storepb.AttachmentStorageType_LOCAL.String():
		return BlobStorageLocalID
	default:
		return ""
	}
}

func (s *Store) deleteOrphanObject(ctx context.Context, orphan *BlobOrphan) (missing bool, err error) {
	switch orphan.StorageType {
	case storepb.AttachmentStorageType_LOCAL.String():
		path := s.localObjectPath(orphan.Location)
		if removeErr := os.Remove(path); removeErr != nil {
			if os.IsNotExist(removeErr) {
				return true, nil
			}
			return false, errors.Wrap(removeErr, "failed to remove local orphan")
		}
		return false, nil
	case storepb.AttachmentStorageType_S3.String():
		driver, err := s.orphanS3Driver(ctx, orphan)
		if err != nil {
			return false, err
		}
		exists, _, err := driver.ObjectExists(ctx, orphan.Location)
		if err != nil {
			return false, errors.Wrap(err, "failed to head orphan s3 object")
		}
		if !exists {
			return true, nil
		}
		return false, driver.DeleteObject(ctx, orphan.Location)
	default:
		return true, nil
	}
}

func (s *Store) orphanS3Driver(ctx context.Context, orphan *BlobOrphan) (providerstorage.Driver, error) {
	if orphan.StorageID != "" {
		return s.StorageDriverByID(ctx, orphan.StorageID)
	}
	setting, err := s.GetInstanceStorageSetting(ctx)
	if err != nil {
		return nil, err
	}
	payload := &storepb.AttachmentPayload{}
	if orphan.Payload != "" {
		if err := protojson.Unmarshal([]byte(orphan.Payload), payload); err != nil {
			return nil, errors.Wrap(err, "failed to parse orphan payload")
		}
	}
	var legacyConfig *storepb.StorageS3Config
	if s3Object := payload.GetS3Object(); s3Object != nil {
		legacyConfig = s3Object.S3Config
	}
	resolved, err := ResolveStorage(setting, "", legacyConfig)
	if err != nil {
		return nil, err
	}
	return s.StorageDriver(ctx, resolved)
}

func (s *Store) sweepAbandonedObjects(ctx context.Context, opts GCOptions, cutoff int64, report *GCReport) error {
	if err := s.sweepLocalAbandoned(ctx, opts, cutoff, report); err != nil {
		return err
	}
	return s.sweepS3Abandoned(ctx, opts, cutoff, report)
}

func (s *Store) sweepLocalAbandoned(ctx context.Context, opts GCOptions, cutoff int64, report *GCReport) error {
	root := filepath.Join(s.profile.Data, BlobLocalDirName)
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !isFinalBlobName(info.Name()) {
			return nil
		}
		if info.ModTime().Unix() > cutoff {
			report.Skipped[GCSkipTooYoung]++
			return nil
		}
		rel, err := filepath.Rel(s.profile.Data, path)
		if err != nil {
			return err
		}
		location := filepath.ToSlash(rel)
		report.AbandonedScanned++
		referenced, err := s.driver.BlobLocationReferenced(ctx, BlobStorageLocalID, location)
		if err != nil {
			return err
		}
		if referenced {
			return nil
		}
		if opts.DryRun {
			report.Skipped[GCSkipDryRun]++
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			report.Skipped[GCSkipDeleteFailed]++
			slog.Warn("failed to remove abandoned local blob", slog.String("path", path), slog.Any("error", err))
			return nil
		}
		report.AbandonedDeleted++
		return s.driver.AppendBlobEvent(ctx, &BlobEvent{
			StorageID: BlobStorageLocalID,
			Type:     BlobEventOrphanDelete,
			Detail:   fmtJSON(map[string]any{"location": location, "reason": BlobOrphanAbandonedUpload}),
		})
	})
}

func (s *Store) sweepS3Abandoned(ctx context.Context, opts GCOptions, cutoff int64, report *GCReport) error {
	setting, err := s.GetInstanceStorageSetting(ctx)
	if err != nil {
		return err
	}
	for _, configured := range setting.Storages {
		if configured.Type != storepb.StorageType_STORAGE_TYPE_S3 {
			continue
		}
		driver, err := s.StorageDriver(ctx, configured)
		if err != nil {
			slog.Warn("skipping unreachable storage during orphan sweep", slog.String("storage", configured.Id), slog.Any("error", err))
			report.Skipped[GCSkipStorageMissing]++
			continue
		}
		objects, err := driver.ListObjects(ctx, BlobS3KeyPrefix)
		if err != nil {
			slog.Warn("failed to list objects during orphan sweep", slog.String("storage", configured.Id), slog.Any("error", err))
			report.Skipped[GCSkipStorageMissing]++
			continue
		}
		for _, object := range objects {
			if !isFinalBlobName(filepath.Base(object.Key)) {
				continue
			}
			report.AbandonedScanned++
		if object.LastModified.Unix() > cutoff {
				report.Skipped[GCSkipTooYoung]++
				continue
			}
			referenced, err := s.driver.BlobLocationReferenced(ctx, configured.Id, object.Key)
			if err != nil {
				return err
			}
			if referenced {
				continue
			}
			if opts.DryRun {
				report.Skipped[GCSkipDryRun]++
				continue
			}
			if err := driver.DeleteObject(ctx, object.Key); err != nil {
				report.Skipped[GCSkipDeleteFailed]++
				slog.Warn("failed to delete abandoned s3 blob", slog.String("key", object.Key), slog.Any("error", err))
				continue
			}
			report.AbandonedDeleted++
			_ = s.driver.AppendBlobEvent(ctx, &BlobEvent{
				StorageID: configured.Id,
				Type:     BlobEventOrphanDelete,
				Detail:   fmtJSON(map[string]any{"location": object.Key, "reason": BlobOrphanAbandonedUpload}),
			})
		}
	}
	return nil
}

func isFinalBlobName(name string) bool {
	if len(name) != 64 {
		return false
	}
	return strings.IndexFunc(name, func(r rune) bool {
		return !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f'))
	}) == -1
}

func intPtr(value int) *int { return &value }

func fmtJSON(values map[string]any) string {
	data, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(data)
}
