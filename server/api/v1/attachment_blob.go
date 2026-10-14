package v1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/usememos/memos/internal/profile"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

const (
	// blobLocalDirName is the data-directory subfolder holding
	// content-addressed local objects, sharded by the first digest bytes.
	blobLocalDirName = ".blobs"
	// blobS3KeyPrefix is the object key prefix for content-addressed S3 objects.
	blobS3KeyPrefix = "blobs/"
	// blobStagePrefix names non-final staging temp files inside the blob
	// folder; the orphan sweep only considers 64-hex-digit final names.
	blobStagePrefix = ".blob-stage-"
)

// hashAndRewind consumes reader to compute its SHA-256 and byte size, then
// rewinds seekable readers so the bytes can be uploaded again. Non-seekable
// readers are returned consumed along with their buffered content.
func hashAndRewind(reader io.Reader) (sha string, size int64, content []byte, err error) {
	if seeker, ok := reader.(io.Seeker); ok {
		hasher := sha256.New()
		if size, err = io.Copy(hasher, reader); err != nil {
			return "", 0, nil, errors.Wrap(err, "failed to hash attachment content")
		}
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return "", 0, nil, errors.Wrap(err, "failed to rewind attachment content")
		}
		sha = hex.EncodeToString(hasher.Sum(nil))
		return sha, size, nil, nil
	}
	if content, err = io.ReadAll(reader); err != nil {
		return "", 0, nil, errors.Wrap(err, "failed to read attachment content")
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), int64(len(content)), content, nil
}

// storeLocalBlob stages content at its content-addressed local path via a
// same-directory temp file and atomic rename.
func storeLocalBlob(ctx context.Context, dataDir string, content io.Reader, create *store.Attachment) (bool, error) {
	rootDir := filepath.Join(dataDir, blobLocalDirName)
	if err := os.MkdirAll(rootDir, 0o770); err != nil {
		return false, errors.Wrap(err, "failed to create blob directory")
	}
	file, err := os.CreateTemp(rootDir, blobStagePrefix+"*")
	if err != nil {
		return false, errors.Wrap(err, "failed to create blob staging file")
	}
	stageName := file.Name()
	hasher := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hasher), &attachmentContextReader{ctx: ctx, reader: content})
	if copyErr != nil {
		_ = file.Close()
		_ = os.Remove(stageName)
		return false, errors.Wrap(copyErr, "failed to stage blob content")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(stageName)
		return false, errors.Wrap(err, "failed to sync blob staging file")
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		_ = os.Remove(stageName)
		return false, errors.Wrap(err, "failed to set blob permissions")
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(stageName)
		return false, errors.Wrap(err, "failed to close blob staging file")
	}
	sha := hex.EncodeToString(hasher.Sum(nil))
	shardDir := filepath.Join(rootDir, sha[:2])
	if err := os.MkdirAll(shardDir, 0o770); err != nil {
		_ = os.Remove(stageName)
		return false, errors.Wrap(err, "failed to create blob shard directory")
	}
	target := filepath.Join(shardDir, sha)
	objectExisted := false
	if _, err := os.Stat(target); err == nil {
		// The identical object already materialized; keep the canonical
		// file and drop the staged copy.
		objectExisted = true
		if err := os.Remove(stageName); err != nil {
			return false, errors.Wrap(err, "failed to remove duplicate staged blob")
		}
	} else if os.IsNotExist(err) {
		if err := os.Rename(stageName, target); err != nil {
			return false, errors.Wrap(err, "failed to finalize blob")
		}
	} else {
		_ = os.Remove(stageName)
		return false, errors.Wrap(err, "failed to inspect target blob")
	}
	internalPath := filepath.ToSlash(filepath.Join(blobLocalDirName, sha[:2], sha))
	create.StorageType = storepb.AttachmentStorageType_LOCAL
	create.Reference = internalPath
	create.Blob = nil
	create.Size = size
	create.BlobAcquire = &store.BlobAcquire{
		StorageID: store.BlobStorageLocalID,
		Sha256:    sha,
		Size:      size,
		Location:  internalPath,
	}
	return objectExisted, nil
}

// storeS3Blob uploads content to the content-addressed S3 key, skipping the
// upload when an object with the same key and size already exists.
func storeS3Blob(ctx context.Context, s *store.Store, storageID string, fileType string, content io.Reader, create *store.Attachment) (bool, error) {
	driver, err := s.StorageDriverByID(ctx, storageID)
	if err != nil {
		return false, err
	}
	sha, size, buffered, err := hashAndRewind(content)
	if err != nil {
		return false, err
	}
	key := blobS3KeyPrefix + sha
	exists, existingSize, err := driver.ObjectExists(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "failed to inspect existing blob object")
	}
	objectExisted := false
	switch {
	case exists && existingSize == size:
		objectExisted = true
	case buffered != nil:
		if _, err := driver.UploadObject(ctx, key, fileType, bytes.NewReader(buffered)); err != nil {
			return false, errors.Wrap(err, "failed to upload via blob")
		}
	default:
		if _, err := driver.UploadObject(ctx, key, fileType, content); err != nil {
			return false, errors.Wrap(err, "failed to upload via blob")
		}
	}
	create.StorageType = storepb.AttachmentStorageType_S3
	create.Reference = ""
	create.Blob = nil
	create.Size = size
	create.BlobAcquire = &store.BlobAcquire{
		StorageID: storageID,
		Sha256:    sha,
		Size:      size,
		Location:  key,
	}
	return objectExisted, nil
}

// storeDatabaseBlob binds the bytes to a DATABASE storage blob row.
func storeDatabaseBlob(content io.Reader, create *store.Attachment) error {
	data, err := io.ReadAll(content)
	if err != nil {
		return errors.Wrap(err, "failed to read database blob content")
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	create.StorageType = storepb.AttachmentStorageType_ATTACHMENT_STORAGE_TYPE_UNSPECIFIED
	create.Reference = ""
	create.Blob = nil
	create.Size = int64(len(data))
	create.BlobAcquire = &store.BlobAcquire{
		StorageID: store.BlobStorageDatabaseID,
		Sha256:    sha,
		Size:      int64(len(data)),
		Content:   data,
	}
	return nil
}

// cleanupBlobUploadAfterCreateFailure removes a freshly materialized object only when
// neither the attachment row nor any committed blob row survived the failed
// database transaction and this upload itself created the object. Anything
// else is reclaimed later by the orphan sweep.
func cleanupBlobUploadAfterCreateFailure(ctx context.Context, profile *profile.Profile, s *store.Store, create *store.Attachment, objectExisted bool) {
	if create.BlobAcquire == nil || objectExisted {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	persisted, err := s.GetAttachment(cleanupCtx, &store.FindAttachment{UID: &create.UID})
	if err != nil {
		slog.Error("failed to verify attachment create outcome", slog.String("attachment_uid", create.UID), slog.Any("error", err))
		return
	}
	// A same-uid row only suppresses compensation when it references the
	// same content-addressed object (the post-commit failpoint case). A
	// row left over from an earlier, different upload means this
	// create failed and its fresh object still needs reclaiming.
	if persisted != nil && attachmentReferencesBlob(cleanupCtx, s, persisted, create.BlobAcquire) {
		slog.Warn("attachment persisted after create returned an error; skipping blob compensation", slog.String("attachment_uid", create.UID))
		return
	}
	existing, err := s.GetBlob(cleanupCtx, &store.FindBlob{
		StorageID: &create.BlobAcquire.StorageID,
		Sha256:   &create.BlobAcquire.Sha256,
	})
	if err != nil {
		slog.Error("failed to verify committed blob", slog.String("attachment_uid", create.UID), slog.Any("error", err))
		return
	}
	if existing != nil {
		// A committed identical blob owns the object.
		return
	}
	if err := deleteBlobObject(cleanupCtx, profile, s, create.BlobAcquire, create.StorageType); err != nil {
		slog.Error("failed to compensate abandoned blob object; the orphan sweep will reclaim it", slog.String("attachment_uid", create.UID), slog.Any("error", err))
	}
}

// attachmentReferencesBlob reports whether a persisted attachment points at
// the content-addressed object this failed upload tried to create.
func attachmentReferencesBlob(ctx context.Context, s *store.Store, attachment *store.Attachment, acquire *store.BlobAcquire) bool {
	if attachment == nil || attachment.BlobID == nil {
		return false
	}
	if attachment.StorageType == storepb.AttachmentStorageType_LOCAL {
		return attachment.Reference == acquire.Location
	}
	blob, err := s.GetAttachmentBlob(ctx, attachment)
	if err != nil || blob == nil {
		return false
	}
	return blob.StorageID == acquire.StorageID && blob.Sha256 == acquire.Sha256
}

// deleteBlobObject deletes the physical object of one content-addressed blob.
// DATABASE blobs hold bytes inside the blob row and have no object.
func deleteBlobObject(ctx context.Context, profile *profile.Profile, s *store.Store, acquire *store.BlobAcquire, storageType storepb.AttachmentStorageType) error {
	switch {
	case storageType == storepb.AttachmentStorageType_LOCAL || acquire.StorageID == store.BlobStorageLocalID:
		p := filepath.Join(profile.Data, filepath.FromSlash(acquire.Location))
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "failed to remove local blob object")
		}
		return nil
	case acquire.StorageID == store.BlobStorageDatabaseID:
		return nil
	default:
		driver, err := s.StorageDriverByID(ctx, acquire.StorageID)
		if err != nil {
			return err
		}
		if err := driver.DeleteObject(ctx, acquire.Location); err != nil {
			return errors.Wrap(err, "failed to delete s3 blob object")
		}
		return nil
	}
}
