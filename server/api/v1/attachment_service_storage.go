package v1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/usememos/memos/internal/profile"
	"github.com/usememos/memos/internal/random"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func convertAttachmentFromStore(attachment *store.Attachment) *v1pb.Attachment {
	attachmentMessage := &v1pb.Attachment{
		Name:          fmt.Sprintf("%s%s", AttachmentNamePrefix, attachment.UID),
		CreateTime:    timestamppb.New(time.Unix(attachment.CreatedTs, 0)),
		Filename:      attachment.Filename,
		Type:          attachment.Type,
		Size:          attachment.Size,
		MotionMedia:   convertMotionMediaFromStore(getAttachmentMotionMedia(attachment)),
		MediaMetadata: convertMediaMetadataFromStore(attachment.Payload.GetMediaMetadata()),
	}
	if attachment.MemoUID != nil && *attachment.MemoUID != "" {
		memoName := fmt.Sprintf("%s%s", MemoNamePrefix, *attachment.MemoUID)
		attachmentMessage.Memo = &memoName
	}
	// Managed storage is always addressed through the authenticated file route.
	// In particular, never expose an expiring S3 presigned URL as attachment API
	// metadata because it can outlive a memo visibility change.
	if attachment.StorageType == storepb.AttachmentStorageType_EXTERNAL {
		attachmentMessage.ExternalLink = attachment.Reference
	}

	return attachmentMessage
}

// attachmentContextReader stops a long local copy once the request context is
// canceled; io.Copy has no context of its own, unlike the S3 client.
type attachmentContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *attachmentContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// saveAttachmentContent writes the processed attachment bytes to a
// content-addressed object in the default storage and fills
// create.BlobAcquire so the attachment insert binds the blob row in the
// same transaction. It reports whether the physical object already
// existed and was shared; callers must not delete such an object on
// failure.
func saveAttachmentContent(
	ctx context.Context,
	profile *profile.Profile,
	stores *store.Store,
	create *store.Attachment,
	instanceStorageSetting *storepb.InstanceStorageSetting,
	content io.Reader,
) (bool, error) {
	defaultStorage := store.GetDefaultStorage(instanceStorageSetting)
	if defaultStorage == nil {
		return false, errors.New("default storage is not configured")
	}

	switch defaultStorage.Type {
	case storepb.StorageType_STORAGE_TYPE_LOCAL:
		return storeLocalBlob(ctx, profile.Data, content, create)
	case storepb.StorageType_STORAGE_TYPE_S3:
		return storeS3Blob(ctx, stores, defaultStorage.Id, create.Type, content, create)
	default:
		return false, storeDatabaseBlob(content, create)
	}
}

func (s *APIV1Service) cleanupDeletedAttachmentStorage(ctx context.Context, attachments []*store.Attachment) error {
	var instanceStorageSetting *storepb.InstanceStorageSetting
	var instanceStorageSettingErr error
	for _, attachment := range attachments {
		if store.AttachmentNeedsInstanceStorageSetting(attachment) {
			instanceStorageSetting, instanceStorageSettingErr = s.Store.GetInstanceStorageSetting(ctx)
			break
		}
	}

	var firstErr error
	for _, attachment := range attachments {
		if attachment == nil {
			continue
		}
		var err error
		if instanceStorageSettingErr != nil && store.AttachmentNeedsInstanceStorageSetting(attachment) {
			err = errors.Wrap(instanceStorageSettingErr, "failed to get instance storage setting")
		} else {
			err = s.Store.DeleteAttachmentStorageWithInstanceSetting(ctx, attachment, instanceStorageSetting)
		}
		if err != nil && firstErr == nil {
			firstErr = errors.Wrapf(err, "attachment %d", attachment.ID)
		}
	}
	return firstErr
}

// GetAttachmentBlob reads an attachment from its configured storage.
func (s *APIV1Service) GetAttachmentBlob(ctx context.Context, attachment *store.Attachment) ([]byte, error) {
	content, err := s.openAttachmentContent(ctx, attachment)
	if err != nil {
		return nil, err
	}
	defer content.Close()
	blob, err := io.ReadAll(content)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read attachment content")
	}
	return blob, nil
}

// openAttachmentContent streams an attachment from its configured storage.
// Content-addressed attachments resolve the physical object through their blob
// row; legacy attachments keep using reference/payload/inline bytes.
func (s *APIV1Service) openAttachmentContent(ctx context.Context, attachment *store.Attachment) (io.ReadCloser, error) {
	if attachment.BlobID != nil {
		blob, err := s.Store.GetAttachmentBlob(ctx, attachment)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load attachment blob")
		}
		if blob == nil {
			return nil, errors.New("attachment blob not found")
		}
		switch blob.StorageID {
		case store.BlobStorageLocalID:
			attachmentPath := filepath.Join(s.Profile.Data, filepath.FromSlash(blob.Location))
			file, err := os.Open(attachmentPath)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, errors.Wrap(err, "file not found")
				}
				return nil, errors.Wrap(err, "failed to open the file")
			}
			return file, nil
		case store.BlobStorageDatabaseID:
			return io.NopCloser(bytes.NewReader(blob.Content)), nil
		default:
			driver, err := s.Store.StorageDriverByID(ctx, blob.StorageID)
			if err != nil {
				return nil, errors.Wrap(err, "failed to resolve blob storage driver")
			}
			object, err := driver.GetObjectStream(ctx, blob.Location, "")
			if err != nil {
				return nil, errors.Wrap(err, "failed to stream blob object from S3")
			}
			return object.Body, nil
		}
	}

	switch attachment.StorageType {
	case storepb.AttachmentStorageType_LOCAL:
		attachmentPath := filepath.FromSlash(attachment.Reference)
		if !filepath.IsAbs(attachmentPath) {
			attachmentPath = filepath.Join(s.Profile.Data, attachmentPath)
		}
		file, err := os.Open(attachmentPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, errors.Wrap(err, "file not found")
			}
			return nil, errors.Wrap(err, "failed to open the file")
		}
		return file, nil
	case storepb.AttachmentStorageType_S3:
		driver, s3Object, err := s.Store.ResolveAttachmentS3Driver(ctx, attachment)
		if err != nil {
			return nil, errors.Wrap(err, "failed to resolve S3 attachment driver")
		}
		object, err := driver.GetObjectStream(ctx, s3Object.Key, "")
		if err != nil {
			return nil, errors.Wrap(err, "failed to stream object from S3")
		}
		return object.Body, nil
	default:
		blob := attachment.Blob
		if blob == nil {
			stored, err := s.Store.GetAttachment(ctx, &store.FindAttachment{ID: &attachment.ID, GetBlob: true})
			if err != nil {
				return nil, errors.Wrap(err, "failed to load attachment blob")
			}
			if stored == nil {
				return nil, errors.New("attachment not found")
			}
			blob = stored.Blob
		}
		return io.NopCloser(bytes.NewReader(blob)), nil
	}
}

var fileKeyPattern = regexp.MustCompile(`\{[a-z]{1,9}\}`)

func replaceFilenameWithPathTemplate(path, filename string) string {
	t := time.Now()
	path = fileKeyPattern.ReplaceAllStringFunc(path, func(s string) string {
		switch s {
		case "{filename}":
			return filename
		case "{timestamp}":
			return fmt.Sprintf("%d", t.Unix())
		case "{year}":
			return fmt.Sprintf("%d", t.Year())
		case "{month}":
			return fmt.Sprintf("%02d", t.Month())
		case "{day}":
			return fmt.Sprintf("%02d", t.Day())
		case "{hour}":
			return fmt.Sprintf("%02d", t.Hour())
		case "{minute}":
			return fmt.Sprintf("%02d", t.Minute())
		case "{second}":
			return fmt.Sprintf("%02d", t.Second())
		case "{uuid}":
			return random.UUID()
		default:
			return s
		}
	})
	return path
}

func ensureUniqueLocalAttachmentPath(path, uid string) string {
	if _, err := os.Stat(path); err != nil {
		return path
	}

	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	return base + "_" + uid + ext
}

func validateFilename(filename string) bool {
	// Reject path traversal attempts and make sure no additional directories are created
	if !filepath.IsLocal(filename) || strings.ContainsAny(filename, "/\\") {
		return false
	}

	// Reject filenames starting or ending with spaces or periods
	if strings.HasPrefix(filename, " ") || strings.HasSuffix(filename, " ") ||
		strings.HasPrefix(filename, ".") || strings.HasSuffix(filename, ".") {
		return false
	}

	return true
}

func normalizeMimeType(mimeType string) (string, bool) {
	mimeType = strings.TrimSpace(mimeType)
	if mimeType == "" || len(mimeType) > 255 {
		return "", false
	}

	mediaType, _, err := mime.ParseMediaType(mimeType)
	if err != nil || mediaType == "" || len(mediaType) > 255 {
		return "", false
	}

	return mediaType, true
}
