package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func TestLocalBlobDeduplicatedAcrossAttachments(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	user, err := ts.CreateRegularUser(ctx, "blob-dedup-local")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	content := []byte("same local bytes shared by two uploads")
	var attachments []*v1pb.Attachment
	for i := 0; i < 2; i++ {
		attachment, err := ts.Service.CreateAttachment(userCtx, &v1pb.CreateAttachmentRequest{Attachment: &v1pb.Attachment{
			Filename: "shared.txt",
			Type:     "text/plain",
			Content:  content,
		}})
		require.NoError(t, err)
		attachments = append(attachments, attachment)
	}

	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	canonicalPath := filepath.Join(ts.Profile.Data, ".blobs", sha[:2], sha)
	require.FileExists(t, canonicalPath)

	blobs, err := ts.Store.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.Equal(t, int32(2), blobs[0].ReferenceCount)
	require.Equal(t, store.BlobStatusActive, blobs[0].Status)
	require.Equal(t, int64(len(content)), blobs[0].Size)
	require.Equal(t, "local", blobs[0].StorageID)

	for _, attachment := range attachments {
		uid, err := extractUID(attachment.Name)
		require.NoError(t, err)
		stored, err := ts.Store.GetAttachment(ctx, &store.FindAttachment{UID: &uid})
		require.NoError(t, err)
		require.NotNil(t, stored.BlobID)
		require.Equal(t, blobs[0].ID, *stored.BlobID)
		downloaded, err := ts.Service.GetAttachmentBlob(ctx, stored)
		require.NoError(t, err)
		require.Equal(t, content, downloaded)
	}
}

func TestDatabaseBlobDeduplicatedAcrossAttachments(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	_, err := ts.Store.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_STORAGE,
		Value: &storepb.InstanceSetting_StorageSetting{StorageSetting: &storepb.InstanceStorageSetting{
			StorageType: storepb.InstanceStorageSetting_DATABASE,
		}},
	})
	require.NoError(t, err)
	user, err := ts.CreateRegularUser(ctx, "blob-dedup-database")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	content := []byte("same database bytes")
	var rows []*store.Attachment
	for i := 0; i < 2; i++ {
		attachment, err := ts.Service.CreateAttachment(userCtx, &v1pb.CreateAttachmentRequest{Attachment: &v1pb.Attachment{
			Filename: "shared-db.txt",
			Type:     "text/plain",
			Content:  content,
		}})
		require.NoError(t, err)
		uid, err := extractUID(attachment.Name)
		require.NoError(t, err)
		stored, err := ts.Store.GetAttachment(ctx, &store.FindAttachment{UID: &uid})
		require.NoError(t, err)
		rows = append(rows, stored)
	}

	blobs, err := ts.Store.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.Equal(t, int32(2), blobs[0].ReferenceCount)
	require.Equal(t, "database", blobs[0].StorageID)
	for _, row := range rows {
		require.Nil(t, row.Blob, "bytes must no longer be duplicated in the attachment column")
		downloaded, err := ts.Service.GetAttachmentBlob(ctx, row)
		require.NoError(t, err)
		require.Equal(t, content, downloaded)
	}
}

func TestInterruptedChunkedUploadLeavesNoBlobObject(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	defer ts.Service.CloseUploads()
	user, err := ts.CreateRegularUser(ctx, "blob-interrupted")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	start, err := ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{Upload: &v1pb.UploadAttachmentRequest_Spec{
		Spec: &v1pb.UploadAttachmentSpec{Attachment: &v1pb.Attachment{Filename: "half.bin", Type: "application/octet-stream"}, TotalSize: 6},
	}})
	require.NoError(t, err)
	_, err = ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{
		Upload: &v1pb.UploadAttachmentRequest_UploadId{UploadId: start.UploadId},
		Data:   []byte("abc"),
	})
	require.NoError(t, err)

	blobDir := filepath.Join(ts.Profile.Data, ".blobs")
	if entries, err := os.ReadDir(blobDir); err == nil {
		require.Empty(t, entries, "an unfinished chunked upload must never materialize a blob object")
	}
	blobs, err := ts.Store.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Empty(t, blobs)
}

func extractUID(name string) (string, error) {
	return name[len("attachments/"):], nil
}
