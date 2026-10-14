package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func newBlobTestUser(ctx context.Context, t *testing.T, ts *store.Store, name string) *store.User {
	t.Helper()
	user, err := ts.CreateUser(ctx, &store.User{Username: name, Role: store.RoleAdmin, Email: name + "@example.com"})
	require.NoError(t, err)
	return user
}

func acquireBlob(sha string) *store.BlobAcquire {
	return &store.BlobAcquire{
		StorageID: store.BlobStorageLocalID,
		Sha256:    sha,
		Size:      int64(len(sha)),
		Location:  ".blobs/" + sha[:2] + "/" + sha,
	}
}

func requireBlobState(t *testing.T, ts *store.Store, ctx context.Context, storageID, sha string, referenceCount int32, status string) *store.Blob {
	t.Helper()
	blob, err := ts.GetBlob(ctx, &store.FindBlob{StorageID: &storageID, Sha256: &sha})
	require.NoError(t, err)
	require.NotNil(t, blob, "blob %s not found", sha)
	require.Equal(t, referenceCount, blob.ReferenceCount, "unexpected reference count for %s", sha)
	require.Equal(t, status, blob.Status, "unexpected status for %s", sha)
	return blob
}

func createBlobBackedAttachment(ctx context.Context, t *testing.T, ts *store.Store, user *store.User, uid, sha string) *store.Attachment {
	t.Helper()
	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID:          uid,
		CreatorID:    user.ID,
		Filename:    uid + ".bin",
		Type:        "application/octet-stream",
		Size:        int64(len(sha)),
		BlobAcquire: acquireBlob(sha),
	})
	require.NoError(t, err)
	return attachment
}

func TestBlobReferenceCountGraceLifecycle(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-lifecycle")

	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	first := createBlobBackedAttachment(ctx, t, ts, user, "blob-life-1", sha)
	second := createBlobBackedAttachment(ctx, t, ts, user, "blob-life-2", sha)

	blobs, err := ts.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 2, store.BlobStatusActive)
	refs, err := ts.CountAttachmentRefsByBlobs(ctx, []int64{blobs[0].ID})
	require.NoError(t, err)
	require.Equal(t, int32(2), refs[blobs[0].ID])

	// Deleting one reference keeps the blob active without a grace event.
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: first.ID}}))
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 1, store.BlobStatusActive)
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtr(store.BlobEventGraceStart)})
	require.NoError(t, err)
	require.Empty(t, events)

	// Deleting the last reference starts the grace period.
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: second.ID}}))
	blob := requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusPendingDelete)
	require.NotNil(t, blob.OrphanedTs)
	events, err = ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtr(store.BlobEventGraceStart)})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, sha, events[0].Sha256)
	require.Equal(t, blob.ID, *events[0].BlobID)

	refs, err = ts.CountAttachmentRefsByBlobs(ctx, []int64{blob.ID})
	require.NoError(t, err)
	require.Equal(t, int32(0), refs[blob.ID])
}

func TestBlobPendingDeleteIsResurrectedByNewReference(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-resurrect")

	sha := string(make([]byte, 0)) + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	first := createBlobBackedAttachment(ctx, t, ts, user, "blob-res-1", sha)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: first.ID}}))
	pending := requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusPendingDelete)
	orphanedTs := *pending.OrphanedTs

	// A new identical upload resurrects the blob instead of creating a new row.
	again := createBlobBackedAttachment(ctx, t, ts, user, "blob-res-2", sha)
	blobs, err := ts.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NotNil(t, again.BlobID)
	require.Equal(t, pending.ID, *again.BlobID)
	active := requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 1, store.BlobStatusActive)
	require.Nil(t, active.OrphanedTs)
	require.Greater(t, active.UpdatedTs, orphanedTs-1)
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtr(store.BlobEventResurrect)})
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestBlobDistinctContentGetsDistinctBlob(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-distinct")

	shaA := "1111111111111111111111111111111111111111111111111111111111111111"
	shaB := "2222222222222222222222222222222222222222222222222222222222222222"
	createBlobBackedAttachment(ctx, t, ts, user, "blob-distinct-a", shaA)
	createBlobBackedAttachment(ctx, t, ts, user, "blob-distinct-b", shaB)

	blobs, err := ts.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 2)
}

func TestBlobConcurrentAcquireAndRelease(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-concurrent")

	sha := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const writers = 12
	const rounds = 25

	// Seed one reference so the blob always exists.
	anchor := createBlobBackedAttachment(ctx, t, ts, user, "blob-conc-anchor", sha)

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				uid := fmt.Sprintf("blob-conc-%d-%d", i, r)
				attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
					UID: uid, CreatorID: user.ID, Filename: uid + ".bin",
					Type: "application/octet-stream", Size: 1,
					BlobAcquire: acquireBlob(sha),
				})
				if err != nil {
					errCh <- err
					return
				}
				if err := ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: attachment.ID}}); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	blob := requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 1, store.BlobStatusActive)
	refs, err := ts.CountAttachmentRefsByBlobs(ctx, []int64{blob.ID})
	require.NoError(t, err)
	require.Equal(t, int32(1), refs[blob.ID])

	// Removing the anchor drives the blob into the grace period exactly once.
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: anchor.ID}}))
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusPendingDelete)
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtr(store.BlobEventGraceStart)})
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestLegacyManagedAttachmentDeleteRegistersOrphan(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-legacy")

	// A not-yet-backfilled managed attachment has no blob reference.
	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "blob-legacy-1", CreatorID: user.ID, Filename: "legacy.txt",
		Type: "text/plain", Size: 3,
		StorageType: storepb.AttachmentStorageType_LOCAL,
		Reference:  "assets/legacy.txt",
	})
	require.NoError(t, err)
	require.Nil(t, attachment.BlobID)

	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: attachment.ID}}))
	orphans, err := ts.ListBlobOrphans(ctx, &store.FindBlobOrphan{Status: strPtr(store.BlobOrphanStatusPending)})
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.Equal(t, store.BlobOrphanLegacyDelete, orphans[0].Reason)
	require.Equal(t, store.BlobStorageLocalID, orphans[0].StorageID)
	require.Equal(t, "assets/legacy.txt", orphans[0].Location)
}

func TestLegacyExternalAttachmentDeleteRegistersNoOrphan(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "blob-external")

	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "blob-external-1", CreatorID: user.ID, Filename: "external.png",
		Type: "image/png", Size: 0,
		StorageType: storepb.AttachmentStorageType_EXTERNAL,
		Reference:  "https://example.com/a.png",
	})
	require.NoError(t, err)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: attachment.ID}}))
	orphans, err := ts.ListBlobOrphans(ctx, &store.FindBlobOrphan{})
	require.NoError(t, err)
	require.Empty(t, orphans)
	blobs, err := ts.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Empty(t, blobs)
}

func strPtr(value string) *string {
	return &value
}
