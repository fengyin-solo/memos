package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func gcBlobSetup(ctx context.Context, t *testing.T, ts *store.Store) (*store.User, []byte, string) {
	t.Helper()
	user := newBlobTestUser(ctx, t, ts, "blob-gc")
	content := []byte("content destined for the garbage collector")
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	return user, content, sha
}

// materializeLocalBlob writes a content-addressed local object at its
// canonical path and returns its internal location.
func materializeLocalBlob(t *testing.T, ts *store.Store, sha string, content []byte) string {
	t.Helper()
	location := filepath.Join(".blobs", sha[:2], sha)
	path := filepath.Join(ts.GetDataDir(), location)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o770))
	require.NoError(t, os.WriteFile(path, content, 0o644))
	return filepath.ToSlash(location)
}

func createManagedAttachment(ctx context.Context, t *testing.T, ts *store.Store, user *store.User, uid, sha, location string, size int64, storageID string) *store.Attachment {
	t.Helper()
	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: uid, CreatorID: user.ID, Filename: uid + ".bin",
		Type: "application/octet-stream", Size: size,
		BlobAcquire: &store.BlobAcquire{StorageID: storageID, Sha256: sha, Size: size, Location: location},
	})
	require.NoError(t, err)
	return attachment
}

func TestBlobGCGracePeriodDryRunAndDelete(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, content, sha := gcBlobSetup(ctx, t, ts)
	location := materializeLocalBlob(t, ts, sha, content)
	first := createManagedAttachment(ctx, t, ts, user, "gc-grace-1", sha, location, int64(len(content)), store.BlobStorageLocalID)
	second := createManagedAttachment(ctx, t, ts, user, "gc-grace-2", sha, location, int64(len(content)), store.BlobStorageLocalID)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: first.ID}, {ID: second.ID}}))

	// Not due yet while inside the grace period.
	report, err := ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: time.Hour, OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 0, report.ScannedBlobs)
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusPendingDelete)
	require.FileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))

	// Dry run reports the candidate but changes nothing.
	report, err = ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: time.Hour, DryRun: true, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.DueCandidates)
	require.EqualValues(t, 0, report.DeletedBlobs)
	require.NotZero(t, report.Skipped[store.GCSkipDryRun])
	require.FileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))

	// The real run removes object, row, and records the completion event.
	report, err = ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.DeletedBlobs)
	require.NoFileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))
	localStorageID := store.BlobStorageLocalID
	blob, err := ts.GetBlob(ctx, &store.FindBlob{StorageID: &localStorageID, Sha256: &sha})
	require.NoError(t, err)
	require.Nil(t, blob)
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtrStore(store.BlobEventDeleteCompleted)})
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestBlobGCSkipsResurrectedBlob(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, content, sha := gcBlobSetup(ctx, t, ts)
	location := materializeLocalBlob(t, ts, sha, content)
	first := createManagedAttachment(ctx, t, ts, user, "gc-res-1", sha, location, int64(len(content)), store.BlobStorageLocalID)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: first.ID}}))
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusPendingDelete)

	// A new identical attachment resurrects the blob.
	createManagedAttachment(ctx, t, ts, user, "gc-res-2", sha, location, int64(len(content)), store.BlobStorageLocalID)
	report, err := ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 0, report.DeletedBlobs)
	require.NotZero(t, report.Skipped[store.GCSkipResurrected])
	require.FileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 1, store.BlobStatusActive)
}

func TestBlobGCCorruptObjectMarkedNotDeleted(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, content, sha := gcBlobSetup(ctx, t, ts)
	location := materializeLocalBlob(t, ts, sha, content)
	attachment := createManagedAttachment(ctx, t, ts, user, "gc-corrupt", sha, location, int64(len(content)), store.BlobStorageLocalID)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: attachment.ID}}))

	// Tamper with the physical object.
	require.NoError(t, os.WriteFile(filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)), []byte("totally different bytes here"), 0o644))

	report, err := ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.CorruptBlobs)
	require.EqualValues(t, 0, report.DeletedBlobs)
	require.FileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))
	requireBlobState(t, ts, ctx, store.BlobStorageLocalID, sha, 0, store.BlobStatusCorrupt)
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtrStore(store.BlobEventCorruptDetected)})
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestBlobGCMissingObjectFinalizesIdempotently(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, content, sha := gcBlobSetup(ctx, t, ts)
	location := materializeLocalBlob(t, ts, sha, content)
	attachment := createManagedAttachment(ctx, t, ts, user, "gc-missing", sha, location, int64(len(content)), store.BlobStorageLocalID)
	require.NoError(t, ts.GetDriver().DeleteAttachments(ctx, []*store.DeleteAttachment{{ID: attachment.ID}}))
	require.NoError(t, os.Remove(filepath.Join(ts.GetDataDir(), filepath.FromSlash(location))))

	report, err := ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.MissingBlobs)
	localStorageID := store.BlobStorageLocalID
	blob, err := ts.GetBlob(ctx, &store.FindBlob{StorageID: &localStorageID, Sha256: &sha})
	require.NoError(t, err)
	require.Nil(t, blob)
}

func TestLegacyAttachmentBackfillAndOrphanReclaim(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "backfill")
	content := []byte("duplicate legacy bytes")

	pathA := filepath.Join(ts.GetDataDir(), "assets", "dup-a.txt")
	pathB := filepath.Join(ts.GetDataDir(), "assets", "dup-b.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(pathA), 0o770))
	require.NoError(t, os.WriteFile(pathA, content, 0o644))
	require.NoError(t, os.WriteFile(pathB, content, 0o644))

	createLegacy := func(uid, relPath string) *store.Attachment {
		attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
			UID: uid, CreatorID: user.ID, Filename: uid + ".txt", Type: "text/plain",
			Size: int64(len(content)), StorageType: storepb.AttachmentStorageType_LOCAL, Reference: relPath,
		})
		require.NoError(t, err)
		return attachment
	}
	a := createLegacy("backfill-a", filepath.ToSlash(filepath.Join("assets", "dup-a.txt")))
	b := createLegacy("backfill-b", filepath.ToSlash(filepath.Join("assets", "dup-b.txt")))
	_, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "backfill-ext", CreatorID: user.ID, Filename: "ext.png", Type: "image/png",
		StorageType: storepb.AttachmentStorageType_EXTERNAL, Reference: "https://example.com/e.png",
	})
	require.NoError(t, err)

	stats, err := ts.RunAttachmentBackfill(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Done)
	require.Equal(t, 1, stats.Duplicates)
	require.Equal(t, 1, stats.Skipped)
	require.Zero(t, stats.Failed)

	storedA, err := ts.GetAttachment(ctx, &store.FindAttachment{ID: &a.ID})
	require.NoError(t, err)
	storedB, err := ts.GetAttachment(ctx, &store.FindAttachment{ID: &b.ID})
	require.NoError(t, err)
	require.NotNil(t, storedA.BlobID)
	require.Equal(t, storedA.BlobID, storedB.BlobID)
	require.Equal(t, store.AttachmentBlobBackfillDone, storedA.BlobBackfillStatus)
	require.Equal(t, store.AttachmentBlobBackfillSkipped, func() string {
		uid := "backfill-ext"
		ext, _ := ts.GetAttachment(ctx, &store.FindAttachment{UID: &uid})
		return ext.BlobBackfillStatus
	}())

	blobs, err := ts.ListBlobs(ctx, &store.FindBlob{})
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.Equal(t, int32(2), blobs[0].ReferenceCount)
	// Blob canonical location is the first legacy path; the other is a duplicate orphan.
	require.Equal(t, storedA.Reference, blobs[0].Location)

	orphans, err := ts.ListBlobOrphans(ctx, &store.FindBlobOrphan{Reason: strPtrStore(store.BlobOrphanMigrationDup)})
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.Equal(t, filepath.ToSlash(filepath.Join("assets", "dup-b.txt")), orphans[0].Location)

	// GC reclaims the duplicate but keeps the canonical object.
	report, err := ts.RunBlobGC(ctx, store.GCOptions{GracePeriod: 0, OrphanSafetyAge: 0, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.OrphansDeleted)
	require.NoFileExists(t, pathB)
	require.FileExists(t, pathA)
	// Both backfilled attachments still resolve content via the canonical blob.
	for _, stored := range []*store.Attachment{storedA, storedB} {
		blob, err := ts.GetBlob(ctx, &store.FindBlob{ID: stored.BlobID})
		require.NoError(t, err)
		require.Equal(t, filepath.ToSlash(filepath.Join("assets", "dup-a.txt")), blob.Location)
	}
}

func TestLegacyBackfillFailedIsRetried(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := newBlobTestUser(ctx, t, ts, "backfill-fail")

	missing := "assets/does-not-exist.txt"
	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "backfill-missing", CreatorID: user.ID, Filename: "missing.txt", Type: "text/plain",
		Size: 4, StorageType: storepb.AttachmentStorageType_LOCAL, Reference: missing,
	})
	require.NoError(t, err)

	stats, err := ts.RunAttachmentBackfill(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Failed)
	stored, err := ts.GetAttachment(ctx, &store.FindAttachment{ID: &attachment.ID})
	require.NoError(t, err)
	require.Nil(t, stored.BlobID)
	require.Equal(t, store.AttachmentBlobBackfillFailed, stored.BlobBackfillStatus)

	// Make the object available; the next pass succeeds.
	require.NoError(t, os.MkdirAll(filepath.Join(ts.GetDataDir(), "assets"), 0o770))
	require.NoError(t, os.WriteFile(filepath.Join(ts.GetDataDir(), missing), []byte("data"), 0o644))
	stats, err = ts.RunAttachmentBackfill(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Done)
}

func TestGCAbandonedObjectSafetyAge(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	content := []byte("abandoned upload bytes")
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	location := materializeLocalBlob(t, ts, sha, content)

	// Fresh object: protected by the safety age even with no blob row.
	report, err := ts.RunBlobGC(ctx, store.GCOptions{OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.NotZero(t, report.Skipped[store.GCSkipTooYoung])
	require.FileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))

	// Backdate the object past the safety age: swept as an abandoned upload.
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)), old, old))
	report, err = ts.RunBlobGC(ctx, store.GCOptions{OrphanSafetyAge: time.Hour, SweepAbandonedObjects: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, report.AbandonedDeleted)
	require.NoFileExists(t, filepath.Join(ts.GetDataDir(), filepath.FromSlash(location)))
	events, err := ts.ListBlobEvents(ctx, &store.FindBlobEvent{Type: strPtrStore(store.BlobEventOrphanDelete)})
	require.NoError(t, err)
	require.NotEmpty(t, events)
}

func strPtrStore(value string) *string { return &value }
