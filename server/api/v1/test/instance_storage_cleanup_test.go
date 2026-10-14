package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/internal/testutil/fakes3"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func insertCleanupRow(t *testing.T, ts *TestService, source, storageType, storageID, key, status string, attempts int32, leasedBy string, nextRun, leaseExpires int64) {
	t.Helper()
	keyHash := store.StorageCleanupKeyHash(storepb.AttachmentStorageType(storepb.AttachmentStorageType_value[storageType]), storageID, key)
	_, err := ts.Store.GetDriver().GetDB().ExecContext(context.Background(), `INSERT INTO storage_cleanup_task
		(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error)
		VALUES (?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, 100, 100, ?, ?, 0, '')`,
		source, storageType, storageID, keyHash, key, "uid-"+status+"-"+key, status, attempts, leasedBy, nextRun, leaseExpires)
	require.NoError(t, err)
}

// TestReconcileStorageCleanupReportsAndStaysReadOnly covers admin-only
// access, per-source state counts, orphan detection and the read-only
// guarantee (two scans change neither storage objects nor ledger rows).
func TestReconcileStorageCleanupReportsAndStaysReadOnly(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	fake := fakes3.New(t, "reconcile")
	storage := fakeStorage("s3-reconcile", "Reconcile", fake.Config("reconcile"))
	upsertS3StorageSetting(ctx, t, ts, storage.Id, storage)

	admin, err := ts.CreateHostUser(ctx, "reconcile-admin")
	require.NoError(t, err)
	user, err := ts.CreateRegularUser(ctx, "reconcile-user")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	// Live S3 attachment: object referenced by an existing attachment row.
	liveKey := "live/kept.txt"
	require.NoError(t, fake.PutObject("reconcile", liveKey, "text/plain", []byte("live")))
	_, err = ts.Store.CreateAttachment(ctx, &store.Attachment{
		UID: "reconcile-live", CreatorID: user.ID, Filename: "kept.txt", Type: "text/plain",
		StorageType: storepb.AttachmentStorageType_S3,
		Payload:     &storepb.AttachmentPayload{Payload: &storepb.AttachmentPayload_S3Object_{S3Object: &storepb.AttachmentPayload_S3Object{Key: liveKey, StorageId: storage.Id}}},
	})
	require.NoError(t, err)

	// Ledger-covered S3 object: row deleted, task pending, object still there.
	trackedKey := "tracked/pending.txt"
	require.NoError(t, fake.PutObject("reconcile", trackedKey, "text/plain", []byte("tracked")))
	insertCleanupRow(t, ts, "ATTACHMENT", "S3", storage.Id, trackedKey, "PENDING", 0, "", 0, 0)

	// Orphan objects: one S3, one local.
	orphanKey := "orphan/stray.txt"
	require.NoError(t, fake.PutObject("reconcile", orphanKey, "text/plain", []byte("stray")))
	localOrphan := filepath.Join(ts.Profile.Data, "assets", "local-orphan.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(localOrphan), 0o770))
	require.NoError(t, os.WriteFile(localOrphan, []byte("local"), 0o660))

	// Deterministic four-state coverage across sources (sqlite test store).
	insertCleanupRow(t, ts, "ATTACHMENT", "S3", storage.Id, "done/a.txt", "SUCCEEDED", 1, "", 0, 0)
	insertCleanupRow(t, ts, "MEMO", "S3", storage.Id, "done/b.txt", "SUCCEEDED", 2, "", 0, 0)
	insertCleanupRow(t, ts, "MEMO", "S3", storage.Id, "fail/c.txt", "FAILED", 1, "", 0, 0)
	insertCleanupRow(t, ts, "SPACE", "S3", storage.Id, "run/d.txt", "PROCESSING", 1, "worker-1", 0, 5000)
	insertCleanupRow(t, ts, "USER", "LOCAL", store.LocalStorageID, "assets/user-old.txt", "SUCCEEDED", 1, "", 0, 0)

	// Anonymous and non-admin callers are rejected.
	_, err = ts.Service.ReconcileStorageCleanup(ctx, &v1pb.ReconcileStorageCleanupRequest{})
	require.Error(t, err)
	_, err = ts.Service.ReconcileStorageCleanup(userCtx, &v1pb.ReconcileStorageCleanupRequest{})
	require.Error(t, err)

	response, err := ts.Service.ReconcileStorageCleanup(adminCtx, &v1pb.ReconcileStorageCleanupRequest{})
	require.NoError(t, err)
	require.NotNil(t, response.GeneratedTime)

	bySource := map[v1pb.StorageCleanupSource]*v1pb.ReconcileStorageCleanupResponse_SourceCount{}
	for _, count := range response.SourceCounts {
		bySource[count.Source] = count
	}
	require.Equal(t, int64(1), bySource[v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_ATTACHMENT].PendingCount, "tracked pending seed")
	require.Equal(t, int64(1), bySource[v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_ATTACHMENT].SucceededCount, "done/a.txt seed")
	memoCounts := bySource[v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_MEMO]
	require.Equal(t, int64(1), memoCounts.SucceededCount)
	require.Equal(t, int64(1), memoCounts.FailedCount)
	require.Equal(t, int64(1), bySource[v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_SPACE].ProcessingCount)
	require.Equal(t, int64(1), bySource[v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_USER].SucceededCount)

	var s3Orphans, localOrphans int64
	for _, orphan := range response.OrphanCounts {
		require.True(t, orphan.Available)
		switch orphan.StorageType {
		case v1pb.InstanceSetting_S3:
			s3Orphans += orphan.SuspectedOrphanCount
		case v1pb.InstanceSetting_LOCAL:
			localOrphans += orphan.SuspectedOrphanCount
		}
	}
	require.Equal(t, int64(1), s3Orphans, "only the key without attachment or ledger coverage")
	require.GreaterOrEqual(t, localOrphans, int64(1))

	// Read-only guarantee: a second scan leaves every object and row untouched.
	tasksBefore := snapshotCleanupTasks(t, ts)
	response2, err := ts.Service.ReconcileStorageCleanup(adminCtx, &v1pb.ReconcileStorageCleanupRequest{})
	require.NoError(t, err)
	require.Equal(t, response.SourceCounts, response2.SourceCounts)
	require.Equal(t, response.OrphanCounts, response2.OrphanCounts)
	require.Equal(t, tasksBefore, snapshotCleanupTasks(t, ts))
	for _, key := range []string{liveKey, trackedKey, orphanKey} {
		_, err := fake.GetObject("reconcile", key)
		require.NoError(t, err, "reconciliation must not delete %s", key)
	}
}

func snapshotCleanupTasks(t *testing.T, ts *TestService) string {
	t.Helper()
	rows, err := ts.Store.ListStorageCleanupTasks(context.Background(), 0, 1000)
	require.NoError(t, err)
	snapshot := ""
	for _, row := range rows {
		snapshot += string(row.Status) + ":" + row.StorageType.String() + ":" + row.StorageKey + ":" + row.LeasedBy + ":"
	}
	return snapshot
}
