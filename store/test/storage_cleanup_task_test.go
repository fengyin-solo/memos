package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lithammer/shortuuid/v4"
	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func newCleanupTask(kind string, n int, status store.StorageCleanupStatus) *store.StorageCleanupTask {
	task := &store.StorageCleanupTask{
		Source:        store.StorageCleanupSourceMemo,
		Status:        status,
		CreatedTs:     100,
		UpdatedTs:     100,
		AttachmentUID: shortuuid.New(),
	}
	switch kind {
	case "local":
		task.StorageType = storepb.AttachmentStorageType_LOCAL
		task.StorageID = store.LocalStorageID
		task.StorageKey = fmt.Sprintf("assets/%d/file.txt", n)
	default:
		task.StorageType = storepb.AttachmentStorageType_S3
		task.StorageID = "s3-primary"
		task.StorageKey = fmt.Sprintf("attachments/%d/file.txt", n)
		task.Payload = `{"s3Object":{"key":"attachments/` + fmt.Sprintf("%d", n) + `/file.txt","storageId":"s3-primary"}}`
	}
	task.KeyHash = store.StorageCleanupKeyHash(task.StorageType, task.StorageID, task.StorageKey)
	return task
}

func insertCleanupTaskRows(t *testing.T, ts *store.Store, tasks []*store.StorageCleanupTask) {
	t.Helper()
	ctx := context.Background()
	args := func(task *store.StorageCleanupTask) []any {
		return []any{
			string(task.Source), task.StorageType.String(), task.StorageID, task.KeyHash, task.StorageKey, task.Payload, task.AttachmentUID,
			string(task.Status), task.Attempts, task.LeasedBy, task.CreatedTs, task.UpdatedTs, task.NextRunTs, task.LeaseExpiresTs, task.CompletedTs, task.LastError,
		}
	}
	statement := `INSERT INTO storage_cleanup_task
			(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	postgres := getDriverFromEnv() == "postgres"
	if postgres {
		statement = `INSERT INTO storage_cleanup_task
			(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING id`
	}
	// Direct insert lets each test choose status/lease/backoff fields.
	for _, task := range tasks {
		if postgres {
			require.NoError(t, ts.GetDriver().GetDB().QueryRowContext(ctx, statement, args(task)...).Scan(&task.ID))
			continue
		}
		result, err := ts.GetDriver().GetDB().ExecContext(ctx, statement, args(task)...)
		require.NoError(t, err)
		id, err := result.LastInsertId()
		require.NoError(t, err)
		task.ID = id
	}
}

func listAllCleanupTasks(t *testing.T, ts *store.Store) []*store.StorageCleanupTask {
	t.Helper()
	ctx := context.Background()
	all := make([]*store.StorageCleanupTask, 0)
	var afterID int64
	for {
		page, err := ts.ListStorageCleanupTasks(ctx, afterID, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			return all
		}
		all = append(all, page...)
		afterID = page[len(page)-1].ID
	}
}

// TestStorageCleanupInsertDeduplicatesKey inserts the same object twice and
// expects a single row.
func TestStorageCleanupInsertDeduplicatesKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	task := newCleanupTask("s3", 1, store.StorageCleanupStatusPending)
	require.NoError(t, ts.InsertStorageCleanupTasks(ctx, []*store.StorageCleanupTask{task}))
	// Same identity, different source/uid must still collapse to one row.
	duplicate := *task
	duplicate.Source = store.StorageCleanupSourceSpace
	require.NoError(t, ts.InsertStorageCleanupTasks(ctx, []*store.StorageCleanupTask{&duplicate}))

	tasks := listAllCleanupTasks(t, ts)
	require.Len(t, tasks, 1)
	require.Equal(t, string(store.StorageCleanupStatusPending), string(tasks[0].Status))
	require.Equal(t, "attachments/1/file.txt", tasks[0].StorageKey)
}

// TestStorageCleanupClaimSelection covers due PENDING rows, future PENDING
// rows, expired PROCESSING leases and fresh PROCESSING leases.
func TestStorageCleanupClaimSelection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	due := newCleanupTask("local", 1, store.StorageCleanupStatusPending)
	future := newCleanupTask("local", 2, store.StorageCleanupStatusPending)
	future.NextRunTs = 2000
	expired := newCleanupTask("s3", 3, store.StorageCleanupStatusProcessing)
	expired.LeasedBy = "dead-worker"
	expired.Attempts = 2
	expired.LeaseExpiresTs = 500
	fresh := newCleanupTask("s3", 4, store.StorageCleanupStatusProcessing)
	fresh.LeasedBy = "live-worker"
	fresh.Attempts = 1
	fresh.LeaseExpiresTs = 5000
	insertCleanupTaskRows(t, ts, []*store.StorageCleanupTask{due, future, expired, fresh})

	claimed, err := ts.ClaimStorageCleanupTasks(ctx, 1000, 1100, "new-worker", 16)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	ids := map[int64]string{}
	for _, task := range claimed {
		ids[task.ID] = string(task.Status)
		require.Equal(t, store.StorageCleanupStatusProcessing, task.Status)
		require.Equal(t, "new-worker", task.LeasedBy)
		require.Equal(t, int64(1100), task.LeaseExpiresTs)
	}
	require.Contains(t, ids, due.ID)
	require.Contains(t, ids, expired.ID)

	claimedByID := map[int64]*store.StorageCleanupTask{}
	for _, task := range claimed {
		claimedByID[task.ID] = task
	}
	require.Equal(t, int32(1), claimedByID[due.ID].Attempts, "first claim increments attempts")
	require.Equal(t, int32(3), claimedByID[expired.ID].Attempts, "lease recovery accumulates attempts")

	// Nothing is due now: future is not ready and the other two have fresh leases.
	again, err := ts.ClaimStorageCleanupTasks(ctx, 1000, 1100, "new-worker", 16)
	require.NoError(t, err)
	require.Empty(t, again)
}

// TestStorageCleanupConcurrentClaim proves two concurrent claimants never
// process the same row.
func TestStorageCleanupConcurrentClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	const taskCount = 24
	tasks := make([]*store.StorageCleanupTask, 0, taskCount)
	for i := range taskCount {
		tasks = append(tasks, newCleanupTask("s3", i, store.StorageCleanupStatusPending))
	}
	insertCleanupTaskRows(t, ts, tasks)

	const workers = 6
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		claimed  []int64
		claimErr error
		start    = make(chan struct{})
	)
	for workerIndex := range workers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			batch, err := ts.ClaimStorageCleanupTasks(ctx, 1000, 1100, fmt.Sprintf("worker-%d", index), taskCount)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				claimErr = err
				return
			}
			for _, task := range batch {
				claimed = append(claimed, task.ID)
			}
		}(workerIndex)
	}
	close(start)
	wg.Wait()
	require.NoError(t, claimErr)
	require.Len(t, claimed, taskCount)

	seen := map[int64]int{}
	for _, id := range claimed {
		seen[id]++
	}
	require.Len(t, seen, taskCount, "every task claimed exactly once")
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
	for _, task := range listAllCleanupTasks(t, ts) {
		require.Equal(t, int32(1), task.Attempts)
		require.Equal(t, store.StorageCleanupStatusProcessing, task.Status)
	}
}

// TestStorageCleanupFinishTransitions covers success, terminal failure and
// retry write-backs plus the lease-owner guard.
func TestStorageCleanupFinishTransitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	succeed := newCleanupTask("local", 1, store.StorageCleanupStatusProcessing)
	succeed.LeasedBy = "w1"
	fail := newCleanupTask("local", 2, store.StorageCleanupStatusProcessing)
	fail.LeasedBy = "w1"
	retry := newCleanupTask("s3", 3, store.StorageCleanupStatusProcessing)
	retry.LeasedBy = "w1"
	stolen := newCleanupTask("s3", 4, store.StorageCleanupStatusProcessing)
	stolen.LeasedBy = "w2"
	insertCleanupTaskRows(t, ts, []*store.StorageCleanupTask{succeed, fail, retry, stolen})

	ok, err := ts.FinishStorageCleanupTask(ctx, &store.StorageCleanupTaskUpdate{
		ID: succeed.ID, WorkerID: "w1", Status: store.StorageCleanupStatusSucceeded, Now: 2000,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ts.FinishStorageCleanupTask(ctx, &store.StorageCleanupTaskUpdate{
		ID: fail.ID, WorkerID: "w1", Status: store.StorageCleanupStatusFailed, LastError: "AccessDenied", Now: 2000,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = ts.FinishStorageCleanupTask(ctx, &store.StorageCleanupTaskUpdate{
		ID: retry.ID, WorkerID: "w1", Status: store.StorageCleanupStatusPending, LastError: "timeout", NextRunTs: 3000, Now: 2000,
	})
	require.NoError(t, err)
	require.True(t, ok)

	// A worker that lost its lease cannot write results.
	ok, err = ts.FinishStorageCleanupTask(ctx, &store.StorageCleanupTaskUpdate{
		ID: stolen.ID, WorkerID: "w1", Status: store.StorageCleanupStatusSucceeded, Now: 2000,
	})
	require.NoError(t, err)
	require.False(t, ok)

	byID := map[int64]*store.StorageCleanupTask{}
	for _, task := range listAllCleanupTasks(t, ts) {
		byID[task.ID] = task
	}
	require.Equal(t, store.StorageCleanupStatusSucceeded, byID[succeed.ID].Status)
	require.Equal(t, int64(2000), byID[succeed.ID].CompletedTs)
	require.Empty(t, byID[succeed.ID].LeasedBy)
	require.Empty(t, byID[succeed.ID].LastError)

	require.Equal(t, store.StorageCleanupStatusFailed, byID[fail.ID].Status)
	require.Equal(t, int64(2000), byID[fail.ID].CompletedTs)
	require.Equal(t, "AccessDenied", byID[fail.ID].LastError)

	require.Equal(t, store.StorageCleanupStatusPending, byID[retry.ID].Status)
	require.Equal(t, int64(3000), byID[retry.ID].NextRunTs)
	require.Zero(t, byID[retry.ID].CompletedTs)
	require.Equal(t, "timeout", byID[retry.ID].LastError)

	require.Equal(t, store.StorageCleanupStatusProcessing, byID[stolen.ID].Status)
	require.Equal(t, "w2", byID[stolen.ID].LeasedBy)

	// Terminal rows are never claimable; the retry row becomes claimable at due time.
	claimed, err := ts.ClaimStorageCleanupTasks(ctx, 2500, 2600, "w3", 16)
	require.NoError(t, err)
	require.Empty(t, claimed)
	claimed, err = ts.ClaimStorageCleanupTasks(ctx, 3500, 3600, "w3", 16)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, retry.ID, claimed[0].ID)
}

// TestStorageCleanupStats groups counts by source and status.
func TestStorageCleanupStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	rows := []*store.StorageCleanupTask{
		newCleanupTask("local", 1, store.StorageCleanupStatusPending),
		newCleanupTask("local", 2, store.StorageCleanupStatusPending),
		newCleanupTask("s3", 3, store.StorageCleanupStatusSucceeded),
	}
	rows[0].Source = store.StorageCleanupSourceAttachment
	rows[1].Source = store.StorageCleanupSourceSpace
	rows[2].Source = store.StorageCleanupSourceAttachment
	insertCleanupTaskRows(t, ts, rows)

	stats, err := ts.GetStorageCleanupStats(ctx)
	require.NoError(t, err)
	counts := map[string]int64{}
	for _, stat := range stats {
		counts[string(stat.Source)+"/"+string(stat.Status)] = stat.Count
	}
	require.Equal(t, int64(1), counts["ATTACHMENT/PENDING"])
	require.Equal(t, int64(1), counts["ATTACHMENT/SUCCEEDED"])
	require.Equal(t, int64(1), counts["SPACE/PENDING"])
}

// TestListAttachmentStorageRefs returns keys for live LOCAL/S3 attachments
// and excludes DATABASE blobs, paging internally.
func TestListAttachmentStorageRefs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	_, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: shortuuid.New(), CreatorID: 101, Filename: "local.txt", Type: "text/plain",
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/local.txt",
	})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: shortuuid.New(), CreatorID: 101, Filename: "s3.txt", Type: "text/plain",
		StorageType: storepb.AttachmentStorageType_S3,
		Payload: &storepb.AttachmentPayload{Payload: &storepb.AttachmentPayload_S3Object_{
			S3Object: &storepb.AttachmentPayload_S3Object{Key: "attachments/s3.txt", StorageId: "s3-primary"},
		}},
	})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: shortuuid.New(), CreatorID: 101, Filename: "db.bin", Type: "application/octet-stream",
		StorageType: storepb.AttachmentStorageType_ATTACHMENT_STORAGE_TYPE_UNSPECIFIED, Blob: []byte("blob"),
	})
	require.NoError(t, err)

	refs, err := ts.ListAttachmentStorageRefs(ctx)
	require.NoError(t, err)
	keys := map[string]string{}
	for _, ref := range refs {
		keys[ref.StorageType.String()+":"+ref.StorageID+":"+ref.StorageKey] = ref.StorageKey
	}
	require.Contains(t, keys, "LOCAL:local:assets/local.txt")
	require.Contains(t, keys, "S3:s3-primary:attachments/s3.txt")
	require.Len(t, keys, 2)
}
