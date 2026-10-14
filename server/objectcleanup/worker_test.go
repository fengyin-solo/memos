package objectcleanup

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corecleanup "github.com/usememos/memos/core/objectcleanup"
	"github.com/usememos/memos/internal/profile"
	"github.com/usememos/memos/internal/testutil/fakes3"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
	teststore "github.com/usememos/memos/store/test"
)

// failingS3Proxy fronts the in-memory S3 and injects DELETE failures so the
// worker retry paths run without real network chaos.
type failingS3Proxy struct {
	mu sync.Mutex

	target        *url.URL
	proxy         *httputil.ReverseProxy
	deleteCount   int
	failRemaining int
	always403     bool
}

func newFailingS3Proxy(target string) *failingS3Proxy {
	parsed, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	p := &failingS3Proxy{target: parsed}
	p.proxy = httputil.NewSingleHostReverseProxy(parsed)
	return p
}

func (p *failingS3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		p.mu.Lock()
		p.deleteCount++
		fail := p.always403
		if !fail && p.failRemaining > 0 {
			p.failRemaining--
			fail = true
		}
		p.mu.Unlock()
		if fail {
			code := http.StatusInternalServerError
			codeName := "InternalError"
			if p.always403 {
				code = http.StatusForbidden
				codeName = "AccessDenied"
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(code)
			_, _ = io.WriteString(w, fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected failure</Message></Error>`, codeName))
			return
		}
	}
	p.proxy.ServeHTTP(w, r)
}

func (p *failingS3Proxy) deletes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deleteCount
}

type workerTestHarness struct {
	t       *testing.T
	ctx     context.Context
	store   *store.Store
	profile *profile.Profile
	fake    *fakes3.Server
	proxy   *failingS3Proxy
	clock   *fakeClock
}

type fakeClock struct{ t int64 }

func (c *fakeClock) now() time.Time  { return time.Unix(c.t, 0) }
func (c *fakeClock) advance(d int64) { c.t += d }

func setupWorkerTest(t *testing.T) (*workerTestHarness, *Worker) {
	t.Helper()
	ctx := context.Background()
	ts := teststore.NewTestingStore(ctx, t)
	t.Cleanup(func() { _ = ts.Close() })
	fake := fakes3.New(t, "cleanup-worker")
	proxy := newFailingS3Proxy(fake.URL)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)

	storage := &storepb.Storage{
		Id:     "s3-test",
		Name:   "Test S3",
		Type:   storepb.StorageType_STORAGE_TYPE_S3,
		Config: &storepb.Storage_S3Config{S3Config: serverConfig(server.URL, "cleanup-worker")},
	}
	_, err := ts.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_STORAGE,
		Value: &storepb.InstanceSetting_StorageSetting{StorageSetting: &storepb.InstanceStorageSetting{
			Storages: []*storepb.Storage{storage}, DefaultStorageId: storage.Id,
		}},
	})
	require.NoError(t, err)

	h := &workerTestHarness{
		t:       t,
		ctx:     ctx,
		store:   ts,
		profile: &profile.Profile{Data: ts.GetDataDir(), Driver: "sqlite", Version: "test"},
		fake:    fake,
		proxy:   proxy,
		clock:   &fakeClock{t: 1000},
	}
	worker := New(ts, h.profile,
		WithClock(h.clock.now),
		WithWorkerID("test-worker"),
		WithLease(time.Hour),
		WithAttemptTimeout(5*time.Second),
		WithBackoff(objectcleanupBackoff()),
	)
	return h, worker
}

// insertRawCleanupTask writes a task with caller-chosen lease fields directly,
// simulating rows left by a crashed prior worker. These tests run on sqlite.
func insertRawCleanupTask(t *testing.T, ts *store.Store, task *store.StorageCleanupTask) {
	t.Helper()
	_, err := ts.GetDriver().GetDB().ExecContext(context.Background(), `INSERT INTO storage_cleanup_task
		(source, storage_type, storage_id, key_hash, storage_key, payload, attachment_uid, status, attempts, leased_by, created_ts, updated_ts, next_run_ts, lease_expires_ts, completed_ts, last_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(task.Source), task.StorageType.String(), task.StorageID, task.KeyHash, task.StorageKey, task.Payload, task.AttachmentUID,
		string(task.Status), task.Attempts, task.LeasedBy, task.CreatedTs, task.UpdatedTs, task.NextRunTs, task.LeaseExpiresTs, task.CompletedTs, task.LastError)
	require.NoError(t, err)
}

func objectcleanupBackoff() corecleanup.BackoffConfig {
	return corecleanup.BackoffConfig{Base: 10 * time.Second, Factor: 2, Cap: time.Hour}
}

func serverConfig(endpoint, bucket string) *storepb.StorageS3Config {
	return &storepb.StorageS3Config{
		AccessKeyId: "test-access-key", AccessKeySecret: "test-secret-key",
		Endpoint: endpoint, Region: "us-east-1", Bucket: bucket, UsePathStyle: true,
	}
}

// enqueueMemoDeletion creates a private memo with one S3 attachment and
// deletes the memo through the store facade, producing one ledger task.
func (h *workerTestHarness) enqueueMemoDeletion(name, key string) *store.StorageCleanupTask {
	h.t.Helper()
	user, err := h.store.CreateUser(h.ctx, &store.User{Username: name + "-user", Role: store.RoleUser})
	require.NoError(h.t, err)
	memo, err := h.store.CreateMemo(h.ctx, &store.Memo{UID: name + "-memo", CreatorID: user.ID, Content: "x", Visibility: store.Private})
	require.NoError(h.t, err)
	_, err = h.store.CreateAttachment(h.ctx, &store.Attachment{
		UID: name + "-attachment", CreatorID: user.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_S3,
		Payload:     &storepb.AttachmentPayload{Payload: &storepb.AttachmentPayload_S3Object_{S3Object: &storepb.AttachmentPayload_S3Object{Key: key, StorageId: "s3-test"}}},
	})
	require.NoError(h.t, err)
	require.NoError(h.t, h.store.DeleteMemo(h.ctx, &store.DeleteMemo{ID: memo.ID}))

	tasks, err := h.store.ListStorageCleanupTasks(h.ctx, 0, 1000)
	require.NoError(h.t, err)
	for _, task := range tasks {
		if task.StorageKey == key {
			return task
		}
	}
	h.t.Fatalf("ledger task for %q not found", key)
	return nil
}

func (h *workerTestHarness) getTask(key string) *store.StorageCleanupTask {
	h.t.Helper()
	tasks, err := h.store.ListStorageCleanupTasks(h.ctx, 0, 100)
	require.NoError(h.t, err)
	for _, task := range tasks {
		if task.StorageKey == key {
			return task
		}
	}
	h.t.Fatalf("ledger task for %q not found", key)
	return nil
}

// TestWorkerRetriesUntilSucceeds verifies backoff scheduling, last_error and
// the final terminal success state. The S3 SDK retries a 500 three times
// before surfacing the error, so each worker-level failure costs three
// injected DELETE failures.
func TestWorkerRetriesUntilSucceeds(t *testing.T) {
	h, worker := setupWorkerTest(t)
	require.NoError(t, h.fake.PutObject("cleanup-worker", "retry/a.txt", "text/plain", []byte("data")))
	task := h.enqueueMemoDeletion("retry", "retry/a.txt")
	h.proxy.failRemaining = 6 // two worker-level failures (3 SDK attempts each)

	require.NoError(t, worker.RunOnce(h.ctx))
	task = h.getTask(task.StorageKey)
	require.Equal(t, store.StorageCleanupStatusPending, task.Status)
	require.Equal(t, int32(1), task.Attempts)
	require.Equal(t, h.clock.t+10, task.NextRunTs)
	require.NotEmpty(t, task.LastError)
	_, err := h.fake.GetObject("cleanup-worker", task.StorageKey)
	require.NoError(t, err, "object must survive failed attempts")
	require.Equal(t, 3, h.proxy.deletes())

	// Not due yet: nothing happens.
	require.NoError(t, worker.RunOnce(h.ctx))
	require.Equal(t, 3, h.proxy.deletes())

	h.clock.advance(20)
	require.NoError(t, worker.RunOnce(h.ctx))
	task = h.getTask(task.StorageKey)
	require.Equal(t, int32(2), task.Attempts)
	require.Equal(t, h.clock.t+20, task.NextRunTs)
	require.Equal(t, 6, h.proxy.deletes())

	h.clock.advance(30)
	require.NoError(t, worker.RunOnce(h.ctx))
	task = h.getTask(task.StorageKey)
	require.Equal(t, store.StorageCleanupStatusSucceeded, task.Status)
	require.Equal(t, int64(1050), task.CompletedTs)
	require.Empty(t, task.LastError)
	require.Empty(t, task.LeasedBy)
	_, err = h.fake.GetObject("cleanup-worker", task.StorageKey)
	require.Error(t, err, "object deleted after successful retry")
	require.Equal(t, 7, h.proxy.deletes())

	// Terminal tasks are never claimed again.
	require.NoError(t, worker.RunOnce(h.ctx))
	require.Equal(t, 7, h.proxy.deletes(), "succeeded tasks must not be reprocessed")
}

// TestWorkerTerminalFailure verifies deterministic API refusals become FAILED
// and are never retried, and that malformed ledger rows fail terminally
// without calling storage.
func TestWorkerTerminalFailure(t *testing.T) {
	h, worker := setupWorkerTest(t)
	require.NoError(t, h.fake.PutObject("cleanup-worker", "denied/a.txt", "text/plain", []byte("data")))
	task := h.enqueueMemoDeletion("denied", "denied/a.txt")
	h.proxy.always403 = true

	require.NoError(t, worker.RunOnce(h.ctx))
	task = h.getTask(task.StorageKey)
	require.Equal(t, store.StorageCleanupStatusFailed, task.Status)
	require.NotZero(t, task.CompletedTs)
	require.Contains(t, task.LastError, "AccessDenied")
	require.Equal(t, 1, h.proxy.deletes())

	// No further attempts over multiple passes.
	require.NoError(t, worker.RunOnce(h.ctx))
	require.NoError(t, worker.RunOnce(h.ctx))
	require.Equal(t, 1, h.proxy.deletes())
	task = h.getTask(task.StorageKey)
	require.Equal(t, int32(1), task.Attempts)

	// A local task without a key is unrecoverable ledger data: terminal, no delete call.
	bad := &store.StorageCleanupTask{
		Source:      store.StorageCleanupSourceAttachment,
		StorageType: storepb.AttachmentStorageType_LOCAL,
		StorageID:   store.LocalStorageID,
		StorageKey:  "",
		KeyHash:     store.StorageCleanupKeyHash(storepb.AttachmentStorageType_LOCAL, store.LocalStorageID, ""),
		Status:      store.StorageCleanupStatusPending,
		CreatedTs:   1,
		UpdatedTs:   1,
	}
	require.NoError(t, h.store.InsertStorageCleanupTasks(h.ctx, []*store.StorageCleanupTask{bad}))
	require.NoError(t, worker.RunOnce(h.ctx))
	rows, err := h.store.ListStorageCleanupTasks(h.ctx, 0, 100)
	require.NoError(t, err)
	var badRow *store.StorageCleanupTask
	for _, row := range rows {
		if row.StorageType == storepb.AttachmentStorageType_LOCAL {
			badRow = row
		}
	}
	require.NotNil(t, badRow)
	require.Equal(t, store.StorageCleanupStatusFailed, badRow.Status)
	require.Contains(t, badRow.LastError, "storage key")
}

// TestWorkerConcurrentRunOnceProcessesEachTaskOnce proves two workers sharing
// a database never delete the same object twice.
func TestWorkerConcurrentRunOnceProcessesEachTaskOnce(t *testing.T) {
	h, _ := setupWorkerTest(t)
	const taskCount = 16
	for i := range taskCount {
		key := fmt.Sprintf("concurrent/%d.txt", i)
		require.NoError(t, h.fake.PutObject("cleanup-worker", key, "text/plain", []byte(key)))
		h.enqueueMemoDeletion(fmt.Sprintf("concurrent-%d", i), key)
	}

	workerA := New(h.store, h.profile, WithClock(h.clock.now), WithWorkerID("worker-a"), WithLease(time.Hour), WithBackoff(objectcleanupBackoff()), WithBatchSize(taskCount))
	workerB := New(h.store, h.profile, WithClock(h.clock.now), WithWorkerID("worker-b"), WithLease(time.Hour), WithBackoff(objectcleanupBackoff()), WithBatchSize(taskCount))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, worker := range []*Worker{workerA, workerB} {
		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			<-start
			_ = worker.RunOnce(h.ctx)
		}(worker)
	}
	close(start)
	wg.Wait()

	require.Equal(t, taskCount, h.proxy.deletes(), "every object deleted exactly once")
	rows, err := h.store.ListStorageCleanupTasks(h.ctx, 0, taskCount+1)
	require.NoError(t, err)
	require.Len(t, rows, taskCount)
	for _, row := range rows {
		require.Equal(t, store.StorageCleanupStatusSucceeded, row.Status)
		require.Equal(t, int32(1), row.Attempts)
	}
}

// TestWorkerRestartRecovery verifies due PENDING and lease-expired
// PROCESSING rows are picked up by a new incarnation while future rows stay
// put until due.
func TestWorkerRestartRecovery(t *testing.T) {
	h, worker := setupWorkerTest(t)

	seed := func(name string, key string, status store.StorageCleanupStatus, nextRun, leaseExpires int64, attempts int32, leasedBy string) {
		require.NoError(t, h.fake.PutObject("cleanup-worker", key, "text/plain", []byte(key)))
		task := &store.StorageCleanupTask{
			Source: store.StorageCleanupSourceMemo, StorageType: storepb.AttachmentStorageType_S3, StorageID: "s3-test",
			KeyHash: store.StorageCleanupKeyHash(storepb.AttachmentStorageType_S3, "s3-test", key), StorageKey: key,
			Payload:        `{"s3Object":{"key":"` + key + `","storageId":"s3-test"}}`,
			AttachmentUID:  name,
			Status:         status,
			Attempts:       attempts,
			LeasedBy:       leasedBy,
			NextRunTs:      nextRun,
			LeaseExpiresTs: leaseExpires,
			CreatedTs:      1,
			UpdatedTs:      1,
		}
		insertRawCleanupTask(t, h.store, task)
	}
	seed("due", "restart/due.txt", store.StorageCleanupStatusPending, 1000, 0, 0, "")
	seed("future", "restart/future.txt", store.StorageCleanupStatusPending, 5000, 0, 0, "")
	seed("expired", "restart/expired.txt", store.StorageCleanupStatusProcessing, 0, 500, 2, "dead-worker")
	seed("fresh", "restart/fresh.txt", store.StorageCleanupStatusProcessing, 0, 5000, 1, "live-worker")

	require.NoError(t, worker.RunOnce(h.ctx))
	require.Equal(t, 2, h.proxy.deletes(), "due pending and expired processing rows are reclaimed")

	states := map[string]*store.StorageCleanupTask{}
	for _, row := range h.getTasks() {
		states[row.AttachmentUID] = row
	}
	require.Equal(t, store.StorageCleanupStatusSucceeded, states["due"].Status)
	require.Equal(t, store.StorageCleanupStatusSucceeded, states["expired"].Status)
	require.Equal(t, int32(3), states["expired"].Attempts, "reclaimed processing attempts accumulate")
	require.Equal(t, store.StorageCleanupStatusPending, states["future"].Status)
	require.Equal(t, store.StorageCleanupStatusProcessing, states["fresh"].Status)
	require.Equal(t, "live-worker", states["fresh"].LeasedBy)

	// Once the future row is due and the stale lease expires, both finish.
	h.clock.advance(5000)
	require.NoError(t, worker.RunOnce(h.ctx))
	states = map[string]*store.StorageCleanupTask{}
	for _, row := range h.getTasks() {
		states[row.AttachmentUID] = row
	}
	require.Equal(t, store.StorageCleanupStatusSucceeded, states["future"].Status)
	require.Equal(t, store.StorageCleanupStatusSucceeded, states["fresh"].Status)
	require.Equal(t, 4, h.proxy.deletes())
}

func (h *workerTestHarness) getTasks() []*store.StorageCleanupTask {
	h.t.Helper()
	rows, err := h.store.ListStorageCleanupTasks(h.ctx, 0, 100)
	require.NoError(h.t, err)
	return rows
}

// TestWorkerUnreachableStorageRetries verifies an unavailable S3 endpoint
// keeps the task pending with backoff instead of failing the business
// deletion path.
func TestWorkerUnreachableStorageRetries(t *testing.T) {
	ctx := context.Background()
	ts := teststore.NewTestingStore(ctx, t)
	t.Cleanup(func() { _ = ts.Close() })
	storage := &storepb.Storage{
		Id: "s3-gone", Name: "Gone", Type: storepb.StorageType_STORAGE_TYPE_S3,
		Config: &storepb.Storage_S3Config{S3Config: serverConfig("http://127.0.0.1:1", "nowhere")},
	}
	_, err := ts.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_STORAGE,
		Value: &storepb.InstanceSetting_StorageSetting{StorageSetting: &storepb.InstanceStorageSetting{
			Storages: []*storepb.Storage{storage}, DefaultStorageId: storage.Id,
		}},
	})
	require.NoError(t, err)
	user, err := ts.CreateUser(ctx, &store.User{Username: "unreachable-user", Role: store.RoleUser})
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "unreachable-memo", CreatorID: user.ID, Content: "x", Visibility: store.Private})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: "unreachable-attachment", CreatorID: user.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_S3,
		Payload:     &storepb.AttachmentPayload{Payload: &storepb.AttachmentPayload_S3Object_{S3Object: &storepb.AttachmentPayload_S3Object{Key: "gone/a.txt", StorageId: "s3-gone"}}},
	})
	require.NoError(t, err)

	// Business deletion commits even though the object store is unreachable.
	require.NoError(t, ts.DeleteMemo(ctx, &store.DeleteMemo{ID: memo.ID}))
	stored, err := ts.GetMemo(ctx, &store.FindMemo{ID: &memo.ID})
	require.NoError(t, err)
	require.Nil(t, stored, "business delete must commit regardless of storage availability")

	clock := &fakeClock{t: 1000}
	worker := New(ts, &profile.Profile{Data: ts.GetDataDir(), Driver: "sqlite", Version: "test"},
		WithClock(clock.now), WithWorkerID("gone-worker"), WithAttemptTimeout(3*time.Second), WithBackoff(objectcleanupBackoff()))
	require.NoError(t, worker.RunOnce(ctx))
	rows, err := ts.ListStorageCleanupTasks(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, store.StorageCleanupStatusPending, rows[0].Status, "unreachable storage is retryable")
	require.Equal(t, int32(1), rows[0].Attempts)
	require.NotEmpty(t, rows[0].LastError)
}
