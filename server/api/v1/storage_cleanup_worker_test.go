package v1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corecleanup "github.com/usememos/memos/core/objectcleanup"
	servercleanup "github.com/usememos/memos/server/objectcleanup"
	"github.com/usememos/memos/store"
)

// runCleanupWorkerOnce drives a single storage cleanup worker pass so tests
// can observe the asynchronous object deletion that previously happened
// synchronously after a committed business deletion.
func runCleanupWorkerOnce(t *testing.T, svc *APIV1Service) {
	t.Helper()
	runCleanupWorkerOnceCtx(t, svc, context.Background())
}

// runCleanupWorkerOnceCtx is like runCleanupWorkerOnce with a custom context,
// used to exercise test-only storage deletion failpoints.
func runCleanupWorkerOnceCtx(t *testing.T, svc *APIV1Service, ctx context.Context) {
	t.Helper()
	worker := servercleanup.New(svc.Store, svc.Profile,
		servercleanup.WithWorkerID("test-worker"),
		servercleanup.WithLease(time.Hour),
		// Zero base keeps failed attempts immediately due so tests can drive
		// the retry transition without waiting for the production backoff.
		servercleanup.WithBackoff(corecleanup.BackoffConfig{Base: 0, Factor: 2, Cap: time.Hour}),
	)
	require.NoError(t, worker.RunOnce(ctx))
}

// requireCleanupTaskStatus finds the ledger task for storageKey and asserts
// its final status.
func requireCleanupTaskStatus(t *testing.T, svc *APIV1Service, storageKey string, status store.StorageCleanupStatus) *store.StorageCleanupTask {
	t.Helper()
	ctx := context.Background()
	var afterID int64
	for {
		tasks, err := svc.Store.ListStorageCleanupTasks(ctx, afterID, 1000)
		require.NoError(t, err)
		if len(tasks) == 0 {
			t.Fatalf("cleanup task for %q not found", storageKey)
		}
		for _, task := range tasks {
			if task.StorageKey == storageKey {
				require.Equal(t, status, task.Status, "task %s state", storageKey)
				return task
			}
		}
		afterID = tasks[len(tasks)-1].ID
	}
}
