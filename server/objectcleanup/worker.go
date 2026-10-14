// Package objectcleanup runs the background worker that deletes external
// attachment objects recorded in the storage cleanup ledger, and the read-only
// reconciliation used by the instance admin API.
package objectcleanup

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/usememos/memos/core/objectcleanup"
	"github.com/usememos/memos/internal/profile"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

const (
	defaultPollInterval   = 30 * time.Second
	defaultBatchSize      = 16
	defaultLeaseDuration  = 2 * time.Minute
	defaultAttemptTimeout = 30 * time.Second
)

// Worker claims storage cleanup tasks and deletes their objects. It is safe
// for multiple worker instances (or processes) to run against the same
// database: claim and finish writes are conditional on lease ownership.
type Worker struct {
	stores  *store.Store
	profile *profile.Profile

	workerID       string
	pollInterval   time.Duration
	batchSize      int
	leaseDuration  time.Duration
	attemptTimeout time.Duration
	backoff        objectcleanup.BackoffConfig
	now            func() time.Time

	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}
	cancel context.CancelFunc

	runMu sync.Mutex
}

// Option customizes a Worker, primarily to make timing deterministic in tests.
type Option func(*Worker)

// WithPollInterval sets the idle polling interval.
func WithPollInterval(interval time.Duration) Option {
	return func(w *Worker) { w.pollInterval = interval }
}

// WithBatchSize sets the maximum number of tasks claimed per pass.
func WithBatchSize(size int) Option {
	return func(w *Worker) { w.batchSize = size }
}

// WithLease sets how long a claim stays owned before another worker may
// reclaim it after a crash.
func WithLease(lease time.Duration) Option {
	return func(w *Worker) { w.leaseDuration = lease }
}

// WithAttemptTimeout bounds a single object deletion.
func WithAttemptTimeout(timeout time.Duration) Option {
	return func(w *Worker) { w.attemptTimeout = timeout }
}

// WithBackoff replaces the retry backoff policy.
func WithBackoff(backoff objectcleanup.BackoffConfig) Option {
	return func(w *Worker) { w.backoff = backoff }
}

// WithClock injects the time source.
func WithClock(now func() time.Time) Option {
	return func(w *Worker) { w.now = now }
}

// WithWorkerID sets the lease owner identity.
func WithWorkerID(id string) Option {
	return func(w *Worker) { w.workerID = id }
}

// New creates a worker. Start launches its loop; Shutdown stops it.
func New(stores *store.Store, profile *profile.Profile, options ...Option) *Worker {
	w := &Worker{
		stores:         stores,
		profile:        profile,
		workerID:       defaultWorkerID(),
		pollInterval:   defaultPollInterval,
		batchSize:      defaultBatchSize,
		leaseDuration:  defaultLeaseDuration,
		attemptTimeout: defaultAttemptTimeout,
		backoff:        objectcleanup.DefaultBackoff(),
		now:            time.Now,
		wake:           make(chan struct{}, 1),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	for _, option := range options {
		option(w)
	}
	return w
}

// Start launches the polling loop. It runs an initial pass immediately so
// tasks committed while the process was down are handled without waiting for
// the first tick.
func (w *Worker) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	go w.loop(ctx)
}

// Shutdown stops claiming, cancels in-flight attempts and waits for the loop
// to exit or the context deadline. Tasks interrupted this way keep their
// PROCESSING lease and are reclaimed after it expires.
func (w *Worker) Shutdown(ctx context.Context) error {
	close(w.stop)
	if w.cancel != nil {
		w.cancel()
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Notify asks the loop to run a pass soon (e.g. after a fresh deletion). It
// never blocks: without a reader the signal collapses onto the next tick.
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	kick := true
	for {
		if kick {
			if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("storage cleanup pass failed", slog.Any("err", err))
			}
			kick = false
		}
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-ticker.C:
			kick = true
		case <-w.wake:
			kick = true
		}
	}
}

// RunOnce claims one batch of due tasks and processes it. Exported so tests
// can drive processing deterministically.
func (w *Worker) RunOnce(parent context.Context) error {
	// Serialize concurrent RunOnce calls (loop plus Notify-driven callers in
	// tests): a lease row must not be observed by two passes in one process.
	w.runMu.Lock()
	defer w.runMu.Unlock()

	now := w.now()
	tasks, err := w.stores.ClaimStorageCleanupTasks(parent, now.Unix(), now.Add(w.leaseDuration).Unix(), w.workerID, w.batchSize)
	if err != nil {
		if parent.Err() != nil {
			return nil
		}
		return errors.Wrap(err, "failed to claim storage cleanup tasks")
	}
	if len(tasks) == 0 {
		return nil
	}

	// The instance storage registry is read once per batch. A read failure is
	// not fatal: named S3 tasks become retryable until the registry is readable
	// again, while tasks carrying legacy embedded config can still resolve.
	instanceStorageSetting, settingErr := w.stores.GetInstanceStorageSetting(parent)
	if settingErr != nil && parent.Err() != nil {
		return nil
	}
	if settingErr != nil {
		slog.Warn("storage cleanup proceeds without instance storage registry", slog.Any("err", settingErr))
		instanceStorageSetting = nil
	}

	for _, task := range tasks {
		if parent.Err() != nil {
			return nil
		}
		w.processTask(parent, task, instanceStorageSetting)
	}
	return nil
}

// processTask deletes one claimed object and writes the result back. When the
// worker is shutting down it leaves the row in PROCESSING state so lease
// expiry hands it to the next incarnation.
func (w *Worker) processTask(parent context.Context, task *store.StorageCleanupTask, instanceStorageSetting *storepb.InstanceStorageSetting) {
	attachment, err := attachmentFromCleanupTask(task)
	if err != nil {
		w.finish(parent, task, objectcleanup.OutcomeFailTerminal, errors.Wrap(err, "malformed cleanup task").Error(), 0)
		return
	}

	attemptCtx, cancel := context.WithTimeout(parent, w.attemptTimeout)
	defer cancel()
	deleteErr := w.stores.DeleteAttachmentStorageWithInstanceSetting(attemptCtx, attachment, instanceStorageSetting)
	if deleteErr == nil {
		w.finish(parent, task, objectcleanup.OutcomeSucceed, "", 0)
		return
	}

	// Shutdown cancellation is not a deletion outcome: keep the lease for the
	// next process incarnation.
	if parent.Err() != nil {
		return
	}

	var apiErr smithy.APIError
	apiErrorCode := ""
	if errors.As(deleteErr, &apiErr) {
		apiErrorCode = apiErr.ErrorCode()
	}
	outcome := objectcleanup.ClassifyDeleteError(task.StorageType, apiErrorCode)
	var nextRunTs int64
	if outcome == objectcleanup.OutcomeRetry {
		nextRunTs = w.backoff.NextRun(int(task.Attempts), w.now()).Unix()
	}
	w.finish(parent, task, outcome, deleteErr.Error(), nextRunTs)
}

// finish writes the processing result unless the claim was lost, in which
// case another worker owns the object deletion and this result is discarded.
func (w *Worker) finish(parent context.Context, task *store.StorageCleanupTask, outcome objectcleanup.Outcome, lastError string, nextRunTs int64) {
	update := &store.StorageCleanupTaskUpdate{ID: task.ID, WorkerID: w.workerID, Now: w.now().Unix(), LastError: lastError}
	switch outcome {
	case objectcleanup.OutcomeSucceed:
		update.Status = store.StorageCleanupStatusSucceeded
		slog.Info("storage cleanup task completed", slog.Int64("task_id", task.ID), slog.String("source", string(task.Source)), slog.String("key", task.StorageKey))
	case objectcleanup.OutcomeFailTerminal:
		update.Status = store.StorageCleanupStatusFailed
		slog.Error("storage cleanup task failed permanently", slog.Int64("task_id", task.ID), slog.String("source", string(task.Source)), slog.String("error", lastError))
	default:
		update.Status = store.StorageCleanupStatusPending
		update.NextRunTs = nextRunTs
		slog.Warn("storage cleanup task scheduled for retry", slog.Int64("task_id", task.ID), slog.Int64("attempts", int64(task.Attempts)), slog.Int64("next_run_ts", nextRunTs), slog.String("error", lastError))
	}
	owned, err := w.stores.FinishStorageCleanupTask(parent, update)
	if err != nil && parent.Err() == nil {
		slog.Error("failed to write storage cleanup task result", slog.Int64("task_id", task.ID), slog.Any("err", err))
		return
	}
	if !owned {
		slog.Warn("storage cleanup lease lost, discarding result", slog.Int64("task_id", task.ID))
	}
}

var cleanupPayloadUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// attachmentFromCleanupTask reconstructs the store attachment the existing
// deletion code expects. Missing keys or payloads are unrecoverable data
// errors rather than storage failures.
func attachmentFromCleanupTask(task *store.StorageCleanupTask) (*store.Attachment, error) {
	attachment := &store.Attachment{
		UID:         task.AttachmentUID,
		StorageType: task.StorageType,
		Reference:   task.StorageKey,
	}
	switch task.StorageType {
	case storepb.AttachmentStorageType_LOCAL:
		if task.StorageKey == "" {
			return nil, errors.New("local cleanup task is missing its storage key")
		}
	case storepb.AttachmentStorageType_ATTACHMENT_STORAGE_TYPE_UNSPECIFIED:
		// Cache-only task for a database blob: no object to delete, the
		// existing cleanup tail removes derived thumbnail/motion caches.
		if task.AttachmentUID == "" {
			return nil, errors.New("cache-only task is missing its attachment uid")
		}
		attachment.Reference = ""
		attachment.Payload = nil
	case storepb.AttachmentStorageType_S3:
		payload := &storepb.AttachmentPayload{}
		if task.Payload != "" {
			if err := cleanupPayloadUnmarshal.Unmarshal([]byte(task.Payload), payload); err != nil {
				return nil, errors.Wrap(err, "S3 cleanup task payload is not valid JSON")
			}
		}
		s3Object := payload.GetS3Object()
		if s3Object == nil {
			return nil, errors.New("S3 cleanup task is missing its object payload")
		}
		if task.StorageKey == "" {
			return nil, errors.New("S3 cleanup task is missing its storage key")
		}
		// The ledger key is authoritative; keep it consistent with the payload.
		s3Object.Key = task.StorageKey
		if task.StorageID != "" {
			s3Object.StorageId = task.StorageID
		}
		attachment.Payload = payload
	default:
		return nil, errors.Errorf("unsupported cleanup task storage type %s", task.StorageType.String())
	}
	return attachment, nil
}
