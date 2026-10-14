package webhookdelivery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/pkg/errors"

	"github.com/usememos/memos/internal/webhook"
	"github.com/usememos/memos/store"
)

// lastErrorLimit keeps error messages readable and bounded in the database.
const lastErrorLimit = 1024

// completeTimeout bounds the outcome write independently of the worker
// lifecycle context, so a shutdown during an HTTP attempt still records the
// result instead of relying on stale-claim reclamation.
const completeTimeout = 10 * time.Second

// Start launches the worker goroutines that claim and send due deliveries.
// Calling Start more than once is a no-op.
func (q *Queue) Start(ctx context.Context) {
	if !q.started.CompareAndSwap(false, true) {
		return
	}
	q.ctx, q.cancel = context.WithCancel(ctx)
	for i := 0; i < q.config.WorkerCount; i++ {
		q.wg.Add(1)
		go q.runWorker(q.ctx, i)
	}
	slog.Info("webhook delivery queue started",
		slog.Int("workers", q.config.WorkerCount),
		slog.Int("batch_size", q.config.BatchSize),
		slog.Int("max_attempts", int(q.config.MaxAttempts)))
}

// Shutdown stops the workers. In-flight attempts keep their PROCESSING claim;
// if shutdown interrupts the outcome write, the claim goes stale and is
// reclaimed on the next start, preserving at-least-once delivery.
func (q *Queue) Shutdown(ctx context.Context) error {
	if !q.started.CompareAndSwap(true, false) {
		return nil
	}
	q.cancel()

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "webhook delivery workers did not stop")
	}
}

func (q *Queue) runWorker(ctx context.Context, index int) {
	defer q.wg.Done()
	workerID := fmt.Sprintf("webhook-worker-%d", index)
	ticker := time.NewTicker(q.config.PollInterval)
	defer ticker.Stop()

	q.tick(ctx, workerID)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-q.notify:
		}
		q.tick(ctx, workerID)
	}
}

// tick claims one batch and sends every delivery in it. Claim errors are
// transient (locked database, closed connection) and simply retried next tick.
func (q *Queue) tick(ctx context.Context, workerID string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("webhook delivery worker panicked",
				slog.String("worker", workerID),
				slog.Any("panic", r))
		}
	}()

	nowTime := q.now()
	nowSec := nowTime.Unix()
	deliveries, err := q.store.ClaimDueWebhookDeliveries(ctx, q.config.BatchSize, nowSec, nowTime.Add(-q.config.ClaimTTL).Unix(), workerID)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("failed to claim webhook deliveries", slog.String("worker", workerID), slog.Any("err", err))
		}
		return
	}
	for _, delivery := range deliveries {
		if ctx.Err() != nil {
			// Claimed but unsent rows stay PROCESSING and are reclaimed once
			// the claim goes stale after restart.
			return
		}
		q.process(ctx, delivery)
	}
}

// process performs one attempt for a claimed delivery and records the outcome.
func (q *Queue) process(ctx context.Context, delivery *store.WebhookDelivery) {
	sendCtx, cancel := context.WithTimeout(ctx, q.config.SendTimeout)
	result, retryable, sendErr := q.sender(sendCtx, &webhook.Request{
		URL:           delivery.URL,
		Label:         delivery.Label,
		SigningSecret: delivery.SigningSecret,
		Payload:       json.RawMessage(delivery.Payload),
		MessageID:     delivery.MessageID,
	})
	cancel()

	nowSec := q.now().Unix()
	complete := &store.CompleteWebhookDelivery{
		ID:       delivery.ID,
		Attempts: delivery.Attempts,
	}
	if result != nil {
		complete.LastStatusCode = int32(result.StatusCode)
	}

	switch {
	case sendErr == nil:
		complete.Status = store.WebhookDeliverySucceeded
		complete.SucceededTs = nowSec
	case !retryable:
		complete.Status = store.WebhookDeliveryFailed
		complete.LastError = truncateError(sendErr)
		slog.Warn("webhook delivery permanently failed",
			slog.Int64("delivery_id", delivery.ID),
			slog.String("url", delivery.URL),
			slog.String("label", delivery.Label),
			slog.Any("err", sendErr))
	case delivery.Attempts >= delivery.MaxAttempts:
		complete.Status = store.WebhookDeliveryDeadLetter
		complete.LastError = truncateError(sendErr)
		complete.NextAttemptTs = 0
		slog.Error("webhook delivery moved to dead letter after exhausting attempts",
			slog.Int64("delivery_id", delivery.ID),
			slog.String("url", delivery.URL),
			slog.String("label", delivery.Label),
			slog.Int("attempts", int(delivery.Attempts)),
			slog.Any("err", sendErr))
	default:
		delay := q.retryDelay(delivery.Attempts)
		complete.Status = store.WebhookDeliveryPending
		complete.NextAttemptTs = nowSec + int64(delay.Seconds())
		complete.LastError = truncateError(sendErr)
		slog.Info("webhook delivery scheduled for retry",
			slog.Int64("delivery_id", delivery.ID),
			slog.String("url", delivery.URL),
			slog.Int("attempts", int(delivery.Attempts)),
			slog.Duration("backoff", delay))
	}

	// Persist the outcome even when the worker context is being canceled.
	completeCtx, completeCancel := context.WithTimeout(context.Background(), completeTimeout)
	defer completeCancel()
	if err := q.store.CompleteWebhookDelivery(completeCtx, complete); err != nil {
		slog.Error("failed to record webhook delivery outcome",
			slog.Int64("delivery_id", delivery.ID),
			slog.String("status", string(complete.Status)),
			slog.Any("err", err))
	}
}

func truncateError(err error) string {
	message := err.Error()
	if len(message) > lastErrorLimit {
		return message[:lastErrorLimit]
	}
	return message
}
