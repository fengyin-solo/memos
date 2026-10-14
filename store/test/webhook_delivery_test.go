package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/store"
)

func createTestDelivery(ctx context.Context, t *testing.T, ts *store.Store, key, webhookID string, nextAttemptSec int64) *store.WebhookDelivery {
	t.Helper()
	delivery, inserted, err := ts.CreateWebhookDelivery(ctx, &store.CreateWebhookDelivery{
		IdempotencyKey: key,
		WebhookID:      webhookID,
		UserID:         3,
		URL:            "http://example.com/hook",
		Label:          "memos.memo.created",
		SigningSecret:  "secret",
		Payload:        []byte(`{"hello":"world"}`),
		MessageID:      "msg_dlv_" + key,
		MaxAttempts:    5,
	})
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, store.WebhookDeliveryPending, delivery.Status)
	require.Equal(t, 5, int(delivery.MaxAttempts))

	if nextAttemptSec != 0 {
		require.NoError(t, ts.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
			ID:            delivery.ID,
			Status:        store.WebhookDeliveryPending,
			Attempts:      0,
			NextAttemptTs: nextAttemptSec,
		}))
		refreshed, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDelivery{ID: &delivery.ID})
		require.NoError(t, err)
		require.Len(t, refreshed, 1)
		return refreshed[0]
	}
	return delivery
}

// TestWebhookDeliveryIdempotentInsert verifies the same event never creates a
// second row, regardless of how many times it is enqueued.
func TestWebhookDeliveryIdempotentInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	create := &store.CreateWebhookDelivery{
		IdempotencyKey: "idem-1",
		WebhookID:      "hook-1",
		URL:            "http://example.com",
		Payload:        []byte(`{}`),
		MessageID:      "msg_dlv_1",
		MaxAttempts:    5,
	}
	first, inserted, err := ts.CreateWebhookDelivery(ctx, create)
	require.NoError(t, err)
	require.True(t, inserted)

	second, inserted, err := ts.CreateWebhookDelivery(ctx, create)
	require.NoError(t, err)
	require.False(t, inserted, "duplicate idempotency key must not insert")
	require.Equal(t, first.ID, second.ID)

	list, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDelivery{})
	require.NoError(t, err)
	require.Len(t, list, 1)
}

// TestWebhookDeliveryClaimLifecycle verifies due rows are claimed atomically,
// future rows are skipped, completed rows leave the queue, and a stale
// PROCESSING claim is reclaimed.
func TestWebhookDeliveryClaimLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	nowSec := time.Now().Unix()
	due1 := createTestDelivery(ctx, t, ts, "key-due-1", "hook-1", nowSec-10)
	due2 := createTestDelivery(ctx, t, ts, "key-due-2", "hook-1", nowSec-5)
	createTestDelivery(ctx, t, ts, "key-future", "hook-1", nowSec+3600)

	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 1, nowSec, nowSec-120, "worker-a")
	require.NoError(t, err)
	require.Len(t, claimed, 1, "batch size limits the claim")
	require.Equal(t, due1.ID, claimed[0].ID, "oldest due row is claimed first")
	require.Equal(t, store.WebhookDeliveryProcessing, claimed[0].Status)
	require.Equal(t, int32(1), claimed[0].Attempts)
	require.Equal(t, "worker-a", claimed[0].ClaimedBy)

	claimed, err = ts.ClaimDueWebhookDeliveries(ctx, 10, nowSec, nowSec-120, "worker-b")
	require.NoError(t, err)
	require.Len(t, claimed, 1, "the freshly claimed PROCESSING row and the future row are skipped")
	require.Equal(t, due2.ID, claimed[0].ID)

	// Succeed the first one; it must never be claimable again.
	require.NoError(t, ts.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
		ID:             due1.ID,
		Status:         store.WebhookDeliverySucceeded,
		Attempts:       1,
		LastStatusCode: 200,
		SucceededTs:    nowSec,
	}))
	claimed, err = ts.ClaimDueWebhookDeliveries(ctx, 10, nowSec, nowSec-120, "worker-c")
	require.NoError(t, err)
	require.Empty(t, claimed)

	// A PROCESSING claim older than the staleness cutoff (simulating a crashed
	// worker) is reclaimed and its attempt count increments.
	staleClaimed, err := ts.ClaimDueWebhookDeliveries(ctx, 10, nowSec, nowSec-120, "worker-d")
	require.NoError(t, err)
	require.Empty(t, staleClaimed, "due2 was just claimed and is still fresh")

	reclaimed, err := ts.ClaimDueWebhookDeliveries(ctx, 10, nowSec+300, nowSec+300-120, "worker-e")
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, due2.ID, reclaimed[0].ID)
	require.Equal(t, int32(2), reclaimed[0].Attempts, "the reclaim counts as the second attempt")
	require.Equal(t, "worker-e", reclaimed[0].ClaimedBy)
}

// TestWebhookDeliveryGroupedCounts verifies per-webhook, per-status aggregation
// the administrator stats endpoint builds on, including folding PROCESSING in
// separately from terminal states.
func TestWebhookDeliveryGroupedCounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	// hook-a: 1 succeeded, 1 failed, 2 dead letter, 1 pending, 1 processing.
	statusesA := []store.WebhookDeliveryStatus{
		store.WebhookDeliverySucceeded,
		store.WebhookDeliveryFailed,
		store.WebhookDeliveryDeadLetter,
		store.WebhookDeliveryDeadLetter,
	}
	for i, status := range statusesA {
		delivery, _, err := ts.CreateWebhookDelivery(ctx, &store.CreateWebhookDelivery{
			IdempotencyKey: "a-" + string(rune('a'+i)),
			WebhookID:      "hook-a",
			UserID:         1,
			URL:            "http://example.com/a",
			Payload:        []byte(`{}`),
			MaxAttempts:    5,
		})
		require.NoError(t, err)
		require.NoError(t, ts.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
			ID:       delivery.ID,
			Status:   status,
			Attempts: 1,
		}))
	}
	// One row stays in flight as PROCESSING.
	_, _, err := ts.CreateWebhookDelivery(ctx, &store.CreateWebhookDelivery{
		IdempotencyKey: "a-processing",
		WebhookID:      "hook-a",
		UserID:         1,
		URL:            "http://example.com/a",
		Payload:        []byte(`{}`),
		MaxAttempts:    5,
	})
	require.NoError(t, err)
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 10, time.Now().Unix(), 0, "w")
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	// A waiting PENDING row is enqueued after the claim and must not move.
	createTestDelivery(ctx, t, ts, "a-pending", "hook-a", 0)

	// hook-b: one succeeded.
	bDelivery, _, err := ts.CreateWebhookDelivery(ctx, &store.CreateWebhookDelivery{
		IdempotencyKey: "b-1",
		WebhookID:      "hook-b",
		UserID:         2,
		URL:            "http://example.com/b",
		Payload:        []byte(`{}`),
		MaxAttempts:    5,
	})
	require.NoError(t, err)
	require.NoError(t, ts.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
		ID: bDelivery.ID, Status: store.WebhookDeliverySucceeded, Attempts: 1,
	}))

	counts, err := ts.CountWebhookDeliveriesGrouped(ctx, nil)
	require.NoError(t, err)
	grouped := map[string]map[store.WebhookDeliveryStatus]int64{}
	for _, count := range counts {
		if grouped[count.WebhookID] == nil {
			grouped[count.WebhookID] = map[store.WebhookDeliveryStatus]int64{}
		}
		grouped[count.WebhookID][count.Status] = count.Count
	}
	require.Equal(t, int64(1), grouped["hook-a"][store.WebhookDeliverySucceeded])
	require.Equal(t, int64(1), grouped["hook-a"][store.WebhookDeliveryFailed])
	require.Equal(t, int64(2), grouped["hook-a"][store.WebhookDeliveryDeadLetter])
	require.Equal(t, int64(1), grouped["hook-a"][store.WebhookDeliveryPending])
	require.Equal(t, int64(1), grouped["hook-a"][store.WebhookDeliveryProcessing])
	require.Equal(t, int64(1), grouped["hook-b"][store.WebhookDeliverySucceeded])

	hookA := "hook-a"
	filtered, err := ts.CountWebhookDeliveriesGrouped(ctx, &hookA)
	require.NoError(t, err)
	for _, count := range filtered {
		require.Equal(t, "hook-a", count.WebhookID)
	}
	require.Len(t, filtered, 5)
}
