package webhookdelivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/usememos/memos/internal/profile"
	"github.com/usememos/memos/internal/webhook"
	"github.com/usememos/memos/store"
	"github.com/usememos/memos/store/db"
)

var errDeliveryNotFound = stderrors.New("webhook delivery not found")

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	p := &profile.Profile{
		Driver:  "sqlite",
		DSN:     filepath.Join(t.TempDir(), "webhook_queue_test.db"),
		Version: "test",
	}
	driver, err := db.NewDBDriver(p)
	require.NoError(t, err)
	st := store.New(driver, p)
	require.NoError(t, st.Migrate(context.Background()))
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// testConfig returns a fast, deterministic-feeling configuration: tight poll
// and backoff so retry/dead-letter transitions complete quickly.
func testConfig() Config {
	return Config{
		WorkerCount:    2,
		PollInterval:   15 * time.Millisecond,
		BatchSize:      8,
		MaxAttempts:    3,
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ClaimTTL:       2 * time.Second,
		SendTimeout:    2 * time.Second,
	}
}

func startQueue(t *testing.T, st *store.Store, opts ...Option) *Queue {
	t.Helper()
	q := New(st, opts...)
	q.Start(context.Background())
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.Shutdown(shutdownCtx)
	})
	return q
}

func allowPrivateWebhooks(t *testing.T) {
	t.Helper()
	// httptest receivers listen on the 127.0.0.0/8 loopback range, which the
	// SSRF guard blocks by default.
	require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist([]string{"127.0.0.0/8"}))
	t.Cleanup(func() { require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist(nil)) })
}

func findDelivery(st *store.Store, id int64) (*store.WebhookDelivery, error) {
	rows, err := st.ListWebhookDeliveries(context.Background(), &store.FindWebhookDelivery{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errDeliveryNotFound
	}
	return rows[0], nil
}

func getDelivery(t *testing.T, st *store.Store, id int64) *store.WebhookDelivery {
	t.Helper()
	delivery, err := findDelivery(st, id)
	require.NoError(t, err)
	return delivery
}

func waitForStatus(t *testing.T, st *store.Store, id int64, status store.WebhookDeliveryStatus) *store.WebhookDelivery {
	t.Helper()
	require.Eventually(t, func() bool {
		delivery, err := findDelivery(st, id)
		return err == nil && delivery.Status == status
	}, 5*time.Second, 10*time.Millisecond, "delivery %d never reached %s", id, status)
	return getDelivery(t, st, id)
}

func enqueueSimple(t *testing.T, q *Queue, webhookID, eventKey string, url string) *store.WebhookDelivery {
	t.Helper()
	delivery, inserted, err := q.Enqueue(context.Background(), &EnqueueRequest{
		EventKey:      eventKey,
		WebhookID:     webhookID,
		UserID:        1,
		URL:           url,
		Label:         "memos.memo.created",
		SigningSecret: "",
		Payload:       map[string]any{"hello": eventKey},
	})
	require.NoError(t, err)
	require.True(t, inserted)
	return delivery
}

// TestQueueDeliversAndSigns covers the happy path plus signature verification:
// the receiver recomputes the Standard Webhooks HMAC over the exact stored
// body and confirms a stable message id.
func TestQueueDeliversAndSigns(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	rawKey := []byte("0123456789abcdef0123456789abcdef")
	secret := "whsec_" + base64.StdEncoding.EncodeToString(rawKey)
	var attempts atomic.Int32
	type receivedRequest struct {
		id        string
		timestamp string
		signature string
		body      []byte
	}
	var received []receivedRequest
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, receivedRequest{
			id:        r.Header.Get("webhook-id"),
			timestamp: r.Header.Get("webhook-timestamp"),
			signature: r.Header.Get("webhook-signature"),
			body:      body,
		})
		mu.Unlock()
		attempts.Add(1)
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	q := startQueue(t, st, WithConfig(testConfig()))
	delivery, _, err := q.Enqueue(context.Background(), &EnqueueRequest{
		EventKey:      "event-1",
		WebhookID:     "hook-1",
		UserID:        7,
		URL:           server.URL,
		Label:         "memos.memo.created",
		SigningSecret: secret,
		Payload:       map[string]any{"hello": "world"},
	})
	require.NoError(t, err)

	final := waitForStatus(t, st, delivery.ID, store.WebhookDeliverySucceeded)
	require.Equal(t, int32(1), final.Attempts)
	require.Equal(t, int32(http.StatusOK), final.LastStatusCode)
	require.Greater(t, final.SucceededTs, int64(0))
	require.Equal(t, int32(1), attempts.Load())

	mu.Lock()
	require.Len(t, received, 1)
	request := received[0]
	mu.Unlock()
	require.True(t, strings.HasPrefix(request.id, "msg_dlv_"))
	mac := hmac.New(sha256.New, rawKey)
	mac.Write([]byte(request.id + "." + request.timestamp + "."))
	mac.Write(request.body)
	expected := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	require.Equal(t, expected, request.signature)
	require.Contains(t, string(request.body), `"hello":"world"`)
}

// TestQueue4xxPermanentFailure verifies an explicit 4xx is recorded as a
// non-retryable failure after exactly one attempt.
func TestQueue4xxPermanentFailure(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	q := startQueue(t, st, WithConfig(testConfig()))
	delivery := enqueueSimple(t, q, "hook", "ev-4xx", server.URL)

	final := waitForStatus(t, st, delivery.ID, store.WebhookDeliveryFailed)
	require.Equal(t, int32(1), final.Attempts)
	require.Equal(t, int32(http.StatusNotFound), final.LastStatusCode)
	require.NotEmpty(t, final.LastError)
	require.Equal(t, int32(1), attempts.Load())

	// A failed delivery must not be redelivered.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(1), attempts.Load())
}

// TestQueueRetries5xxToDeadLetter verifies retryable 5xx burns the attempt
// budget with repeated sends and ends in the dead-letter terminal state.
func TestQueueRetries5xxToDeadLetter(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	config := testConfig()
	config.MaxAttempts = 3
	q := startQueue(t, st, WithConfig(config))
	delivery := enqueueSimple(t, q, "hook", "ev-5xx", server.URL)

	final := waitForStatus(t, st, delivery.ID, store.WebhookDeliveryDeadLetter)
	require.Equal(t, int32(3), final.Attempts)
	require.Equal(t, int32(http.StatusServiceUnavailable), final.LastStatusCode)
	require.Equal(t, int32(3), attempts.Load())
	require.NotEmpty(t, final.LastError)
}

// TestQueueRetryableFailureRecovers verifies a receiver that fails twice and
// then accepts leads to SUCCEEDED, with identical body and message id on
// every attempt and attempts reflecting the retry count.
func TestQueueRetryableFailureRecovers(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var attempts atomic.Int32
	messageIDs := map[string]struct{}{}
	bodies := map[string]struct{}{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		messageIDs[r.Header.Get("webhook-id")] = struct{}{}
		bodies[string(body)] = struct{}{}
		mu.Unlock()
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	config := testConfig()
	config.MaxAttempts = 5
	q := startQueue(t, st, WithConfig(config))
	delivery := enqueueSimple(t, q, "hook", "ev-flaky", server.URL)

	final := waitForStatus(t, st, delivery.ID, store.WebhookDeliverySucceeded)
	require.Equal(t, int32(3), final.Attempts)
	mu.Lock()
	require.Len(t, messageIDs, 1, "all retries must reuse the same webhook-id")
	require.Len(t, bodies, 1, "all retries must send the identical payload")
	mu.Unlock()
}

// TestQueueTimeoutIsRetryable covers the timeout case: an attempt that exceeds
// SendTimeout counts as retryable and eventually dead-letters when the
// receiver never responds.
func TestQueueTimeoutIsRetryable(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	config := testConfig()
	config.MaxAttempts = 2
	config.SendTimeout = 100 * time.Millisecond
	config.ClaimTTL = 5 * time.Second
	q := startQueue(t, st, WithConfig(config))
	delivery := enqueueSimple(t, q, "hook", "ev-timeout", server.URL)

	final := waitForStatus(t, st, delivery.ID, store.WebhookDeliveryDeadLetter)
	require.Equal(t, int32(2), final.Attempts)
	require.Equal(t, int32(2), attempts.Load())
	require.NotEmpty(t, final.LastError)
}

// TestEnqueueDeduplicatesRepeatedEvents verifies the idempotency contract:
// the same event for the same webhook produces one row and one delivery,
// while a distinct event is enqueued separately.
func TestEnqueueDeduplicatesRepeatedEvents(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	q := startQueue(t, st, WithConfig(testConfig()))

	first, inserted, err := q.Enqueue(context.Background(), &EnqueueRequest{
		EventKey:  "same-event",
		WebhookID: "hook",
		URL:       server.URL,
		Payload:   map[string]any{"n": 1},
	})
	require.NoError(t, err)
	require.True(t, inserted)

	second, inserted, err := q.Enqueue(context.Background(), &EnqueueRequest{
		EventKey:  "same-event",
		WebhookID: "hook",
		URL:       server.URL,
		Payload:   map[string]any{"n": 1},
	})
	require.NoError(t, err)
	require.False(t, inserted, "the duplicate event must collapse onto the existing delivery")
	require.Equal(t, first.ID, second.ID)

	other := enqueueSimple(t, q, "hook", "different-event", server.URL)
	waitForStatus(t, st, first.ID, store.WebhookDeliverySucceeded)
	waitForStatus(t, st, other.ID, store.WebhookDeliverySucceeded)
	require.Equal(t, int32(2), attempts.Load())
}

// TestQueueRecoversAcrossRestart covers two restart scenarios: pending rows
// are picked up by a freshly started queue, and a stale PROCESSING claim left
// by a crashed worker is reclaimed and redelivered.
func TestQueueRecoversAcrossRestart(t *testing.T) {
	allowPrivateWebhooks(t)

	t.Run("pending row is delivered after restart", func(t *testing.T) {
		st := newTestStore(t)
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			_, _ = w.Write([]byte(`{"code":0}`))
		}))
		defer server.Close()

		// Enqueue with a queue whose workers never start, simulating an
		// event persisted immediately before shutdown.
		before := New(st, WithConfig(testConfig()))
		delivery := enqueueSimple(t, before, "hook", "ev-restart-pending", server.URL)

		after := startQueue(t, st, WithConfig(testConfig()))
		_ = after
		waitForStatus(t, st, delivery.ID, store.WebhookDeliverySucceeded)
		require.Equal(t, int32(1), attempts.Load())
	})

	t.Run("stale processing claim is reclaimed", func(t *testing.T) {
		st := newTestStore(t)
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			_, _ = w.Write([]byte(`{"code":0}`))
		}))
		defer server.Close()

		crashTime := time.Unix(1_700_000_000, 0)
		crashed := New(st, WithConfig(testConfig()), WithClock(func() time.Time { return crashTime }))
		delivery := enqueueSimple(t, crashed, "hook", "ev-crashed-claim", server.URL)

		// Simulate a worker claiming the row and dying without recording an outcome.
		claimed, err := st.ClaimDueWebhookDeliveries(context.Background(), 8, crashTime.Unix(), crashTime.Unix(), "dead-worker")
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, store.WebhookDeliveryProcessing, claimed[0].Status)
		require.Equal(t, int32(1), claimed[0].Attempts)

		// Three hours later a new process starts; its ClaimTTL makes the old claim stale.
		restartedClock := func() time.Time { return crashTime.Add(3 * time.Hour) }
		config := testConfig()
		config.ClaimTTL = time.Hour
		startQueue(t, st, WithConfig(config), WithClock(restartedClock))

		final := waitForStatus(t, st, delivery.ID, store.WebhookDeliverySucceeded)
		require.Equal(t, int32(2), final.Attempts, "the reclaim counts as a second attempt")
		require.Equal(t, int32(1), attempts.Load(), "at-least-once: the redelivery is the one HTTP request that completed")
	})
}

// TestQueueConcurrentWorkers verifies many deliveries under multiple workers
// are each sent exactly once despite contention.
func TestQueueConcurrentWorkers(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	const total = 50
	var mu sync.Mutex
	seen := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen[string(body)]++
		mu.Unlock()
		time.Sleep(time.Duration(1+(hash(body)%5)) * time.Millisecond)
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	config := testConfig()
	config.WorkerCount = 8
	config.BatchSize = 4
	q := startQueue(t, st, WithConfig(config))

	ids := make([]int64, total)
	for i := range total {
		delivery, _, err := q.Enqueue(context.Background(), &EnqueueRequest{
			EventKey:  fmt.Sprintf("event-%d", i),
			WebhookID: "hook",
			URL:       server.URL,
			Payload:   map[string]any{"index": i},
		})
		require.NoError(t, err)
		ids[i] = delivery.ID
	}

	require.Eventually(t, func() bool {
		for _, id := range ids {
			if getDelivery(t, st, id).Status != store.WebhookDeliverySucceeded {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, total, "each distinct event must be delivered")
	for body, count := range seen {
		require.Equal(t, 1, count, "delivery duplicated for %s", body)
	}
}

// TestReplayDeadLetter verifies read-only replay: the original dead-letter row
// is untouched while a fresh delivery is enqueued, retried independently, and
// succeeds once the receiver recovers.
func TestReplayDeadLetter(t *testing.T) {
	allowPrivateWebhooks(t)
	st := newTestStore(t)

	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if healthy.Load() {
			_, _ = w.Write([]byte(`{"code":0}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	config := testConfig()
	config.MaxAttempts = 2
	q := startQueue(t, st, WithConfig(config))
	delivery := enqueueSimple(t, q, "hook", "ev-replay", server.URL)

	dead := waitForStatus(t, st, delivery.ID, store.WebhookDeliveryDeadLetter)

	// Replay is rejected for non-dead-letter rows and unknown ids.
	other := enqueueSimple(t, q, "hook", "ev-other", server.URL)
	_, err := q.ReplayDeadLetter(context.Background(), other.ID)
	require.ErrorIs(t, err, ErrDeliveryNotDeadLetter)
	_, err = q.ReplayDeadLetter(context.Background(), 99999)
	require.ErrorIs(t, err, ErrDeliveryNotFound)

	healthy.Store(true)
	replay, err := q.ReplayDeadLetter(context.Background(), dead.ID)
	require.NoError(t, err)
	require.NotEqual(t, dead.ID, replay.ID)
	require.Equal(t, store.WebhookDeliveryPending, replay.Status)
	require.Equal(t, dead.URL, replay.URL)
	require.Equal(t, dead.WebhookID, replay.WebhookID)
	require.Equal(t, dead.SigningSecret, replay.SigningSecret)
	require.Equal(t, dead.Payload, replay.Payload)
	require.Equal(t, dead.MaxAttempts, replay.MaxAttempts)

	waitForStatus(t, st, replay.ID, store.WebhookDeliverySucceeded)

	// The original audit record stays a dead letter.
	original := getDelivery(t, st, dead.ID)
	require.Equal(t, store.WebhookDeliveryDeadLetter, original.Status)
	require.Equal(t, int32(2), original.Attempts)
}

// TestRetryDelayUsesExponentialBackoffWithJitter checks the backoff ceiling
// doubles per attempt up to MaxBackoff and jitter never escapes it.
func TestRetryDelayUsesExponentialBackoffWithJitter(t *testing.T) {
	q := New(nil, WithConfig(Config{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}))
	for range 100 {
		first := q.retryDelay(1)
		require.LessOrEqual(t, first, 10*time.Millisecond)
		second := q.retryDelay(2)
		require.LessOrEqual(t, second, 20*time.Millisecond)
		third := q.retryDelay(3)
		require.LessOrEqual(t, third, 40*time.Millisecond)
		capped := q.retryDelay(10)
		require.LessOrEqual(t, capped, 100*time.Millisecond)
	}
}

func hash(body []byte) uint64 {
	var h uint64 = 1469598103934665603
	for _, b := range body {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}
