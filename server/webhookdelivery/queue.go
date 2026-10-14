// Package webhookdelivery runs the persistent outbound webhook delivery queue.
//
// Events are persisted as webhook_delivery rows keyed on an idempotency key
// before the triggering request returns, so they survive process restarts.
// Workers claim due rows, send them through the SSRF-protected internal/webhook
// sender, and record the outcome: success, permanent failure, exponential
// backoff for retryable failures, and a dead-letter terminal state once the
// attempt budget is exhausted. Administrators observe counts per webhook and
// replay dead-letter entries, which enqueues a fresh delivery without mutating
// the original audit record.
package webhookdelivery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/usememos/memos/internal/webhook"
	"github.com/usememos/memos/store"
)

// Sender performs one signed HTTP delivery attempt. It mirrors webhook.Deliver
// so tests can inject a fake without touching the network.
type Sender func(ctx context.Context, request *webhook.Request) (*webhook.AttemptResult, bool, error)

// Config controls queue persistence and retry behavior.
type Config struct {
	// WorkerCount is the number of concurrent worker goroutines.
	WorkerCount int
	// PollInterval bounds how long an idle queue waits before polling again.
	PollInterval time.Duration
	// BatchSize caps deliveries claimed by one worker in one poll.
	BatchSize int
	// MaxAttempts is the default attempt budget for a new delivery.
	MaxAttempts int32
	// InitialBackoff is the full-jitter backoff ceiling after the first failure.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponentially grown backoff ceiling.
	MaxBackoff time.Duration
	// ClaimTTL is how long a PROCESSING claim survives before another worker
	// assumes the sender crashed and reclaims the delivery. It must exceed the
	// send timeout.
	ClaimTTL time.Duration
	// SendTimeout bounds one HTTP attempt.
	SendTimeout time.Duration
}

// DefaultConfig returns the production retry and worker configuration.
func DefaultConfig() Config {
	return Config{
		WorkerCount:    4,
		PollInterval:   time.Second,
		BatchSize:      16,
		MaxAttempts:    5,
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Minute,
		ClaimTTL:       2 * time.Minute,
		SendTimeout:    30 * time.Second,
	}
}

// Option customizes a Queue at construction.
type Option func(*Queue)

// WithConfig replaces the whole configuration.
func WithConfig(config Config) Option {
	return func(q *Queue) {
		q.config = config
	}
}

// WithSender replaces the HTTP sender, primarily for tests.
func WithSender(sender Sender) Option {
	return func(q *Queue) {
		q.sender = sender
	}
}

// WithClock replaces the time source, primarily for tests.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) {
		q.now = now
	}
}

// EnqueueRequest is one event destined for a user webhook.
type EnqueueRequest struct {
	// EventKey is the deterministic identity of the logical event, for example
	// "memos.memo.created:users/u/memos/m:<uid>:<create_ts>". Combined with the
	// webhook ID it forms the idempotency key. When empty a random key is used.
	EventKey string
	// WebhookID is the owner's user-setting webhook identifier.
	WebhookID string
	// UserID is the webhook owner.
	UserID int32
	// URL is the destination endpoint.
	URL string
	// Label identifies the event type in logs.
	Label string
	// SigningSecret signs the Standard Webhooks headers when non-empty.
	SigningSecret string
	// Payload is JSON-encoded once, at enqueue time, so every retry and restart
	// sends byte-identical bodies.
	Payload any
	// MaxAttempts optionally overrides the queue default for this delivery.
	MaxAttempts int32
}

// Sentinel errors for dead-letter replay.
var (
	// ErrDeliveryNotFound reports that no delivery exists for the id.
	ErrDeliveryNotFound = errors.New("webhook delivery not found")
	// ErrDeliveryNotDeadLetter reports that replay was requested for a delivery
	// that has not reached the dead-letter terminal state.
	ErrDeliveryNotDeadLetter = errors.New("webhook delivery is not in the dead letter state")
)

// Queue is the persistent outbound webhook delivery queue.
type Queue struct {
	store  *store.Store
	sender Sender
	config Config
	now    func() time.Time

	// notify wakes an idle worker immediately after an enqueue.
	notify chan struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
}

// New creates a queue backed by the given store.
func New(st *store.Store, opts ...Option) *Queue {
	queue := &Queue{
		store:  st,
		sender: webhook.Deliver,
		config: DefaultConfig(),
		now:    time.Now,
		notify: make(chan struct{}, 1),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(queue)
		}
	}
	return queue
}

// deriveIdempotencyKey hashes the webhook and event identities so the same
// event delivered to two different webhooks produces two rows, while repeating
// the same (webhook, event) pair collapses to one.
func deriveIdempotencyKey(webhookID, eventKey string) string {
	sum := sha256.Sum256([]byte(webhookID + "\x1f" + eventKey))
	return hex.EncodeToString(sum[:])
}

// deriveMessageID derives a stable Standard Webhooks message id from the
// idempotency key. The same id is sent on every retry so receivers can
// deduplicate redeliveries.
func deriveMessageID(idempotencyKey string) string {
	sum := sha256.Sum256([]byte("webhook-delivery:" + idempotencyKey))
	return "msg_dlv_" + hex.EncodeToString(sum[:16])
}

// randomEventKey mints a non-deduplicating key for callers that did not provide
// a deterministic event identity.
func randomEventKey() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "anon-" + hex.EncodeToString(buf)
}

// Enqueue persists a delivery. When the same (webhook, event) was already
// enqueued, the existing row is returned and inserted is false; no duplicate
// delivery is created.
func (q *Queue) Enqueue(ctx context.Context, request *EnqueueRequest) (*store.WebhookDelivery, bool, error) {
	if request == nil {
		return nil, false, errors.New("webhook delivery enqueue request is nil")
	}
	if request.URL == "" {
		return nil, false, errors.New("webhook delivery URL is required")
	}
	body, err := json.Marshal(request.Payload)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to marshal webhook payload")
	}

	eventKey := request.EventKey
	if eventKey == "" {
		eventKey = randomEventKey()
	}
	maxAttempts := request.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = q.config.MaxAttempts
	}

	idempotencyKey := deriveIdempotencyKey(request.WebhookID, eventKey)
	delivery, inserted, err := q.store.CreateWebhookDelivery(ctx, &store.CreateWebhookDelivery{
		IdempotencyKey: idempotencyKey,
		WebhookID:      request.WebhookID,
		UserID:         request.UserID,
		URL:            request.URL,
		Label:          request.Label,
		SigningSecret:  request.SigningSecret,
		Payload:        body,
		MessageID:      deriveMessageID(idempotencyKey),
		MaxAttempts:    maxAttempts,
	})
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to persist webhook delivery")
	}
	if inserted {
		q.wakeWorkers()
	}
	return delivery, inserted, nil
}

// ReplayDeadLetter enqueues a fresh delivery copied from a dead-lettered one.
// The original row is left untouched as an audit record; the replay gets its
// own idempotency key, message id, and attempt budget.
func (q *Queue) ReplayDeadLetter(ctx context.Context, id int64) (*store.WebhookDelivery, error) {
	rows, err := q.store.ListWebhookDeliveries(ctx, &store.FindWebhookDelivery{ID: &id})
	if err != nil {
		return nil, errors.Wrap(err, "failed to load webhook delivery for replay")
	}
	if len(rows) != 1 {
		return nil, ErrDeliveryNotFound
	}
	original := rows[0]
	if original.Status != store.WebhookDeliveryDeadLetter {
		return nil, ErrDeliveryNotDeadLetter
	}

	// The nanosecond suffix makes each explicit replay a distinct logical
	// delivery instead of collapsing onto the original idempotency key.
	eventKey := "replay:" + original.IdempotencyKey + ":" + strconv.FormatInt(q.now().UnixNano(), 10)
	delivery, _, err := q.Enqueue(ctx, &EnqueueRequest{
		EventKey:      eventKey,
		WebhookID:     original.WebhookID,
		UserID:        original.UserID,
		URL:           original.URL,
		Label:         original.Label,
		SigningSecret: original.SigningSecret,
		Payload:       json.RawMessage(original.Payload),
		MaxAttempts:   original.MaxAttempts,
	})
	if err != nil {
		return nil, err
	}
	return delivery, nil
}

// wakeWorkers signals one waiting worker without blocking when nobody is
// waiting or the notification slot is still full.
func (q *Queue) wakeWorkers() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// retryDelay returns the wait before another attempt using exponential backoff
// with full jitter: the ceiling doubles per failed attempt up to MaxBackoff,
// and the actual delay is uniformly distributed within it.
func (q *Queue) retryDelay(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := q.config.InitialBackoff
	for i := int32(1); i < attempt; i++ {
		if ceiling >= q.config.MaxBackoff {
			ceiling = q.config.MaxBackoff
			break
		}
		ceiling *= 2
		if ceiling > q.config.MaxBackoff || ceiling <= 0 {
			ceiling = q.config.MaxBackoff
		}
	}
	if ceiling <= 0 {
		return 0
	}
	// crypto/rand keeps jitter uncorrelated across workers; fall through to the
	// ceiling if the system reader fails rather than sleeping zero forever.
	n, err := rand.Int(rand.Reader, big.NewInt(int64(ceiling)+1))
	if err != nil || n == nil {
		return ceiling
	}
	return time.Duration(n.Int64())
}
