package store

import (
	"context"
)

// WebhookDeliveryStatus is the lifecycle state of a persisted outbound webhook delivery.
type WebhookDeliveryStatus string

const (
	// WebhookDeliveryPending means the delivery is waiting for its first attempt
	// or for its backoff window to elapse after a retryable failure.
	WebhookDeliveryPending WebhookDeliveryStatus = "PENDING"
	// WebhookDeliveryProcessing means a worker has claimed the delivery and is
	// sending it. A crash leaves the row in this state until the claim goes
	// stale and another worker reclaims it.
	WebhookDeliveryProcessing WebhookDeliveryStatus = "PROCESSING"
	// WebhookDeliverySucceeded means the receiver accepted the delivery with a
	// 2xx response and a zero application-level code.
	WebhookDeliverySucceeded WebhookDeliveryStatus = "SUCCEEDED"
	// WebhookDeliveryFailed means the last attempt failed in a way that retrying
	// cannot fix, for example an explicit 4xx response or a rejected signing secret.
	WebhookDeliveryFailed WebhookDeliveryStatus = "FAILED"
	// WebhookDeliveryDeadLetter means all attempts failed with retryable errors
	// and the delivery exhausted its attempt budget. It stays stored for
	// inspection and explicit administrator replay.
	WebhookDeliveryDeadLetter WebhookDeliveryStatus = "DEAD_LETTER"
)

// WebhookDelivery is one persisted outbound webhook delivery attempt chain.
// The signed JSON body is stored verbatim so retries and restarts send the
// exact same bytes and receivers can verify a stable signature.
type WebhookDelivery struct {
	ID        int64
	CreatedTs int64
	UpdatedTs int64

	// IdempotencyKey uniquely identifies the logical event delivery. Enqueuing
	// the same (webhook, event) pair twice never creates a second row.
	IdempotencyKey string
	// WebhookID is the owner's user-setting webhook identifier. It groups
	// deliveries for per-webhook observability.
	WebhookID string
	// UserID is the webhook owner.
	UserID int32

	URL           string
	Label         string
	SigningSecret string
	// Payload is the exact JSON request body delivered to the receiver.
	Payload   []byte
	MessageID string

	Status        WebhookDeliveryStatus
	Attempts      int32
	MaxAttempts   int32
	NextAttemptTs int64
	ClaimedBy     string

	LastStatusCode int32
	LastError      string
	SucceededTs    int64
}

// CreateWebhookDelivery carries the fields for an idempotent delivery insert.
type CreateWebhookDelivery struct {
	IdempotencyKey string
	WebhookID      string
	UserID         int32
	URL            string
	Label          string
	SigningSecret  string
	Payload        []byte
	MessageID      string
	MaxAttempts    int32
}

// FindWebhookDelivery filters delivery queries.
type FindWebhookDelivery struct {
	ID  *int64
	IDs []int64

	IdempotencyKey *string
	WebhookID      *string
	UserID         *int32
	Status         *WebhookDeliveryStatus

	// Pagination.
	Limit  *int
	Offset *int
}

// CompleteWebhookDelivery carries the terminal or retry outcome of one attempt.
type CompleteWebhookDelivery struct {
	ID             int64
	Status         WebhookDeliveryStatus
	Attempts       int32
	NextAttemptTs  int64
	LastStatusCode int32
	LastError      string
	SucceededTs    int64
}

// WebhookDeliveryCount is one grouped count row: deliveries of one status for
// one webhook.
type WebhookDeliveryCount struct {
	WebhookID string
	UserID    int32
	Status    WebhookDeliveryStatus
	Count     int64
}

// CreateWebhookDelivery creates a delivery or returns the existing row for the
// same idempotency key.
func (s *Store) CreateWebhookDelivery(ctx context.Context, create *CreateWebhookDelivery) (*WebhookDelivery, bool, error) {
	return s.driver.CreateWebhookDelivery(ctx, create)
}

// ListWebhookDeliveries lists deliveries matching find.
func (s *Store) ListWebhookDeliveries(ctx context.Context, find *FindWebhookDelivery) ([]*WebhookDelivery, error) {
	return s.driver.ListWebhookDeliveries(ctx, find)
}

// ClaimDueWebhookDeliveries claims due or stale deliveries for a worker.
func (s *Store) ClaimDueWebhookDeliveries(ctx context.Context, limit int, nowTs int64, staleBeforeTs int64, workerID string) ([]*WebhookDelivery, error) {
	return s.driver.ClaimDueWebhookDeliveries(ctx, limit, nowTs, staleBeforeTs, workerID)
}

// CompleteWebhookDelivery persists an attempt outcome.
func (s *Store) CompleteWebhookDelivery(ctx context.Context, complete *CompleteWebhookDelivery) error {
	return s.driver.CompleteWebhookDelivery(ctx, complete)
}

// CountWebhookDeliveriesGrouped returns per-webhook, per-status counts.
func (s *Store) CountWebhookDeliveriesGrouped(ctx context.Context, webhookID *string) ([]*WebhookDeliveryCount, error) {
	return s.driver.CountWebhookDeliveriesGrouped(ctx, webhookID)
}
