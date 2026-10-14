package store

import (
	"context"

	"github.com/pkg/errors"
)

// Per-user attachment quota accounting lives in the user_attachment_quota
// table (used_size, per-user override). Every accounting change happens in
// the same transaction as the attachment row it corresponds to, so the two
// can never diverge on a committed state.
//
// Lock ordering rules shared by every driver:
//
//   - Attachment creation first locks the user's quota row (the conditional
//     usage UPDATE) and then inserts the new attachment row.
//   - Attachment deletion first reads/locks the attachment rows being deleted
//     and then updates the quota row within the same transaction.
//
// Creators never wait for another transaction's attachment row locks, and
// deleters never wait for a creator's insert, so the two orders cannot form a
// deadlock cycle.
var (
	// ErrAttachmentQuotaExceeded is returned by a driver when the conditional
	// usage update in an attachment-creation transaction rejects a write that
	// would cross the user's effective quota.
	ErrAttachmentQuotaExceeded = errors.New("attachment quota exceeded")
)

// UserAttachmentQuota is one user's quota accounting state.
type UserAttachmentQuota struct {
	// UserID is the quota owner.
	UserID int32
	// UsedSize is the persisted attachment usage in bytes. When the backing
	// quota row is missing, drivers synthesize the value with a live SUM over
	// the attachment table.
	UsedSize int64
	// LimitSize is the per-user override in bytes when set; nil means the
	// instance default quota applies. An explicit zero blocks all uploads.
	LimitSize *int64
}

// FindUserAttachmentQuota selects quota rows for listing and point reads.
type FindUserAttachmentQuota struct {
	// UserIDList filters to the given users. Empty means every user.
	UserIDList []int32
	Limit      *int
	Offset     *int
}

// GetUserAttachmentQuota returns one user's quota state. A missing quota row
// is synthesized from live attachment usage with no override. It returns
// (nil, nil) when the user does not exist.
func (s *Store) GetUserAttachmentQuota(ctx context.Context, userID int32) (*UserAttachmentQuota, error) {
	quotas, err := s.driver.GetUserAttachmentQuotas(ctx, &FindUserAttachmentQuota{UserIDList: []int32{userID}})
	if err != nil {
		return nil, err
	}
	if len(quotas) == 0 {
		return nil, nil
	}
	return quotas[0], nil
}

// ListUserAttachmentQuotas returns quota states matching find.
func (s *Store) ListUserAttachmentQuotas(ctx context.Context, find *FindUserAttachmentQuota) ([]*UserAttachmentQuota, error) {
	return s.driver.GetUserAttachmentQuotas(ctx, find)
}

// UpsertUserAttachmentQuotaLimit sets the per-user quota override in bytes or
// clears it (nil) so the instance default applies again.
func (s *Store) UpsertUserAttachmentQuotaLimit(ctx context.Context, userID int32, limit *int64) error {
	if userID <= 0 {
		return errors.New("user id must be positive")
	}
	if limit != nil && *limit < 0 {
		return errors.New("quota override must not be negative")
	}
	return s.driver.UpsertUserAttachmentQuotaLimit(ctx, userID, limit)
}
