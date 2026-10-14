package v1

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/usememos/memos/core/attachmentquota"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	"github.com/usememos/memos/store"
)

const (
	// reasonAttachmentQuotaExceeded marks a ResourceExhausted refusal caused by
	// the per-user attachment byte quota, distinct from rate limiting.
	reasonAttachmentQuotaExceeded = "ATTACHMENT_QUOTA_EXCEEDED"

	attachmentQuotaExceededMessage = "attachment quota exceeded: not enough storage space"
)

// attachmentQuotaState is the resolved quota view for one user at one request.
type attachmentQuotaState struct {
	usedBytes       int64
	limitBytes      int64
	enabled         bool
	limitOverridden bool
}

// limitForCreate returns the effective limit to enforce inside the attachment
// insert transaction, or nil when enforcement is disabled. The usage counter
// is maintained either way.
func (q *attachmentQuotaState) limitForCreate() *int64 {
	if !q.enabled {
		return nil
	}
	limit := q.limitBytes
	return &limit
}

// remainingBytes reports -1 when the quota is disabled, matching the API.
func (q *attachmentQuotaState) remainingBytes() int64 {
	if !q.enabled {
		return -1
	}
	return attachmentquota.Remaining(q.usedBytes, q.limitBytes)
}

// getAttachmentQuotaState resolves instance default, per-user override and
// current usage for one user.
func (s *APIV1Service) getAttachmentQuotaState(ctx context.Context, userID int32) (*attachmentQuotaState, error) {
	setting, err := s.Store.GetInstanceStorageSetting(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get instance storage setting: %v", err)
	}
	quota, err := s.Store.GetUserAttachmentQuota(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get attachment quota: %v", err)
	}
	state := &attachmentQuotaState{}
	var override *int64
	if quota != nil {
		state.usedBytes = quota.UsedSize
		override = quota.LimitSize
		state.limitOverridden = override != nil
	} else {
		// Missing quota row (legacy or partly migrated database): fall back to
		// the live attachment sum.
		used, err := s.Store.GetAttachmentStorageUsage(ctx, userID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get attachment usage: %v", err)
		}
		state.usedBytes = used
	}
	state.limitBytes, state.enabled = attachmentquota.EffectiveLimit(setting.AttachmentQuotaLimitBytes, override)
	return state, nil
}

// checkAttachmentQuotaBeforeWrite rejects a write that would cross the quota
// before any byte is stored. It runs at both upload entry points.
func checkAttachmentQuotaBeforeWrite(state *attachmentQuotaState, requestedBytes int64) error {
	if !state.enabled {
		return nil
	}
	if attachmentquota.Fits(state.usedBytes, state.limitBytes, requestedBytes) {
		return nil
	}
	return state.quotaExceededError(requestedBytes)
}

// mapAttachmentQuotaCreateError turns the store's authoritative rejection into
// the structured API error; other errors pass through wrapped.
func mapAttachmentQuotaCreateError(ctx context.Context, s *APIV1Service, userID int32, requestedBytes int64, err error) error {
	if !errors.Is(err, store.ErrAttachmentQuotaExceeded) {
		return mapMemoWriteError(err, "failed to create attachment")
	}
	state, stateErr := s.getAttachmentQuotaState(ctx, userID)
	if stateErr != nil {
		return stateErr
	}
	return state.quotaExceededError(requestedBytes)
}

// quotaExceededError builds the structured ResourceExhausted error with the
// AttachmentQuotaDetails and ErrorInfo rich details.
func (q *attachmentQuotaState) quotaExceededError(requestedBytes int64) error {
	st := status.New(codes.ResourceExhausted, attachmentQuotaExceededMessage)
	limitBytes := int64(-1)
	if q.enabled {
		limitBytes = q.limitBytes
	}
	detailed, err := st.WithDetails(
		&errdetails.ErrorInfo{
			Reason: reasonAttachmentQuotaExceeded,
			Domain: errorInfoDomain,
		},
		&v1pb.AttachmentQuotaDetails{
			UsedBytes:       q.usedBytes,
			LimitBytes:      limitBytes,
			RemainingBytes:  q.remainingBytes(),
			RequestedBytes:  requestedBytes,
			LimitOverridden: q.limitOverridden,
		},
	)
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}
