package v1

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/usememos/memos/core/attachmentquota"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	"github.com/usememos/memos/store"
)

const attachmentQuotaNameSuffix = "/attachmentQuota"

// parseAttachmentQuotaName parses users/{user}/attachmentQuota and returns
// the user name tokens (users/{user}).
func parseAttachmentQuotaName(name string) (string, error) {
	if !strings.HasPrefix(name, UserNamePrefix) || !strings.HasSuffix(name, attachmentQuotaNameSuffix) {
		return "", status.Errorf(codes.InvalidArgument, "invalid attachment quota name: %q", name)
	}
	userName := name[:len(name)-len(attachmentQuotaNameSuffix)]
	if userName == UserNamePrefix {
		return "", status.Errorf(codes.InvalidArgument, "invalid attachment quota name: %q", name)
	}
	if _, err := parseUsernameFromName(userName); err != nil {
		return "", err
	}
	return userName, nil
}

// buildAttachmentQuota converts the resolved store state to the API resource.
func buildAttachmentQuota(user *store.User, state *attachmentQuotaState) *v1pb.AttachmentQuota {
	limitBytes := int64(-1)
	if state.enabled {
		limitBytes = state.limitBytes
	}
	return &v1pb.AttachmentQuota{
		Name:            BuildUserName(user.Username) + attachmentQuotaNameSuffix,
		UsedBytes:       state.usedBytes,
		LimitBytes:      limitBytes,
		RemainingBytes:  state.remainingBytes(),
		Enabled:         state.enabled,
		LimitOverridden: state.limitOverridden,
	}
}

// GetAttachmentQuota returns the attachment byte quota state of one user.
// Users may read their own quota; instance administrators may read any user.
func (s *APIV1Service) GetAttachmentQuota(ctx context.Context, request *v1pb.GetAttachmentQuotaRequest) (*v1pb.AttachmentQuota, error) {
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, err
	}
	userName, err := parseAttachmentQuotaName(request.Name)
	if err != nil {
		return nil, err
	}
	targetUser, err := ResolveUserByName(ctx, s.Store, userName)
	if err != nil || targetUser == nil {
		return nil, status.Errorf(codes.NotFound, "user not found")
	}
	if currentUser.ID != targetUser.ID && currentUser.Role != store.RoleAdmin {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}
	state, err := s.getAttachmentQuotaState(ctx, targetUser.ID)
	if err != nil {
		return nil, err
	}
	return buildAttachmentQuota(targetUser, state), nil
}

// ListAttachmentQuotas returns the attachment quota states of all users.
// Instance administrators only.
func (s *APIV1Service) ListAttachmentQuotas(ctx context.Context, request *v1pb.ListAttachmentQuotasRequest) (*v1pb.ListAttachmentQuotasResponse, error) {
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, err
	}
	if currentUser.Role != store.RoleAdmin {
		return nil, status.Errorf(codes.PermissionDenied, "only instance administrators can list attachment quotas")
	}
	setting, err := s.Store.GetInstanceStorageSetting(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get instance storage setting: %v", err)
	}
	pageSize := normalizePageSize(request.PageSize)
	offset := 0
	if request.PageToken != "" {
		if _, err := fmt.Sscanf(request.PageToken, "%d", &offset); err != nil || offset < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page token")
		}
	}
	limit := pageSize + 1
	quotas, err := s.Store.ListUserAttachmentQuotas(ctx, &store.FindUserAttachmentQuota{
		Limit:  &limit,
		Offset: &offset,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list attachment quotas: %v", err)
	}
	normal := store.Normal
	users, err := s.Store.ListUsers(ctx, &store.FindUser{
		IDList:    quotaUserIDs(quotas),
		RowStatus: &normal,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list users: %v", err)
	}
	usersByID := make(map[int32]*store.User, len(users))
	for _, user := range users {
		usersByID[user.ID] = user
	}

	response := &v1pb.ListAttachmentQuotasResponse{}
	if len(quotas) == pageSize+1 {
		response.NextPageToken = formatQuotaPageToken(offset + pageSize)
		quotas = quotas[:pageSize]
	}
	response.AttachmentQuotas = make([]*v1pb.AttachmentQuota, 0, len(quotas))
	for _, quota := range quotas {
		user, ok := usersByID[quota.UserID]
		if !ok {
			continue
		}
		effectiveLimit, enabled := attachmentquota.EffectiveLimit(setting.AttachmentQuotaLimitBytes, quota.LimitSize)
		state := &attachmentQuotaState{
			usedBytes:       quota.UsedSize,
			limitBytes:      effectiveLimit,
			enabled:         enabled,
			limitOverridden: quota.LimitSize != nil,
		}
		response.AttachmentQuotas = append(response.AttachmentQuotas, buildAttachmentQuota(user, state))
	}
	return response, nil
}

// UpdateAttachmentQuota sets or clears the per-user attachment quota override.
// Instance administrators only.
func (s *APIV1Service) UpdateAttachmentQuota(ctx context.Context, request *v1pb.UpdateAttachmentQuotaRequest) (*v1pb.AttachmentQuota, error) {
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, err
	}
	if currentUser.Role != store.RoleAdmin {
		return nil, status.Errorf(codes.PermissionDenied, "only instance administrators can update attachment quotas")
	}
	if request.AttachmentQuota == nil || request.UpdateMask == nil {
		return nil, status.Errorf(codes.InvalidArgument, "attachment_quota and update_mask are required")
	}
	userName, err := parseAttachmentQuotaName(request.AttachmentQuota.Name)
	if err != nil {
		return nil, err
	}
	paths := request.UpdateMask.GetPaths()
	if len(paths) != 1 || paths[0] != "limit_bytes" {
		return nil, status.Errorf(codes.InvalidArgument, "update_mask must only contain limit_bytes")
	}
	targetUser, err := ResolveUserByName(ctx, s.Store, userName)
	if err != nil || targetUser == nil {
		return nil, status.Errorf(codes.NotFound, "user not found")
	}
	requestedLimit := request.AttachmentQuota.LimitBytes
	var override *int64
	if requestedLimit >= 0 {
		limit := requestedLimit
		override = &limit
	} else if requestedLimit != -1 {
		return nil, status.Errorf(codes.InvalidArgument, "limit_bytes must be non-negative, or -1 to inherit the instance default")
	}
	if err := s.Store.UpsertUserAttachmentQuotaLimit(ctx, targetUser.ID, override); err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to update attachment quota: %v", err)
	}
	state, err := s.getAttachmentQuotaState(ctx, targetUser.ID)
	if err != nil {
		return nil, err
	}
	return buildAttachmentQuota(targetUser, state), nil
}

func quotaUserIDs(quotas []*store.UserAttachmentQuota) []int32 {
	ids := make([]int32, 0, len(quotas))
	for _, quota := range quotas {
		ids = append(ids, quota.UserID)
	}
	return ids
}

func formatQuotaPageToken(offset int) string {
	return strconv.Itoa(offset)
}
