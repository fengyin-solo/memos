package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
)

func setDefaultAttachmentQuota(t *testing.T, ctx context.Context, ts *TestService, limitBytes int64) {
	t.Helper()
	setting, err := ts.Store.GetInstanceStorageSetting(ctx)
	require.NoError(t, err)
	setting.AttachmentQuotaLimitBytes = limitBytes
	_, err = ts.Store.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key:   storepb.InstanceSettingKey_STORAGE,
		Value: &storepb.InstanceSetting_StorageSetting{StorageSetting: setting},
	})
	require.NoError(t, err)
}

func quotaDetails(t *testing.T, err error) *v1pb.AttachmentQuotaDetails {
	t.Helper()
	st := status.Convert(err)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	for _, detail := range st.Details() {
		if quota, ok := detail.(*v1pb.AttachmentQuotaDetails); ok {
			return quota
		}
	}
	t.Fatalf("missing AttachmentQuotaDetails in error details: %v", st.Details())
	return nil
}

func quotaErrorInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	t.Fatalf("missing ErrorInfo in error details: %v", err)
	return nil
}

func createBytesAttachment(t *testing.T, ctx context.Context, ts *TestService, name string, size int) *v1pb.Attachment {
	t.Helper()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte('a' + i%26)
	}
	attachment, err := ts.Service.CreateAttachment(ctx, &v1pb.CreateAttachmentRequest{
		Attachment: &v1pb.Attachment{
			Filename: name,
			Type:     "application/octet-stream",
			Content:  content,
		},
	})
	require.NoError(t, err)
	return attachment
}

func TestAttachmentQuotaDefaultDisabled(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	user, err := ts.CreateRegularUser(ctx, "quota-disabled")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	quota, err := ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.False(t, quota.Enabled)
	require.EqualValues(t, -1, quota.LimitBytes)
	require.EqualValues(t, -1, quota.RemainingBytes)
	require.EqualValues(t, 0, quota.UsedBytes)

	// Uploads of any size keep working and counting.
	createBytesAttachment(t, userCtx, ts, "big.bin", 500)
	quota, err = ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.False(t, quota.Enabled)
	require.EqualValues(t, 500, quota.UsedBytes)
}

func TestAttachmentQuotaBlocksCreateWithDetails(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	setDefaultAttachmentQuota(t, ctx, ts, 10)
	user, err := ts.CreateRegularUser(ctx, "quota-create")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	first := createBytesAttachment(t, userCtx, ts, "six.bin", 6)
	require.EqualValues(t, 6, first.Size)

	// 6 + 5 > 10: rejected before write with structured details.
	content := make([]byte, 5)
	_, err = ts.Service.CreateAttachment(userCtx, &v1pb.CreateAttachmentRequest{
		Attachment: &v1pb.Attachment{Filename: "five.bin", Type: "application/octet-stream", Content: content},
	})
	require.Error(t, err)
	details := quotaDetails(t, err)
	require.EqualValues(t, 6, details.UsedBytes)
	require.EqualValues(t, 10, details.LimitBytes)
	require.EqualValues(t, 4, details.RemainingBytes)
	require.EqualValues(t, 5, details.RequestedBytes)
	require.False(t, details.LimitOverridden)
	info := quotaErrorInfo(t, err)
	require.Equal(t, "memos.api.v1", info.Domain)
	require.Equal(t, "ATTACHMENT_QUOTA_EXCEEDED", info.Reason)

	// Usage and attachment rows are unchanged.
	quota, err := ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.EqualValues(t, 6, quota.UsedBytes)
	require.EqualValues(t, 4, quota.RemainingBytes)

	// A write that fills the quota exactly succeeds.
	createBytesAttachment(t, userCtx, ts, "four.bin", 4)
}

func TestAttachmentQuotaChunkedUploadStartAndFinalize(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	setDefaultAttachmentQuota(t, ctx, ts, 10)
	user, err := ts.CreateRegularUser(ctx, "quota-chunk")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	// A spec whose total crosses the quota is rejected without an upload session.
	_, err = ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{
		Upload: &v1pb.UploadAttachmentRequest_Spec{Spec: &v1pb.UploadAttachmentSpec{
			Attachment: &v1pb.Attachment{Filename: "too-big.bin", Type: "application/octet-stream"},
			TotalSize: 11,
		}},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	// An in-bounds single-call upload finalizes and is charged once.
	result, err := ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{
		Upload: &v1pb.UploadAttachmentRequest_Spec{Spec: &v1pb.UploadAttachmentSpec{
			Attachment: &v1pb.Attachment{Filename: "ten.bin", Type: "application/octet-stream"},
			TotalSize:  10,
		}},
		Data:        []byte("0123456789"),
		FinishWrite: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, 10, result.Attachment.Size)
	quota, err := ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.EqualValues(t, 10, quota.UsedBytes)

	// Re-finalizing the completed upload returns the same attachment and never
	// charges twice.
	repeated, err := ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{
		Upload: &v1pb.UploadAttachmentRequest_UploadId{UploadId: result.UploadId},
	})
	require.NoError(t, err)
	require.Equal(t, result.Attachment.Name, repeated.Attachment.Name)
	quota, err = ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.EqualValues(t, 10, quota.UsedBytes)

	// Now full: a fresh upload start is rejected at finalization.
	_, err = ts.Service.UploadAttachment(userCtx, &v1pb.UploadAttachmentRequest{
		Upload: &v1pb.UploadAttachmentRequest_Spec{Spec: &v1pb.UploadAttachmentSpec{
			Attachment: &v1pb.Attachment{Filename: "one.bin", Type: "application/octet-stream"},
			TotalSize:  1,
		}},
		Data:        []byte("x"),
		FinishWrite: true,
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestAttachmentQuotaDeleteReturnsBytes(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	setDefaultAttachmentQuota(t, ctx, ts, 100)
	user, err := ts.CreateRegularUser(ctx, "quota-delete")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	attachment := createBytesAttachment(t, userCtx, ts, "seven.bin", 7)
	_, err = ts.Service.DeleteAttachment(userCtx, &v1pb.DeleteAttachmentRequest{Name: attachment.Name})
	require.NoError(t, err)
	quota, err := ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + user.Username + "/attachmentQuota",
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, quota.UsedBytes)
	require.EqualValues(t, 100, quota.RemainingBytes)
}

func TestAttachmentQuotaAdminOverrideAndAuthorization(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	setDefaultAttachmentQuota(t, ctx, ts, 100)
	admin, err := ts.CreateHostUser(ctx, "quota-admin")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)
	user, err := ts.CreateRegularUser(ctx, "quota-overridden")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)
	userQuotaName := "users/" + user.Username + "/attachmentQuota"

	// Self read works; reading another user's quota does not.
	_, err = ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{Name: userQuotaName})
	require.NoError(t, err)
	_, err = ts.Service.GetAttachmentQuota(userCtx, &v1pb.GetAttachmentQuotaRequest{
		Name: "users/" + admin.Username + "/attachmentQuota",
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Regular users cannot list or update quotas.
	_, err = ts.Service.ListAttachmentQuotas(userCtx, &v1pb.ListAttachmentQuotasRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = ts.Service.UpdateAttachmentQuota(userCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: userQuotaName, LimitBytes: 10},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limit_bytes"}},
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Unknown user and invalid inputs are rejected before writing.
	_, err = ts.Service.UpdateAttachmentQuota(adminCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: "users/nobody/attachmentQuota", LimitBytes: 10},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limit_bytes"}},
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = ts.Service.UpdateAttachmentQuota(adminCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: userQuotaName, LimitBytes: -2},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limit_bytes"}},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = ts.Service.UpdateAttachmentQuota(adminCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: userQuotaName, LimitBytes: 10},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"used_bytes"}},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// Explicit zero override blocks every upload.
	updated, err := ts.Service.UpdateAttachmentQuota(adminCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: userQuotaName, LimitBytes: 0},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limit_bytes"}},
	})
	require.NoError(t, err)
	require.True(t, updated.Enabled)
	require.True(t, updated.LimitOverridden)
	require.EqualValues(t, 0, updated.LimitBytes)
	_, err = ts.Service.CreateAttachment(userCtx, &v1pb.CreateAttachmentRequest{
		Attachment: &v1pb.Attachment{Filename: "x.bin", Type: "application/octet-stream", Content: []byte("x")},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.True(t, quotaDetails(t, err).LimitOverridden)

	// Clearing the override (-1) restores the instance default of 100.
	updated, err = ts.Service.UpdateAttachmentQuota(adminCtx, &v1pb.UpdateAttachmentQuotaRequest{
		AttachmentQuota: &v1pb.AttachmentQuota{Name: userQuotaName, LimitBytes: -1},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"limit_bytes"}},
	})
	require.NoError(t, err)
	require.False(t, updated.LimitOverridden)
	require.EqualValues(t, 100, updated.LimitBytes)
	createBytesAttachment(t, userCtx, ts, "x.bin", 1)
}

func TestAttachmentQuotaListForAdmin(t *testing.T) {
	ts := NewTestService(t)
	defer ts.Cleanup()
	ctx := context.Background()
	setDefaultAttachmentQuota(t, ctx, ts, 1000)
	admin, err := ts.CreateHostUser(ctx, "quota-list-admin")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)
	first, err := ts.CreateRegularUser(ctx, "quota-list-first")
	require.NoError(t, err)
	second, err := ts.CreateRegularUser(ctx, "quota-list-second")
	require.NoError(t, err)
	createBytesAttachment(t, ts.CreateUserContext(ctx, first.ID), ts, "a.bin", 7)
	createBytesAttachment(t, ts.CreateUserContext(ctx, second.ID), ts, "b.bin", 9)

	response, err := ts.Service.ListAttachmentQuotas(adminCtx, &v1pb.ListAttachmentQuotasRequest{PageSize: 50})
	require.NoError(t, err)
	require.Empty(t, response.NextPageToken)
	usageByName := map[string]int64{}
	for _, quota := range response.AttachmentQuotas {
		require.True(t, quota.Enabled)
		require.EqualValues(t, 1000, quota.LimitBytes)
		usageByName[quota.Name] = quota.UsedBytes
	}
	require.EqualValues(t, 7, usageByName["users/"+first.Username+"/attachmentQuota"])
	require.EqualValues(t, 9, usageByName["users/"+second.Username+"/attachmentQuota"])
}
