package v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	corecleanup "github.com/usememos/memos/core/objectcleanup"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	servercleanup "github.com/usememos/memos/server/objectcleanup"
	"github.com/usememos/memos/store"
)

// ReconcileStorageCleanup runs a read-only scan of the external object
// cleanup ledger against actual storage state. Admin only.
func (s *APIV1Service) ReconcileStorageCleanup(ctx context.Context, _ *v1pb.ReconcileStorageCleanupRequest) (*v1pb.ReconcileStorageCleanupResponse, error) {
	user, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if user == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if user.Role != store.RoleAdmin {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}

	report, err := servercleanup.Reconcile(ctx, s.Store, s.Profile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to reconcile storage cleanup: %v", err)
	}
	return convertStorageCleanupReport(report), nil
}

func convertStorageCleanupReport(report *corecleanup.Report) *v1pb.ReconcileStorageCleanupResponse {
	response := &v1pb.ReconcileStorageCleanupResponse{
		GeneratedTime: timestamppb.New(report.GeneratedTime),
		SourceCounts:  make([]*v1pb.ReconcileStorageCleanupResponse_SourceCount, 0, len(report.SourceCounts)),
		OrphanCounts:  make([]*v1pb.ReconcileStorageCleanupResponse_StorageOrphanCount, 0, len(report.OrphanCounts)),
	}
	for _, count := range report.SourceCounts {
		response.SourceCounts = append(response.SourceCounts, &v1pb.ReconcileStorageCleanupResponse_SourceCount{
			Source:          convertStorageCleanupSource(count.Source),
			PendingCount:    count.Pending,
			ProcessingCount: count.Processing,
			SucceededCount:  count.Succeeded,
			FailedCount:     count.Failed,
		})
	}
	for _, orphan := range report.OrphanCounts {
		response.OrphanCounts = append(response.OrphanCounts, &v1pb.ReconcileStorageCleanupResponse_StorageOrphanCount{
			StorageType:          convertStorageCleanupStorageType(orphan.Group.StorageType),
			StorageId:            orphan.Group.StorageID,
			SuspectedOrphanCount: orphan.SuspectedOrphanCount,
			Available:            orphan.Available,
		})
	}
	return response
}

func convertStorageCleanupSource(source store.StorageCleanupSource) v1pb.StorageCleanupSource {
	switch source {
	case store.StorageCleanupSourceAttachment:
		return v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_ATTACHMENT
	case store.StorageCleanupSourceMemo:
		return v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_MEMO
	case store.StorageCleanupSourceSpace:
		return v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_SPACE
	case store.StorageCleanupSourceUser:
		return v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_USER
	default:
		return v1pb.StorageCleanupSource_STORAGE_CLEANUP_SOURCE_UNSPECIFIED
	}
}

func convertStorageCleanupStorageType(storageType storepb.AttachmentStorageType) v1pb.InstanceSetting_StorageType {
	switch storageType {
	case storepb.AttachmentStorageType_LOCAL:
		return v1pb.InstanceSetting_LOCAL
	case storepb.AttachmentStorageType_S3:
		return v1pb.InstanceSetting_S3
	default:
		return v1pb.InstanceSetting_STORAGE_TYPE_UNSPECIFIED
	}
}
