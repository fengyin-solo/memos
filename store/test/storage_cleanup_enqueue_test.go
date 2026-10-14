package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func s3AttachmentPayload(key, storageID string) *storepb.AttachmentPayload {
	return &storepb.AttachmentPayload{Payload: &storepb.AttachmentPayload_S3Object_{
		S3Object: &storepb.AttachmentPayload_S3Object{Key: key, StorageId: storageID},
	}}
}

func cleanupTasksFor(t *testing.T, ts *store.Store) map[string]*store.StorageCleanupTask {
	t.Helper()
	byKey := map[string]*store.StorageCleanupTask{}
	for _, task := range listAllCleanupTasks(t, ts) {
		byKey[task.StorageType.String()+"/"+task.StorageID+"/"+task.StorageKey] = task
	}
	return byKey
}

// TestDeleteMemoEnqueuesStorageCleanupTasks verifies the memo delete
// transaction enqueues one PENDING task per external object and skips
// database blobs.
func TestDeleteMemoEnqueuesStorageCleanupTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	user, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-memo-user", Role: store.RoleUser})
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-memo", CreatorID: user.ID, Content: "body", Visibility: store.Private})
	require.NoError(t, err)
	local, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-memo-local", CreatorID: user.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/a.txt",
	})
	require.NoError(t, err)
	remote, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-memo-s3", CreatorID: user.ID, Filename: "b.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_S3, Payload: s3AttachmentPayload("attachments/b.txt", "s3-primary"),
	})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-memo-db", CreatorID: user.ID, Filename: "c.bin", Type: "application/octet-stream", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_ATTACHMENT_STORAGE_TYPE_UNSPECIFIED, Blob: []byte("blob"),
	})
	require.NoError(t, err)

	_, err = ts.DeleteMemoWithPolicy(ctx, &store.DeleteMemoWithPolicy{MemoID: memo.ID, ActorUserID: user.ID})
	require.NoError(t, err)

	tasks := cleanupTasksFor(t, ts)
	require.Len(t, tasks, 3, "local, S3 objects and the blob's derived caches are enqueued")
	cacheTask := tasks["ATTACHMENT_STORAGE_TYPE_UNSPECIFIED//cleanup-memo-db"]
	require.NotNil(t, cacheTask, "blob attachments get a cache-only task keyed by uid")
	require.Equal(t, "cleanup-memo-db", cacheTask.StorageKey)
	localTask := tasks["LOCAL/local/assets/a.txt"]
	require.NotNil(t, localTask)
	require.Equal(t, store.StorageCleanupSourceMemo, localTask.Source)
	require.Equal(t, store.StorageCleanupStatusPending, localTask.Status)
	require.Equal(t, local.UID, localTask.AttachmentUID)
	require.NotZero(t, localTask.CreatedTs)

	remoteTask := tasks["S3/s3-primary/attachments/b.txt"]
	require.NotNil(t, remoteTask)
	require.Equal(t, store.StorageCleanupSourceMemo, remoteTask.Source)
	require.Equal(t, remote.UID, remoteTask.AttachmentUID)
	require.Contains(t, remoteTask.Payload, "attachments/b.txt")
}

// TestDeleteSpaceEnqueuesStorageCleanupTasks verifies Space deletions tag
// tasks with the SPACE source.
func TestDeleteSpaceEnqueuesStorageCleanupTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	admin, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-space-admin", Role: store.RoleUser})
	require.NoError(t, err)
	space, err := ts.CreateSpace(ctx, &store.Space{UID: "cleanup-space", Title: "Cleanup"}, admin.ID)
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-space-memo", CreatorID: admin.ID, Content: "x", Visibility: store.SpaceAudience, SpaceID: &space.ID})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-space-attachment", CreatorID: admin.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_S3, Payload: s3AttachmentPayload("attachments/space-a.txt", "s3-primary"),
	})
	require.NoError(t, err)

	_, err = ts.DeleteSpace(ctx, &store.DeleteSpace{ID: space.ID, ActorUserID: admin.ID})
	require.NoError(t, err)

	tasks := cleanupTasksFor(t, ts)
	require.Len(t, tasks, 1)
	task := tasks["S3/s3-primary/attachments/space-a.txt"]
	require.NotNil(t, task)
	require.Equal(t, store.StorageCleanupSourceSpace, task.Source)
	require.Equal(t, store.StorageCleanupStatusPending, task.Status)
}

// TestDeleteAttachmentsWithPolicyEnqueuesStorageCleanupTasks verifies direct
// attachment deletion tags tasks with ATTACHMENT and that failed
// authorization leaves no ledger rows behind.
func TestDeleteAttachmentsWithPolicyEnqueuesStorageCleanupTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	actor, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-attachment-actor", Role: store.RoleUser})
	require.NoError(t, err)
	other, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-attachment-other", Role: store.RoleUser})
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-attachment-memo", CreatorID: actor.ID, Content: "bound", Visibility: store.Private})
	require.NoError(t, err)
	bound, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-attachment-bound", CreatorID: actor.ID, Filename: "bound.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/bound.txt",
	})
	require.NoError(t, err)
	unbound, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-attachment-unbound", CreatorID: actor.ID, Filename: "unbound.txt", Type: "text/plain",
		StorageType: storepb.AttachmentStorageType_S3, Payload: s3AttachmentPayload("attachments/unbound.txt", "s3-primary"),
	})
	require.NoError(t, err)
	foreign, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-attachment-foreign", CreatorID: other.ID, Filename: "foreign.txt", Type: "text/plain",
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/foreign.txt",
	})
	require.NoError(t, err)

	require.NoError(t, ts.DeleteAttachmentsWithPolicy(ctx, attachmentDeletionPolicy(actor.ID, memo), []int32{bound.ID, unbound.ID}))
	tasks := cleanupTasksFor(t, ts)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		require.Equal(t, store.StorageCleanupSourceAttachment, task.Source)
		require.Equal(t, store.StorageCleanupStatusPending, task.Status)
	}

	// Unauthorized deletion rolls back the transaction, including its ledger inserts.
	err = ts.DeleteAttachmentsWithPolicy(ctx, attachmentDeletionPolicy(actor.ID), []int32{foreign.ID})
	require.Error(t, err)
	stored, err := ts.GetAttachment(ctx, &store.FindAttachment{ID: &foreign.ID})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Len(t, cleanupTasksFor(t, ts), 2, "rolled back deletions must not leave ledger rows")
}

// TestDeleteUserEnqueuesStorageCleanupTasks verifies user deletion tags
// tasks with USER and that the pre-commit failpoint rolls ledger inserts back.
func TestDeleteUserEnqueuesStorageCleanupTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	user, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-user", Role: store.RoleUser})
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-user-memo", CreatorID: user.ID, Content: "x", Visibility: store.Private})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-user-attachment", CreatorID: user.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/user-a.txt",
	})
	require.NoError(t, err)

	_, err = ts.DeleteUser(ctx, &store.DeleteUser{ID: user.ID})
	require.NoError(t, err)
	tasks := cleanupTasksFor(t, ts)
	require.Len(t, tasks, 1)
	require.Equal(t, store.StorageCleanupSourceUser, tasks["LOCAL/local/assets/user-a.txt"].Source)

	// A second user whose transaction is forced to roll back leaves no rows.
	other, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-user-rollback", Role: store.RoleUser})
	require.NoError(t, err)
	otherMemo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-user-rollback-memo", CreatorID: other.ID, Content: "x", Visibility: store.Private})
	require.NoError(t, err)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-user-rollback-attachment", CreatorID: other.ID, Filename: "b.txt", Type: "text/plain", MemoID: &otherMemo.ID,
		StorageType: storepb.AttachmentStorageType_LOCAL, Reference: "assets/user-b.txt",
	})
	require.NoError(t, err)
	_, err = ts.DeleteUser(store.WithDeleteUserFailpoint(ctx, store.DeleteUserFailpointBeforeCommit), &store.DeleteUser{ID: other.ID})
	require.Error(t, err)
	require.Len(t, cleanupTasksFor(t, ts), 1, "rolled back user deletion must not leave ledger rows")
}

// TestApplyMemoMutationEnqueuesRemovedAttachmentCleanup verifies memo edits
// that remove attachments enqueue ATTACHMENT tasks in the same transaction.
func TestApplyMemoMutationEnqueuesRemovedAttachmentCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	owner, err := ts.CreateUser(ctx, &store.User{Username: "cleanup-mutation-owner", Role: store.RoleUser})
	require.NoError(t, err)
	memo, err := ts.CreateMemo(ctx, &store.Memo{UID: "cleanup-mutation-memo", CreatorID: owner.ID, Content: "before", Visibility: store.Private})
	require.NoError(t, err)
	removed, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID: "cleanup-mutation-removed", CreatorID: owner.ID, Filename: "a.txt", Type: "text/plain", MemoID: &memo.ID,
		StorageType: storepb.AttachmentStorageType_S3, Payload: s3AttachmentPayload("attachments/mutation-a.txt", "s3-primary"),
	})
	require.NoError(t, err)
	updatedContent := "after"
	mutation := &store.MemoMutation{
		MemoID: memo.ID, MemoCreatorID: owner.ID, ExpectedMemoContent: memo.Content,
		MemoUpdate:                &store.UpdateMemo{ID: memo.ID, Content: &updatedContent},
		RemovedAttachmentIDs:      []int32{removed.ID},
		RequiredAttachmentIDs:     []int32{},
		ReplaceReferenceRelations: true,
	}
	require.NoError(t, ts.ApplyMemoMutation(ctx, mutation))

	tasks := cleanupTasksFor(t, ts)
	require.Len(t, tasks, 1)
	task := tasks["S3/s3-primary/attachments/mutation-a.txt"]
	require.NotNil(t, task)
	require.Equal(t, store.StorageCleanupSourceAttachment, task.Source)
}
