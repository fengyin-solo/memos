package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/store"
)

func createQuotaTestUser(ctx context.Context, t *testing.T, ts *store.Store, name string) *store.User {
	t.Helper()
	user, err := createTestingUserWithRole(ctx, ts, name, store.RoleUser)
	require.NoError(t, err)
	return user
}

func createQuotaAttachment(t *testing.T, ctx context.Context, ts *store.Store, creatorID int32, size int64, limit *int64) *store.Attachment {
	t.Helper()
	attachment, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID:             shortuuid.New(),
		CreatorID:       creatorID,
		Filename:        fmt.Sprintf("%s.bin", shortuuid.New()),
		Type:            "application/octet-stream",
		Size:            size,
		QuotaLimitBytes: limit,
	})
	require.NoError(t, err)
	return attachment
}

func requireQuotaUsed(t *testing.T, ctx context.Context, ts *store.Store, userID int32, want int64) {
	t.Helper()
	quota, err := ts.GetUserAttachmentQuota(ctx, userID)
	require.NoError(t, err)
	require.NotNil(t, quota)
	require.Equal(t, want, quota.UsedSize, "maintained counter")
	usage, err := ts.GetAttachmentStorageUsage(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, want, usage, "live attachment sum")
}

func TestAttachmentQuotaChargesOnCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-charge")

	limit := int64(100)
	createQuotaAttachment(t, ctx, ts, user.ID, 30, &limit)
	requireQuotaUsed(t, ctx, ts, user.ID, 30)

	// A write that fills the remaining quota exactly is allowed.
	createQuotaAttachment(t, ctx, ts, user.ID, 70, &limit)
	requireQuotaUsed(t, ctx, ts, user.ID, 100)

	// With enforcement disabled (nil limit) the counter is still maintained.
	createQuotaAttachment(t, ctx, ts, user.ID, 5, nil)
	requireQuotaUsed(t, ctx, ts, user.ID, 105)

	quota, err := ts.GetUserAttachmentQuota(ctx, user.ID)
	require.NoError(t, err)
	require.Nil(t, quota.LimitSize, "no override was set")
}

func TestAttachmentQuotaRejectsOverLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-over")

	limit := int64(100)
	createQuotaAttachment(t, ctx, ts, user.ID, 60, &limit)
	requireQuotaUsed(t, ctx, ts, user.ID, 60)

	before := countAttachments(t, ctx, ts, user.ID)
	_, err := ts.CreateAttachment(ctx, &store.Attachment{
		UID:             shortuuid.New(),
		CreatorID:       user.ID,
		Filename:        "too-big.bin",
		Type:            "application/octet-stream",
		Size:            41,
		QuotaLimitBytes: &limit,
	})
	require.ErrorIs(t, err, store.ErrAttachmentQuotaExceeded)
	requireQuotaUsed(t, ctx, ts, user.ID, 60)
	require.Equal(t, before, countAttachments(t, ctx, ts, user.ID), "rejected write leaves no attachment row")

	// A write that fills the remaining quota exactly succeeds, then the next
	// one-byte write is rejected.
	createQuotaAttachment(t, ctx, ts, user.ID, 40, &limit)
	requireQuotaUsed(t, ctx, ts, user.ID, 100)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID:             shortuuid.New(),
		CreatorID:       user.ID,
		Filename:        "one-byte.bin",
		Type:            "application/octet-stream",
		Size:            1,
		QuotaLimitBytes: &limit,
	})
	require.ErrorIs(t, err, store.ErrAttachmentQuotaExceeded)

	// An explicit zero limit bans every write, including empty files.
	zero := int64(0)
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID:             shortuuid.New(),
		CreatorID:       user.ID,
		Filename:        "empty.bin",
		Type:            "application/octet-stream",
		Size:            0,
		QuotaLimitBytes: &zero,
	})
	require.ErrorIs(t, err, store.ErrAttachmentQuotaExceeded)
	requireQuotaUsed(t, ctx, ts, user.ID, 100)
}

func TestAttachmentQuotaConcurrentUploads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-concurrent")

	const writers = 16
	const size int64 = 10
	limit := int64(writers / 2 * size) // only half can succeed

	start := make(chan struct{})
	var wg sync.WaitGroup
	var accepted int64
	var acceptedMu sync.Mutex
	errs := make(chan error, writers)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := ts.CreateAttachment(ctx, &store.Attachment{
				UID:             shortuuid.New(),
				CreatorID:       user.ID,
				Filename:        fmt.Sprintf("c-%s.bin", shortuuid.New()),
				Type:            "application/octet-stream",
				Size:            size,
				QuotaLimitBytes: &limit,
			})
			if err == nil {
				acceptedMu.Lock()
				accepted++
				acceptedMu.Unlock()
				return
			}
			if !errors.Is(err, store.ErrAttachmentQuotaExceeded) {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, limit, accepted*size, "accepted writes must not exceed the limit")
	requireQuotaUsed(t, ctx, ts, user.ID, limit)
}

func TestAttachmentQuotaDeductsOnDeletionPaths(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-delete")

	// Direct attachment deletion (also the memo import compensation path).
	a1 := createQuotaAttachment(t, ctx, ts, user.ID, 11, nil)
	a2 := createQuotaAttachment(t, ctx, ts, user.ID, 22, nil)
	requireQuotaUsed(t, ctx, ts, user.ID, 33)
	require.NoError(t, ts.DeleteAttachments(ctx, []*store.Attachment{a1}))
	requireQuotaUsed(t, ctx, ts, user.ID, 22)

	// Deleting an already-missing id must not drive the counter negative.
	require.NoError(t, ts.DeleteAttachments(ctx, []*store.Attachment{a1, a2}))
	requireQuotaUsed(t, ctx, ts, user.ID, 0)

	// Memo edit removing an attachment (ApplyMemoMutation removed IDs).
	memo, err := ts.CreateMemo(ctx, &store.Memo{
		UID:        shortuuid.New(),
		CreatorID:  user.ID,
		Content:    "hello",
		Visibility: store.Private,
	})
	require.NoError(t, err)
	bound := createQuotaAttachment(t, ctx, ts, user.ID, 14, nil)
	now := time.Now().Unix()
	require.NoError(t, ts.ApplyMemoMutation(ctx, &store.MemoMutation{
		MemoID:              memo.ID,
		MemoCreatorID:       user.ID,
		ExpectedMemoContent: memo.Content,
		Bindings: []*store.MemoAttachmentBinding{
			{ID: bound.ID, UID: bound.UID, UpdatedTs: now},
		},
	}))
	requireQuotaUsed(t, ctx, ts, user.ID, 14)
	require.NoError(t, ts.ApplyMemoMutation(ctx, &store.MemoMutation{
		MemoID:               memo.ID,
		MemoCreatorID:        user.ID,
		ExpectedMemoContent:  memo.Content,
		RemovedAttachmentIDs: []int32{bound.ID},
	}))
	requireQuotaUsed(t, ctx, ts, user.ID, 0)

	// Memo deletion cascades to its attachments.
	otherMemo, err := ts.CreateMemo(ctx, &store.Memo{
		UID:        shortuuid.New(),
		CreatorID:  user.ID,
		Content:    "cascade",
		Visibility: store.Private,
	})
	require.NoError(t, err)
	cascaded := createQuotaAttachment(t, ctx, ts, user.ID, 33, nil)
	require.NoError(t, ts.UpdateAttachment(ctx, &store.UpdateAttachment{ID: cascaded.ID, MemoID: &otherMemo.ID}))
	requireQuotaUsed(t, ctx, ts, user.ID, 33)
	_, err = ts.DeleteMemoWithPolicy(ctx, &store.DeleteMemoWithPolicy{MemoID: otherMemo.ID, ActorUserID: user.ID})
	require.NoError(t, err)
	requireQuotaUsed(t, ctx, ts, user.ID, 0)
}

func TestAttachmentQuotaDeductsOnUserDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-user-delete")
	createQuotaAttachment(t, ctx, ts, user.ID, 99, nil)
	require.NotNil(t, t, mustGetQuota(ctx, t, ts, user.ID))

	_, err := ts.DeleteUser(ctx, &store.DeleteUser{ID: user.ID})
	require.NoError(t, err)
	require.Nil(t, mustGetQuota(ctx, t, ts, user.ID))
}

func TestAttachmentQuotaOverrideCRUD(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-override")
	createQuotaAttachment(t, ctx, ts, user.ID, 15, nil)

	// Missing row initially: live usage synthesized, no override.
	quota, err := ts.GetUserAttachmentQuota(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, int64(15), quota.UsedSize)
	require.Nil(t, quota.LimitSize)

	// Set an explicit override; the created row keeps live usage.
	override := int64(500)
	require.NoError(t, ts.UpsertUserAttachmentQuotaLimit(ctx, user.ID, &override))
	quota, err = ts.GetUserAttachmentQuota(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, int64(15), quota.UsedSize)
	require.NotNil(t, quota.LimitSize)
	require.Equal(t, int64(500), *quota.LimitSize)

	// The override is enforced on new writes.
	_, err = ts.CreateAttachment(ctx, &store.Attachment{
		UID:             shortuuid.New(),
		CreatorID:       user.ID,
		Filename:        "over.bin",
		Type:            "application/octet-stream",
		Size:            486,
		QuotaLimitBytes: &override,
	})
	require.ErrorIs(t, err, store.ErrAttachmentQuotaExceeded)

	// Clear the override (inherit default).
	require.NoError(t, ts.UpsertUserAttachmentQuotaLimit(ctx, user.ID, nil))
	quota, err = ts.GetUserAttachmentQuota(ctx, user.ID)
	require.NoError(t, err)
	require.Nil(t, quota.LimitSize)

	// Overrides for unknown users fail.
	missing := int64(10)
	require.Error(t, ts.UpsertUserAttachmentQuotaLimit(ctx, 999999, &missing))
	require.Error(t, ts.UpsertUserAttachmentQuotaLimit(ctx, 0, &missing))
}

func TestAttachmentQuotaSelfHealsMissingRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user := createQuotaTestUser(ctx, t, ts, "quota-heal")
	createQuotaAttachment(t, ctx, ts, user.ID, 40, nil)

	// Simulate a missing quota row (for example a partially migrated database).
	deleteQuotaSQL := "DELETE FROM user_attachment_quota WHERE user_id = ?"
	if getDriverFromEnv() == "postgres" {
		deleteQuotaSQL = "DELETE FROM user_attachment_quota WHERE user_id = $1"
	}
	_, err := ts.GetDriver().GetDB().ExecContext(ctx, deleteQuotaSQL, user.ID)
	require.NoError(t, err)

	limit := int64(100)
	createQuotaAttachment(t, ctx, ts, user.ID, 60, &limit)
	requireQuotaUsed(t, ctx, ts, user.ID, 100)
}

func TestAttachmentQuotaListsAllUsers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	first := createQuotaTestUser(ctx, t, ts, "quota-list-a")
	second := createQuotaTestUser(ctx, t, ts, "quota-list-b")
	createQuotaAttachment(t, ctx, ts, first.ID, 7, nil)
	createQuotaAttachment(t, ctx, ts, second.ID, 9, nil)

	limit5 := 5
	offset0 := 0
	page, err := ts.ListUserAttachmentQuotas(ctx, &store.FindUserAttachmentQuota{Limit: &limit5, Offset: &offset0})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(page), 2)

	var found int
	for _, quota := range page {
		switch quota.UserID {
		case first.ID:
			require.Equal(t, int64(7), quota.UsedSize)
			found++
		case second.ID:
			require.Equal(t, int64(9), quota.UsedSize)
			found++
		}
	}
	require.Equal(t, 2, found)
}

func mustGetQuota(ctx context.Context, t *testing.T, ts *store.Store, userID int32) *store.UserAttachmentQuota {
	t.Helper()
	quota, err := ts.GetUserAttachmentQuota(ctx, userID)
	require.NoError(t, err)
	return quota
}

func countAttachments(t *testing.T, ctx context.Context, ts *store.Store, userID int32) int {
	t.Helper()
	attachments, err := ts.ListAttachments(ctx, &store.FindAttachment{CreatorID: &userID, SkipDefaultLimit: true})
	require.NoError(t, err)
	return len(attachments)
}
