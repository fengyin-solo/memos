package objectcleanup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func TestClassifyDeleteError(t *testing.T) {
	tests := []struct {
		name        string
		storageType storepb.AttachmentStorageType
		code        string
		want        Outcome
	}{
		{"s3 access denied is terminal", storepb.AttachmentStorageType_S3, "AccessDenied", OutcomeFailTerminal},
		{"s3 invalid argument is terminal", storepb.AttachmentStorageType_S3, "InvalidArgument", OutcomeFailTerminal},
		{"s3 missing bucket is terminal", storepb.AttachmentStorageType_S3, "NoSuchBucket", OutcomeFailTerminal},
		{"s3 throttle is retryable", storepb.AttachmentStorageType_S3, "SlowDown", OutcomeRetry},
		{"s3 server error is retryable", storepb.AttachmentStorageType_S3, "InternalError", OutcomeRetry},
		{"s3 network error is retryable", storepb.AttachmentStorageType_S3, "", OutcomeRetry},
		{"local error is retryable", storepb.AttachmentStorageType_LOCAL, "", OutcomeRetry},
		{"local permission-like code stays retryable", storepb.AttachmentStorageType_LOCAL, "AccessDenied", OutcomeRetry},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, ClassifyDeleteError(test.storageType, test.code))
		})
	}
}

func TestBackoffDoublesAndCapsWithoutJitter(t *testing.T) {
	backoff := BackoffConfig{Base: 30 * time.Second, Factor: 2, Cap: time.Hour}
	base := time.Unix(1000, 0)
	require.Equal(t, time.Unix(1030, 0), backoff.NextRun(1, base))
	require.Equal(t, time.Unix(1060, 0), backoff.NextRun(2, base))
	require.Equal(t, time.Unix(1120, 0), backoff.NextRun(3, base))
	// 30m -> 1h cap.
	require.Equal(t, base.Add(time.Hour), backoff.NextRun(13, base))
	require.Equal(t, base.Add(time.Hour), backoff.NextRun(100, base))
}

func TestBackoffStaysWithinJitterBand(t *testing.T) {
	backoff := BackoffConfig{Base: time.Minute, Factor: 2, Cap: time.Hour, Jitter: 0.2}
	for attempt := 1; attempt < 20; attempt++ {
		got := backoff.Delay(attempt)
		expected := time.Minute << (attempt - 1)
		if expected > time.Hour {
			expected = time.Hour
		}
		require.GreaterOrEqual(t, got, time.Duration(float64(expected)*0.8))
		require.LessOrEqual(t, got, time.Duration(float64(expected)*1.2))
	}
}

func TestBuildReportCountsAndOrphans(t *testing.T) {
	now := time.Unix(2000, 0)
	stats := []*store.StorageCleanupSourceStats{
		{Source: store.StorageCleanupSourceAttachment, Status: store.StorageCleanupStatusPending, Count: 2},
		{Source: store.StorageCleanupSourceAttachment, Status: store.StorageCleanupStatusSucceeded, Count: 3},
		{Source: store.StorageCleanupSourceMemo, Status: store.StorageCleanupStatusFailed, Count: 1},
		{Source: store.StorageCleanupSourceSpace, Status: store.StorageCleanupStatusProcessing, Count: 1},
	}
	liveRefs := []*store.StorageObjectRef{
		{StorageType: storepb.AttachmentStorageType_S3, StorageID: "s3-primary", StorageKey: "live.txt"},
	}
	ledgerRefs := []*store.StorageObjectRef{
		{StorageType: storepb.AttachmentStorageType_S3, StorageID: "s3-primary", StorageKey: "tracked.txt"},
	}
	localGroup := StorageGroup{StorageType: storepb.AttachmentStorageType_LOCAL, StorageID: store.LocalStorageID}
	s3Group := StorageGroup{StorageType: storepb.AttachmentStorageType_S3, StorageID: "s3-primary"}
	goneGroup := StorageGroup{StorageType: storepb.AttachmentStorageType_S3, StorageID: "s3-removed"}
	report := BuildReport(now, stats, liveRefs, ledgerRefs, map[StorageGroup]StorageListing{
		s3Group:    {Available: true, Keys: []string{"live.txt", "tracked.txt", "orphan.txt"}},
		localGroup: {Available: true, Keys: []string{"assets/local-orphan.png"}},
		goneGroup:  {Available: false},
	})

	require.Equal(t, now, report.GeneratedTime)
	bySource := map[store.StorageCleanupSource]SourceStatusCount{}
	for _, count := range report.SourceCounts {
		bySource[count.Source] = count
	}
	require.Equal(t, int64(2), bySource[store.StorageCleanupSourceAttachment].Pending)
	require.Equal(t, int64(3), bySource[store.StorageCleanupSourceAttachment].Succeeded)
	require.Equal(t, int64(1), bySource[store.StorageCleanupSourceMemo].Failed)
	require.Equal(t, int64(1), bySource[store.StorageCleanupSourceSpace].Processing)
	require.Equal(t, int64(0), bySource[store.StorageCleanupSourceUser].Pending)
	require.Len(t, report.SourceCounts, 4, "every known source gets a row")

	orphans := map[string]StorageOrphanCount{}
	for _, entry := range report.OrphanCounts {
		orphans[entry.Group.StorageType.String()+"/"+entry.Group.StorageID] = entry
	}
	require.Equal(t, int64(1), orphans["S3/s3-primary"].SuspectedOrphanCount, "only the key without live or ledger coverage")
	require.Equal(t, int64(1), orphans["LOCAL/local"].SuspectedOrphanCount)
	require.False(t, orphans["S3/s3-removed"].Available)
	require.Zero(t, orphans["S3/s3-removed"].SuspectedOrphanCount)
}
