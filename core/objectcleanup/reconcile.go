package objectcleanup

import (
	"slices"
	"time"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// SourceStatusCount is the ledger task count for one source and state.
type SourceStatusCount struct {
	Source     store.StorageCleanupSource
	Pending    int64
	Processing int64
	Succeeded  int64
	Failed     int64
}

// StorageGroup identifies one physical storage namespace to reconcile.
type StorageGroup struct {
	StorageType storepb.AttachmentStorageType
	StorageID   string
}

// StorageListing is the observed object key set of one storage. When
// Available is false the storage could not be listed (removed registry entry,
// unreachable endpoint); orphan counting is skipped for it.
type StorageListing struct {
	Available bool
	Keys      []string
}

// StorageOrphanCount reports objects that exist in storage but are neither
// referenced by a live attachment nor present in the cleanup ledger.
type StorageOrphanCount struct {
	Group                StorageGroup
	Available            bool
	SuspectedOrphanCount int64
}

// Report is the read-only reconciliation result.
type Report struct {
	GeneratedTime time.Time
	// SourceCounts always lists every known source, even those with zero
	// tasks, in a stable order.
	SourceCounts []SourceStatusCount
	// OrphanCounts lists every storage that was scanned, in a stable order.
	OrphanCounts []StorageOrphanCount
}

// orderedSources fixes the report row order.
var orderedSources = []store.StorageCleanupSource{
	store.StorageCleanupSourceAttachment,
	store.StorageCleanupSourceMemo,
	store.StorageCleanupSourceSpace,
	store.StorageCleanupSourceUser,
}

// BuildReport assembles the reconciliation report from ledger stats, the
// object keys covered by live attachments or ledger rows, and actual storage
// listings. It performs no mutation: callers pass snapshots they read.
func BuildReport(
	now time.Time,
	stats []*store.StorageCleanupSourceStats,
	liveRefs []*store.StorageObjectRef,
	ledgerRefs []*store.StorageObjectRef,
	listings map[StorageGroup]StorageListing,
) *Report {
	report := &Report{GeneratedTime: now}

	for _, source := range orderedSources {
		report.SourceCounts = append(report.SourceCounts, SourceStatusCount{Source: source})
	}
	indexBySource := map[store.StorageCleanupSource]int{}
	for index, source := range orderedSources {
		indexBySource[source] = index
	}
	for _, stat := range stats {
		index, ok := indexBySource[stat.Source]
		if !ok {
			// Unknown sources get an extra row rather than being silently dropped.
			report.SourceCounts = append(report.SourceCounts, SourceStatusCount{Source: stat.Source})
			index = len(report.SourceCounts) - 1
			indexBySource[stat.Source] = index
		}
		switch stat.Status {
		case store.StorageCleanupStatusPending:
			report.SourceCounts[index].Pending += stat.Count
		case store.StorageCleanupStatusProcessing:
			report.SourceCounts[index].Processing += stat.Count
		case store.StorageCleanupStatusSucceeded:
			report.SourceCounts[index].Succeeded += stat.Count
		case store.StorageCleanupStatusFailed:
			report.SourceCounts[index].Failed += stat.Count
		}
	}

	// Covered keys are objects that must not be counted as orphans: every
	// live attachment reference and every ledger key regardless of state.
	covered := map[string]struct{}{}
	for _, ref := range liveRefs {
		covered[refKey(ref)] = struct{}{}
	}
	for _, ref := range ledgerRefs {
		covered[refKey(ref)] = struct{}{}
	}

	groups := make([]StorageGroup, 0, len(listings))
	for group := range listings {
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(left, right StorageGroup) int {
		if left.StorageType != right.StorageType {
			if left.StorageType < right.StorageType {
				return -1
			}
			return 1
		}
		if left.StorageID < right.StorageID {
			return -1
		}
		if left.StorageID > right.StorageID {
			return 1
		}
		return 0
	})
	for _, group := range groups {
		listing := listings[group]
		entry := StorageOrphanCount{Group: group, Available: listing.Available}
		if listing.Available {
			for _, key := range listing.Keys {
				ref := &store.StorageObjectRef{StorageType: group.StorageType, StorageID: group.StorageID, StorageKey: key}
				if _, ok := covered[refKey(ref)]; !ok {
					entry.SuspectedOrphanCount++
				}
			}
		}
		report.OrphanCounts = append(report.OrphanCounts, entry)
	}
	return report
}

func refKey(ref *store.StorageObjectRef) string {
	return ref.StorageType.String() + "\x00" + ref.StorageID + "\x00" + ref.StorageKey
}
