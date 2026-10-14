package objectcleanup

import (
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	corecleanup "github.com/usememos/memos/core/objectcleanup"
	"github.com/usememos/memos/internal/profile"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// internalDataEntries are never attachment objects: derived caches written by
// the file server.
var internalDataEntries = map[string]struct{}{
	".thumbnail_cache": {},
	".motion_cache":    {},
}

// Reconcile performs a single read-only reconciliation scan: ledger stats,
// ledger keys, live attachment keys and actual storage listings. Nothing is
// deleted or written. Storages that cannot be listed are reported as
// unavailable instead of failing the whole scan.
func Reconcile(ctx context.Context, stores *store.Store, profile *profile.Profile) (*corecleanup.Report, error) {
	stats, err := stores.GetStorageCleanupStats(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read cleanup task stats")
	}

	ledgerRefs, err := listLedgerObjectRefs(ctx, stores)
	if err != nil {
		return nil, err
	}
	liveRefs, err := stores.ListAttachmentStorageRefs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list live attachment refs")
	}

	listings := map[corecleanup.StorageGroup]corecleanup.StorageListing{}
	listLocalStorage(listings, profile)
	if err := listConfiguredS3Storages(ctx, stores, listings); err != nil {
		return nil, err
	}
	markUnconfiguredLedgerStorages(ledgerRefs, listings)

	return corecleanup.BuildReport(time.Now(), stats, liveRefs, ledgerRefs, listings), nil
}

// listLedgerObjectRefs pages through every ledger task.
func listLedgerObjectRefs(ctx context.Context, stores *store.Store) ([]*store.StorageObjectRef, error) {
	refs := make([]*store.StorageObjectRef, 0)
	var afterID int64
	for {
		tasks, err := stores.ListStorageCleanupTasks(ctx, afterID, 1000)
		if err != nil {
			return nil, errors.Wrap(err, "failed to list cleanup tasks")
		}
		for _, task := range tasks {
			if task.StorageKey == "" {
				continue
			}
			refs = append(refs, &store.StorageObjectRef{StorageType: task.StorageType, StorageID: task.StorageID, StorageKey: task.StorageKey})
		}
		if len(tasks) < 1000 {
			return refs, nil
		}
		afterID = tasks[len(tasks)-1].ID
	}
}

// listLocalStorage walks the data directory once. Per-entry errors do not
// abort the walk, matching the instance stats walker; the result is reported
// as unavailable only when the root itself cannot be read.
func listLocalStorage(listings map[corecleanup.StorageGroup]corecleanup.StorageListing, profile *profile.Profile) {
	group := corecleanup.StorageGroup{StorageType: storepb.AttachmentStorageType_LOCAL, StorageID: store.LocalStorageID}
	if profile == nil || profile.Data == "" {
		listings[group] = corecleanup.StorageListing{Available: false}
		return
	}
	skipFiles := sqliteDataFiles(profile)
	keys := make([]string, 0)
	rootErr := filepath.WalkDir(profile.Data, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == profile.Data {
				return walkErr
			}
			return nil
		}
		if path == profile.Data {
			return nil
		}
		if entry.IsDir() {
			if _, internal := internalDataEntries[entry.Name()]; internal {
				return filepath.SkipDir
			}
			return nil
		}
		if _, skip := skipFiles[entry.Name()]; skip {
			return nil
		}
		relative, err := filepath.Rel(profile.Data, path)
		if err != nil {
			return nil
		}
		keys = append(keys, filepath.ToSlash(relative))
		return nil
	})
	if rootErr != nil {
		slog.Warn("local storage reconciliation unavailable", slog.Any("err", rootErr))
		listings[group] = corecleanup.StorageListing{Available: false}
		return
	}
	listings[group] = corecleanup.StorageListing{Available: true, Keys: keys}
}

// sqliteDataFiles returns the sqlite database and journal file names so they
// are not mistaken for attachment objects. The default prod/demo database
// files under the data directory are always excluded, plus a DSN-named file
// that resolves to a plain name. In-memory DSNs contribute nothing.
func sqliteDataFiles(profile *profile.Profile) map[string]struct{} {
	skip := map[string]struct{}{}
	if profile.Driver != "sqlite" {
		return skip
	}
	for _, base := range []string{"memos_prod.db", "memos_demo.db"} {
		skip[base] = struct{}{}
		skip[base+"-wal"] = struct{}{}
		skip[base+"-shm"] = struct{}{}
	}
	if profile.DSN != "" && !strings.Contains(profile.DSN, ":") && (filepath.IsAbs(profile.DSN) || filepath.Dir(profile.DSN) == ".") {
		base := filepath.Base(profile.DSN)
		skip[base] = struct{}{}
		skip[base+"-wal"] = struct{}{}
		skip[base+"-shm"] = struct{}{}
	}
	return skip
}

// listConfiguredS3Storages lists every S3 bucket registered in the instance
// storage setting. A failure to build or query one storage marks just that
// storage unavailable; a failure to read the setting aborts because no S3
// namespace can be resolved reliably.
func listConfiguredS3Storages(ctx context.Context, stores *store.Store, listings map[corecleanup.StorageGroup]corecleanup.StorageListing) error {
	setting, err := stores.GetInstanceStorageSetting(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read instance storage setting")
	}
	for _, configured := range setting.GetStorages() {
		if configured.GetType() != storepb.StorageType_STORAGE_TYPE_S3 {
			continue
		}
		group := corecleanup.StorageGroup{StorageType: storepb.AttachmentStorageType_S3, StorageID: configured.Id}
		driver, err := stores.StorageDriver(ctx, configured)
		if err != nil {
			slog.Warn("storage cleanup reconciliation cannot resolve storage", slog.String("storage_id", configured.Id), slog.Any("err", err))
			listings[group] = corecleanup.StorageListing{Available: false}
			continue
		}
		keys, err := driver.ListObjectKeys(ctx)
		if err != nil {
			slog.Warn("storage cleanup reconciliation cannot list storage", slog.String("storage_id", configured.Id), slog.Any("err", err))
			listings[group] = corecleanup.StorageListing{Available: false}
			continue
		}
		listings[group] = corecleanup.StorageListing{Available: true, Keys: keys}
	}
	return nil
}

// markUnconfiguredLedgerStorages adds ledger S3 namespaces missing from the
// registry as unavailable groups, so the report surfaces that orphan
// detection could not run for them instead of silently omitting them.
func markUnconfiguredLedgerStorages(refs []*store.StorageObjectRef, listings map[corecleanup.StorageGroup]corecleanup.StorageListing) {
	for _, ref := range refs {
		if ref.StorageType != storepb.AttachmentStorageType_S3 {
			continue
		}
		group := corecleanup.StorageGroup{StorageType: storepb.AttachmentStorageType_S3, StorageID: ref.StorageID}
		if _, exists := listings[group]; exists {
			continue
		}
		listings[group] = corecleanup.StorageListing{Available: false}
	}
}
