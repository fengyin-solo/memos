package store

import (
	"context"
	"time"

	storepb "github.com/usememos/memos/proto/gen/store"
)

const (
	// defaultBlobGracePeriod is how long an unreferenced blob is retained
	// before physical deletion.
	defaultBlobGracePeriod = 7 * 24 * time.Hour
	// defaultBlobGCInterval is the interval between scheduled GC runs.
	defaultBlobGCInterval = 24 * time.Hour
	// defaultBlobOrphanSafetyAge protects objects written by an upload whose
	// database commit may still be in flight.
	defaultBlobOrphanSafetyAge = time.Hour
)

// BlobGCConfig is the resolved blob garbage collection policy.
type BlobGCConfig struct {
	// GracePeriod is how long a PENDING_DELETE blob is retained.
	GracePeriod time.Duration
	// OrphanSafetyAge is the minimum age of an object without a blob row
	// before the orphan sweep may delete it.
	OrphanSafetyAge time.Duration
	// GCInterval is the interval between scheduled GC runs.
	GCInterval time.Duration
	// ScheduledEnabled reports whether automatic GC runs are enabled.
	ScheduledEnabled bool
}

// BlobGCConfigFromSetting resolves the blob GC policy from the stored instance
// storage setting. Unset or invalid fields fall back to built-in defaults;
// a negative GC interval disables scheduled collection.
func BlobGCConfigFromSetting(setting *storepb.InstanceStorageSetting) BlobGCConfig {
	config := BlobGCConfig{
		GracePeriod:      defaultBlobGracePeriod,
		OrphanSafetyAge:  defaultBlobOrphanSafetyAge,
		GCInterval:       defaultBlobGCInterval,
		ScheduledEnabled: true,
	}
	if setting == nil {
		return config
	}
	if setting.BlobGracePeriodSeconds > 0 {
		config.GracePeriod = time.Duration(setting.BlobGracePeriodSeconds) * time.Second
	}
	if setting.BlobOrphanSafetyAgeSeconds > 0 {
		config.OrphanSafetyAge = time.Duration(setting.BlobOrphanSafetyAgeSeconds) * time.Second
	}
	if setting.BlobGcIntervalSeconds < 0 {
		config.ScheduledEnabled = false
	} else if setting.BlobGcIntervalSeconds > 0 {
		config.GCInterval = time.Duration(setting.BlobGcIntervalSeconds) * time.Second
	}
	return config
}

// GetBlobGCConfig loads the current blob GC policy from the instance storage
// setting.
func (s *Store) GetBlobGCConfig(ctx context.Context) (BlobGCConfig, error) {
	setting, err := s.GetInstanceStorageSetting(ctx)
	if err != nil {
		return BlobGCConfig{}, err
	}
	return BlobGCConfigFromSetting(setting), nil
}
