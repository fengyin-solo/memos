package store

import (
	"testing"
	"time"

	storepb "github.com/usememos/memos/proto/gen/store"
)

func TestBlobGCConfigFromSetting(t *testing.T) {
	t.Run("nil setting uses defaults", func(t *testing.T) {
		config := BlobGCConfigFromSetting(nil)
		if config.GracePeriod != defaultBlobGracePeriod || config.OrphanSafetyAge != defaultBlobOrphanSafetyAge || config.GCInterval != defaultBlobGCInterval {
			t.Fatalf("unexpected defaults: %+v", config)
		}
		if !config.ScheduledEnabled {
			t.Fatal("scheduled GC should be enabled by default")
		}
	})

	t.Run("empty setting uses defaults", func(t *testing.T) {
		config := BlobGCConfigFromSetting(&storepb.InstanceStorageSetting{})
		if config.GracePeriod != defaultBlobGracePeriod || config.OrphanSafetyAge != defaultBlobOrphanSafetyAge || config.GCInterval != defaultBlobGCInterval {
			t.Fatalf("unexpected defaults: %+v", config)
		}
	})

	t.Run("explicit values win", func(t *testing.T) {
		config := BlobGCConfigFromSetting(&storepb.InstanceStorageSetting{
			BlobGracePeriodSeconds:     60,
			BlobGcIntervalSeconds:      3600,
			BlobOrphanSafetyAgeSeconds: 120,
		})
		if config.GracePeriod != 60*time.Second || config.OrphanSafetyAge != 120*time.Second || config.GCInterval != time.Hour {
			t.Fatalf("explicit values not honored: %+v", config)
		}
		if !config.ScheduledEnabled {
			t.Fatal("scheduled GC should stay enabled with a positive interval")
		}
	})

	t.Run("negative interval disables schedule only", func(t *testing.T) {
		config := BlobGCConfigFromSetting(&storepb.InstanceStorageSetting{BlobGcIntervalSeconds: -1})
		if config.ScheduledEnabled {
			t.Fatal("negative interval should disable scheduled GC")
		}
		if config.GracePeriod != defaultBlobGracePeriod || config.OrphanSafetyAge != defaultBlobOrphanSafetyAge {
			t.Fatalf("other defaults changed: %+v", config)
		}
	})

	t.Run("negative other fields fall back", func(t *testing.T) {
		config := BlobGCConfigFromSetting(&storepb.InstanceStorageSetting{
			BlobGracePeriodSeconds:     -10,
			BlobOrphanSafetyAgeSeconds: -1,
		})
		if config.GracePeriod != defaultBlobGracePeriod || config.OrphanSafetyAge != defaultBlobOrphanSafetyAge {
			t.Fatalf("negative values should fall back to defaults: %+v", config)
		}
	})
}
