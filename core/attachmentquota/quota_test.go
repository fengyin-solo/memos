package attachmentquota

import (
	"errors"
	"testing"
)

func int64Ptr(v int64) *int64 {
	return &v
}

func TestEffectiveLimit(t *testing.T) {
	tests := []struct {
		name           string
		instance       int64
		override       *int64
		wantLimit      int64
		wantEnabled    bool
	}{
		{name: "default disabled", instance: 0, wantLimit: 0, wantEnabled: false},
		{name: "default enabled", instance: 100, wantLimit: 100, wantEnabled: true},
		{name: "override wins over default", instance: 100, override: int64Ptr(50), wantLimit: 50, wantEnabled: true},
		{name: "override enables even when default disabled", instance: 0, override: int64Ptr(50), wantLimit: 50, wantEnabled: true},
		{name: "override zero blocks all uploads", instance: 100, override: int64Ptr(0), wantLimit: 0, wantEnabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLimit, gotEnabled := EffectiveLimit(tt.instance, tt.override)
			if gotLimit != tt.wantLimit || gotEnabled != tt.wantEnabled {
				t.Fatalf("EffectiveLimit() = (%d, %v), want (%d, %v)", gotLimit, gotEnabled, tt.wantLimit, tt.wantEnabled)
			}
		})
	}
}

func TestRemaining(t *testing.T) {
	tests := []struct {
		name string
		used int64
		limit int64
		want int64
	}{
		{name: "unused", used: 0, limit: 100, want: 100},
		{name: "partially used", used: 40, limit: 100, want: 60},
		{name: "exactly full", used: 100, limit: 100, want: 0},
		{name: "over limit clamps to zero", used: 140, limit: 100, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Remaining(tt.used, tt.limit); got != tt.want {
				t.Fatalf("Remaining() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFits(t *testing.T) {
	if !Fits(0, 100, 100) {
		t.Fatal("a write that exactly fills the quota must fit")
	}
	if !Fits(60, 100, 40) {
		t.Fatal("a write that lands exactly on the limit must fit")
	}
	if Fits(61, 100, 40) {
		t.Fatal("a write that crosses the limit must not fit")
	}
	if Fits(0, 0, 0) {
		t.Fatal("a zero-limit quota must reject any write, including zero bytes")
	}
}

func TestExceededError(t *testing.T) {
	var err error = &ExceededError{UsedBytes: 90, LimitBytes: 100, RequestedBytes: 20}
	if !errors.Is(err, err) {
		t.Fatal("ExceededError should satisfy errors.Is with itself")
	}
	if err.Error() != "attachment quota exceeded" {
		t.Fatalf("unexpected error message: %q", err.Error())
	}
}
