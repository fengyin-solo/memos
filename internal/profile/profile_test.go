package profile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateInstanceURL(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{name: "empty", url: ""},
		{name: "canonical https", url: "https://memos.example.com", want: "https://memos.example.com"},
		{name: "trim spaces and trailing slash", url: "  https://memos.example.com/app/  ", want: "https://memos.example.com/app"},
		{name: "http with port", url: "http://localhost:8080/", want: "http://localhost:8080"},
		{name: "missing scheme", url: "memos.example.com", wantErr: true},
		{name: "unsupported scheme", url: "ftp://memos.example.com", wantErr: true},
		{name: "missing host", url: "https:///memos", wantErr: true},
		{name: "credentials", url: "https://user:pass@memos.example.com", wantErr: true},
		{name: "query", url: "https://memos.example.com?tenant=one", wantErr: true},
		{name: "fragment", url: "https://memos.example.com/#top", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &Profile{Data: t.TempDir(), Driver: "sqlite", InstanceURL: c.url}
			err := p.Validate()
			if c.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, p.InstanceURL)
		})
	}
}

func TestValidateRejectsDemoWithNonSQLiteDriver(t *testing.T) {
	p := &Profile{Demo: true, Driver: "postgres", Data: t.TempDir()}
	require.ErrorContains(t, p.Validate(), "demo mode requires the sqlite database driver")
}

func TestValidateRateLimitDriver(t *testing.T) {
	t.Run("empty defaults to memory", func(t *testing.T) {
		p := &Profile{Driver: "sqlite", Data: t.TempDir()}
		require.NoError(t, p.Validate())
		require.Equal(t, RateLimitDriverMemory, p.RateLimitDriver)
	})
	for _, driver := range []string{RateLimitDriverMemory, RateLimitDriverDatabase} {
		t.Run(driver, func(t *testing.T) {
			p := &Profile{Driver: "sqlite", Data: t.TempDir(), RateLimitDriver: driver}
			require.NoError(t, p.Validate())
		})
	}
	t.Run("unknown rejected", func(t *testing.T) {
		p := &Profile{Driver: "sqlite", Data: t.TempDir(), RateLimitDriver: "redis"}
		require.ErrorContains(t, p.Validate(), "unsupported rate-limit driver")
	})
}
