package test

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/internal/ratelimit"
	"github.com/usememos/memos/store"
	"github.com/usememos/memos/store/db"
)

// newRateLimitTestStores opens two store handles against the same migrated
// database, standing in for two Memos instances sharing one database.
func newRateLimitTestStores(ctx context.Context, t *testing.T) (*store.Store, *store.Store) {
	t.Helper()
	driverName := getDriverFromEnv()
	profile := getTestingProfileForDriver(t, driverName)

	open := func() *store.Store {
		dbDriver, err := db.NewDBDriver(profile)
		require.NoError(t, err)
		return store.New(dbDriver, profile)
	}
	first := open()
	require.NoError(t, first.Migrate(ctx))
	second := open()
	t.Cleanup(func() {
		_ = first.Close()
		_ = second.Close()
	})
	return first, second
}

func rateLimitRule() ratelimit.Rule {
	return ratelimit.Rule{Limit: 10, Window: time.Minute}
}

func TestRateLimitWindowSharedAcrossInstances(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rule := rateLimitRule()
	hit := ratelimit.WindowOp{Kind: ratelimit.WindowOpHit, Now: now, Rule: rule, Cost: 1}

	for range 2 {
		_, _, err := first.ApplyWindow(ctx, "test", "k", hit)
		require.NoError(t, err)
	}

	// The second instance reads what the first persisted.
	state, _, err := second.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpPeek, Now: now, Rule: rule,
	})
	require.NoError(t, err)
	require.True(t, state.Found)
	require.Equal(t, 2, state.Current)
}

// TestRateLimitWindowConcurrentConsume is the cross-instance guarantee: many
// goroutines charging through two independent store handles may not both be
// admitted against the same remaining budget.
func TestRateLimitWindowConcurrentConsume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	// A fixed instant and an hour window removes clock drift from the race;
	// the fresh row has no previous-window weight either.
	now := time.Now().Truncate(time.Hour)
	const limit = 5
	rule := ratelimit.Rule{Limit: limit, Window: time.Hour}

	consume := func(s *store.Store) bool {
		_, admitted, err := s.ApplyWindow(ctx, "test", "race", ratelimit.WindowOp{
			Kind: ratelimit.WindowOpConsume, Now: now, Rule: rule, Cost: 1,
		})
		require.NoError(t, err)
		return admitted
	}

	const racers = 48
	barrier := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for i := range racers {
		wg.Go(func() {
			<-barrier
			handle := first
			if i%2 == 0 {
				handle = second
			}
			if consume(handle) {
				admitted.Add(1)
			}
		})
	}
	close(barrier)
	wg.Wait()
	require.Equal(t, int32(limit), admitted.Load(), "the shared budget must be spent exactly once per unit")

	state, _, err := first.ApplyWindow(ctx, "test", "race", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpPeek, Now: now, Rule: rule,
	})
	require.NoError(t, err)
	require.Equal(t, limit, state.Current)
	require.Equal(t, 0, state.Previous)
}

func TestRateLimitWindowRollRefundAndCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	start := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rule := rateLimitRule()
	apply := func(s *store.Store, kind ratelimit.WindowOpKind, now time.Time, cost int) (ratelimit.WindowState, bool) {
		state, admitted, err := s.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
			Kind: kind, Now: now, Rule: rule, Cost: cost,
		})
		require.NoError(t, err)
		return state, admitted
	}

	// Spend the window; an additional consume refuses and charges nothing.
	for range 10 {
		_, _, err := first.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
			Kind: ratelimit.WindowOpHit, Now: start, Rule: rule, Cost: 1,
		})
		require.NoError(t, err)
	}
	_, admitted := apply(second, ratelimit.WindowOpConsume, start, 1)
	require.False(t, admitted)

	// At the exact boundary the ten hits are the previous window, fully weighted.
	state, _ := apply(first, ratelimit.WindowOpPeek, start.Add(time.Minute), 1)
	require.Equal(t, 0, state.Current)
	require.Equal(t, 10, state.Previous)
	_, admitted = apply(second, ratelimit.WindowOpConsume, start.Add(time.Minute), 1)
	require.False(t, admitted, "previous window still counts in full at the boundary")

	// One second later the weight truncates to nine and one unit fits.
	_, admitted = apply(second, ratelimit.WindowOpConsume, start.Add(time.Minute+time.Second), 1)
	require.True(t, admitted)

	// Refunding five against the single current unit clamps at zero instead
	// of writing a negative count. The refund's returned view is pre-op, so
	// verify the persisted result with a fresh peek.
	_, _, err := first.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpRefund, Now: start.Add(time.Minute + time.Second), Rule: rule, Cost: 5,
	})
	require.NoError(t, err)
	state, _ = apply(second, ratelimit.WindowOpPeek, start.Add(time.Minute+time.Second), 1)
	require.Equal(t, 0, state.Current)
	require.Equal(t, 10, state.Previous)

	// Two windows after the start nothing is weighted and the row is reaped.
	apply(first, ratelimit.WindowOpPeek, start.Add(2*time.Minute), 1)
	deleted, err := second.DeleteExpiredWindows(ctx, []ratelimit.WindowExpiry{
		{Scope: "test", Before: start.Add(2 * time.Minute)},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	state, _, err = first.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpPeek, Now: start.Add(2 * time.Minute), Rule: rule,
	})
	require.NoError(t, err)
	require.False(t, state.Found)
}

func TestRateLimitWindowConsumeRefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rule := ratelimit.Rule{Limit: 3, Window: time.Minute}
	_, admitted, err := first.ApplyWindow(ctx, "test", "big", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpConsume, Now: now, Rule: rule, Cost: 4,
	})
	require.NoError(t, err)
	require.False(t, admitted)

	state, _, err := second.ApplyWindow(ctx, "test", "big", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpPeek, Now: now, Rule: rule,
	})
	require.NoError(t, err)
	require.False(t, state.Found, "a refused consume must not create a row")
}

func TestRateLimitWindowGroupsByScopeAndKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, _ := newRateLimitTestStores(ctx, t)

	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rule := rateLimitRule()
	for _, scope := range []ratelimit.Scope{"a", "b"} {
		for _, key := range []string{"k1", "k2"} {
			_, _, err := first.ApplyWindow(ctx, scope, key, ratelimit.WindowOp{
				Kind: ratelimit.WindowOpHit, Now: now, Rule: rule, Cost: 7,
			})
			require.NoError(t, err)
		}
	}
	for _, scope := range []ratelimit.Scope{"a", "b"} {
		for _, key := range []string{"k1", "k2"} {
			state, _, err := first.ApplyWindow(ctx, scope, key, ratelimit.WindowOp{
				Kind: ratelimit.WindowOpPeek, Now: now, Rule: rule,
			})
			require.NoError(t, err)
			require.True(t, state.Found)
			require.Equal(t, 7, state.Current, "%s/%s", scope, key)
		}
	}
}

// TestStoreBackedLimiterRunsOnRealDatabase drives the same randomized stream
// through the in-memory limiter and a database-backed pair of "instances",
// asserting decisions stay identical end to end, including across handles.
func TestStoreBackedLimiterRunsOnRealDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	clock := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	policy := ratelimit.Policy{
		"short": {Limit: 3, Window: time.Minute},
		"long":  {Limit: 7, Window: 5 * time.Minute},
	}
	memory := ratelimit.NewMemoryLimiter(policy, ratelimit.WithClock(now))
	sharedA := ratelimit.NewStoreBackedLimiter(policy, first, ratelimit.WithStoreClock(now), ratelimit.WithCleanupInterval(0))
	sharedB := ratelimit.NewStoreBackedLimiter(policy, second, ratelimit.WithStoreClock(now), ratelimit.WithCleanupInterval(0))
	t.Cleanup(sharedA.Close)
	t.Cleanup(sharedB.Close)

	scopes := []ratelimit.Scope{"short", "long"}
	keys := []string{"k1", "k2"}
	rng := rand.New(rand.NewPCG(7, 9))
	step := 0
	sameDecision := func(scope ratelimit.Scope, key string, cost int, want, got ratelimit.Decision) {
		t.Helper()
		if want.Allowed != got.Allowed || want.Remaining != got.Remaining || want.RetryAfter != got.RetryAfter {
			t.Fatalf("step %d at %s (%s/%s cost=%d): memory=%+v shared=%+v",
				step, clock.Format("15:04:05"), scope, key, cost, want, got)
		}
	}

	for range 300 {
		scope := scopes[rng.IntN(len(scopes))]
		key := keys[rng.IntN(len(keys))]
		cost := 1 + rng.IntN(4)
		shared := sharedA
		if rng.IntN(2) == 0 {
			shared = sharedB
		}
		opKind := rng.IntN(6)
		switch opKind {
		case 0, 1:
			sameDecision(scope, key, cost, memory.Allowed(scope, key, cost), shared.Allowed(scope, key, cost))
		case 2, 3:
			sameDecision(scope, key, cost, memory.Consume(scope, key, cost), shared.Consume(scope, key, cost))
		case 4:
			memory.Hit(scope, key, cost)
			shared.Hit(scope, key, cost)
		case 5:
			memory.Refund(scope, key, cost)
			shared.Refund(scope, key, cost)
		default:
			// Every operation kind is covered above.
		}
		clock = clock.Add(time.Duration(rng.IntN(40)) * time.Second)
		step++
	}

	// The cleanup path works against the real schema and deletes only stale rows.
	require.NoError(t, sharedA.CleanupExpired(ctx))
	// Recent counters survive; only counters two windows old are reaped.
	for _, scope := range scopes {
		for _, key := range keys {
			state, _, err := first.ApplyWindow(ctx, scope, key, ratelimit.WindowOp{
				Kind: ratelimit.WindowOpPeek, Now: clock, Rule: policy[scope],
			})
			require.NoError(t, err)
			if state.Found {
				require.True(t, state.WindowStart.After(clock.Add(-2*policy[scope].Window)),
					"cleanup left an expired row for %s", scope)
			}
		}
	}
}

// TestRateLimitWindowFailOpenOnClosedDatabase proves a storage outage admits
// instead of refusing: a limiter backed by a closed database must keep serving.
func TestRateLimitWindowFailOpenOnClosedDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	first, second := newRateLimitTestStores(ctx, t)

	clock := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	limiter := ratelimit.NewStoreBackedLimiter(
		ratelimit.Policy{"test": {Limit: 1, Window: time.Minute}},
		second,
		ratelimit.WithStoreClock(func() time.Time { return clock }),
		ratelimit.WithCleanupInterval(0),
	)
	t.Cleanup(limiter.Close)
	require.NoError(t, second.Close())

	require.True(t, limiter.Consume("test", "k", 1).Allowed, "storage failure must fail open")
	require.True(t, limiter.Allowed("test", "k", 1).Allowed)
	require.NotPanics(t, func() {
		limiter.Hit("test", "k", 1)
		limiter.Refund("test", "k", 1)
	})
	require.Error(t, limiter.CleanupExpired(ctx))

	// The healthy instance is unaffected.
	state, _, err := first.ApplyWindow(ctx, "test", "k", ratelimit.WindowOp{
		Kind: ratelimit.WindowOpPeek, Now: clock, Rule: rateLimitRule(),
	})
	require.NoError(t, err)
	require.False(t, state.Found)
}
