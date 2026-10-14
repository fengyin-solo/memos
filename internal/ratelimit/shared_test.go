package ratelimit

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeWindowStore is a correctly serialized reference implementation of
// WindowStore: one mutex stands in for the database's row locks so several
// limiter instances share one budget. The real SQL atomicity is exercised
// against every driver in store/test.
type fakeWindowStore struct {
	mu        sync.Mutex
	rows      map[fakeRowKey]*entry
	applyErr  error
	deleteErr error
}

type fakeRowKey struct {
	scope Scope
	key   string
}

func newFakeWindowStore() *fakeWindowStore {
	return &fakeWindowStore{rows: map[fakeRowKey]*entry{}}
}

func (s *fakeWindowStore) ApplyWindow(_ context.Context, scope Scope, key string, op WindowOp) (WindowState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		return WindowState{}, false, s.applyErr
	}

	id := fakeRowKey{scope: scope, key: key}
	current, ok := s.rows[id]
	if !ok {
		current = &entry{windowStart: op.Now.Truncate(op.Rule.Window)}
	}
	current.roll(op.Rule, op.Now)

	// The state handed back to the limiter is the rolled pre-op view.
	state := WindowState{
		Found:       ok,
		WindowStart: current.windowStart,
		Current:     current.current,
		Previous:    current.previous,
	}

	admitted := false
	switch op.Kind {
	case WindowOpHit:
		current.current += op.Cost
		if !ok {
			s.rows[id] = current
		}
	case WindowOpConsume:
		used := current.estimate(op.Rule, op.Now)
		if used+op.Cost <= op.Rule.Limit {
			admitted = true
			current.current += op.Cost
			if !ok {
				s.rows[id] = current
			}
		}
	case WindowOpRefund:
		if ok {
			current.current = max(current.current-op.Cost, 0)
		}
	default:
		// Peek and any unknown kind mutate nothing.
	}
	return state, admitted, nil
}

func (s *fakeWindowStore) DeleteExpiredWindows(_ context.Context, expiries []WindowExpiry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return 0, s.deleteErr
	}
	var deleted int64
	for id, row := range s.rows {
		for _, expiry := range expiries {
			if id.scope == expiry.Scope && !row.windowStart.After(expiry.Before) {
				delete(s.rows, id)
				deleted++
				break
			}
		}
	}
	return deleted, nil
}

func (s *fakeWindowStore) count(scope Scope, key string) (current, previous int, windowStart time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, exists := s.rows[fakeRowKey{scope: scope, key: key}]
	if !exists {
		return 0, 0, time.Time{}, false
	}
	return row.current, row.previous, row.windowStart, true
}

func (s *fakeWindowStore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

func newSharedLimiter(t *testing.T, p Policy, c *clock, store *fakeWindowStore) *StoreBackedLimiter {
	t.Helper()
	limiter := NewStoreBackedLimiter(p, store, WithStoreClock(c.Now), WithCleanupInterval(0))
	t.Cleanup(limiter.Close)
	return limiter
}

func TestStoreBackedAllowedDoesNotConsume(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	l := newSharedLimiter(t, policy(2, time.Minute), c, store)
	for range 10 {
		require.True(t, l.Allowed("test", "k", 1).Allowed)
	}
	require.Equal(t, 2, l.Allowed("test", "k", 1).Remaining)
	_, _, _, ok := store.count("test", "k")
	require.False(t, ok, "Allowed must not persist anything")
}

func TestStoreBackedHitThenRefuse(t *testing.T) {
	c := newClock()
	l := newSharedLimiter(t, policy(3, time.Minute), c, newFakeWindowStore())
	l.Hit("test", "k", 1)
	l.Hit("test", "k", 1)
	d := l.Allowed("test", "k", 1)
	require.True(t, d.Allowed)
	require.Equal(t, 1, d.Remaining)
	l.Hit("test", "k", 1)
	d = l.Allowed("test", "k", 1)
	require.False(t, d.Allowed)
	require.Equal(t, 0, d.Remaining)
	require.Equal(t, 61*time.Second, d.RetryAfter)
	require.Equal(t, 3, d.Rule.Limit)

	c.Advance(60 * time.Second)
	require.False(t, l.Allowed("test", "k", 1).Allowed)
	c.Advance(time.Second)
	require.True(t, l.Allowed("test", "k", 1).Allowed)

	require.True(t, l.Allowed("test", "other", 1).Allowed)
	require.True(t, l.Allowed("unknown", "k", 1).Allowed)
}

func TestStoreBackedConsumeIsAtomicAcrossInstances(t *testing.T) {
	c := newClock()
	shared := newFakeWindowStore()
	const instances = 4
	limiters := make([]*StoreBackedLimiter, instances)
	for i := range instances {
		limiters[i] = newSharedLimiter(t, policy(1, time.Minute), c, shared)
	}

	const racers = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	var admitted atomic.Int32
	for i := range racers {
		wg.Go(func() {
			<-start
			if limiters[i%instances].Consume("test", "k", 1).Allowed {
				admitted.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), admitted.Load(), "every instance must draw from the one shared unit")

	// With a larger budget the same race admits exactly the limit.
	c2 := newClock()
	shared2 := newFakeWindowStore()
	const limit = 10
	l2s := make([]*StoreBackedLimiter, instances)
	for i := range instances {
		l2s[i] = newSharedLimiter(t, policy(limit, time.Minute), c2, shared2)
	}
	var admitted2 atomic.Int32
	barrier := make(chan struct{})
	var wg2 sync.WaitGroup
	for i := range 80 {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			<-barrier
			if l2s[i%instances].Consume("test", "k", 1).Allowed {
				admitted2.Add(1)
			}
		}(i)
	}
	close(barrier)
	wg2.Wait()
	require.Equal(t, int32(limit), admitted2.Load())
	current, _, _, ok := shared2.count("test", "k")
	require.True(t, ok)
	require.Equal(t, limit, current)
}

func TestStoreBackedSlidingWindowRolls(t *testing.T) {
	c := newClock()
	l := newSharedLimiter(t, policy(10, time.Minute), c, newFakeWindowStore())
	l.Hit("test", "k", 10)
	require.False(t, l.Allowed("test", "k", 1).Allowed)

	c.Advance(90 * time.Second)
	d := l.Allowed("test", "k", 1)
	require.True(t, d.Allowed)
	require.Equal(t, 5, d.Remaining)

	c.Advance(2 * time.Minute)
	require.Equal(t, 10, l.Allowed("test", "k", 1).Remaining)
}

func TestStoreBackedRefund(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	l := newSharedLimiter(t, policy(2, time.Minute), c, store)

	require.True(t, l.Consume("test", "k", 1).Allowed)
	require.True(t, l.Consume("test", "k", 1).Allowed)
	require.False(t, l.Consume("test", "k", 1).Allowed)
	l.Refund("test", "k", 1)
	require.Equal(t, 1, l.Allowed("test", "k", 1).Remaining)

	// Refunding more than held never drives the count negative.
	l.Refund("test", "k", 5)
	require.Equal(t, 2, l.Allowed("test", "k", 1).Remaining)
	current, _, _, _ := store.count("test", "k")
	require.Equal(t, 0, current)

	// An unseen key is a no-op and creates no row.
	l.Refund("test", "never-seen", 1)
	require.Equal(t, 2, l.Allowed("test", "never-seen", 1).Remaining)
	_, _, _, ok := store.count("test", "never-seen")
	require.False(t, ok)

	// A refund landing in a rolled window persists the roll, not a negative count.
	l.Consume("test", "a", 2)
	c.Advance(2 * time.Minute)
	l.Refund("test", "a", 1)
	current, previous, _, ok := store.count("test", "a")
	require.True(t, ok)
	require.Equal(t, 0, current)
	require.Equal(t, 0, previous)
}

func TestStoreBackedBatchCostNeverNegative(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	l := newSharedLimiter(t, policy(10, time.Minute), c, store)

	// A batch of seven spends seven; the next batch of four does not fit and
	// must charge nothing.
	require.True(t, l.Consume("test", "k", 7).Allowed)
	require.False(t, l.Consume("test", "k", 4).Allowed)
	current, _, _, _ := store.count("test", "k")
	require.Equal(t, 7, current)
	require.True(t, l.Consume("test", "k", 3).Allowed)
	require.False(t, l.Consume("test", "k", 1).Allowed)
	current, _, _, _ = store.count("test", "k")
	require.Equal(t, 10, current)

	// A single request larger than the whole limit is refused without a row write.
	store2 := newFakeWindowStore()
	l2 := newSharedLimiter(t, policy(2, time.Minute), c, store2)
	require.False(t, l2.Consume("test", "big", 3).Allowed)
	_, _, _, ok := store2.count("test", "big")
	require.False(t, ok)
}

func TestStoreBackedRetryAfterSemantics(t *testing.T) {
	c := newClock()
	l := newSharedLimiter(t, policy(1, time.Minute), c, newFakeWindowStore())
	c.Advance(20 * time.Second)
	l.Hit("test", "k", 1)
	d := l.Allowed("test", "k", 1)
	require.False(t, d.Allowed)
	require.Equal(t, 41*time.Second, d.RetryAfter)
	c.Advance(d.RetryAfter - time.Second)
	require.False(t, l.Allowed("test", "k", 1).Allowed)
	c.Advance(time.Second)
	require.True(t, l.Allowed("test", "k", 1).Allowed)

	// Oversized cost advertises one window, matching the memory limiter.
	d = l.Allowed("test", "fresh", 2)
	require.False(t, d.Allowed)
	require.Equal(t, time.Minute, d.RetryAfter)
}

func TestStoreBackedCleanupReapsByScopeWindow(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	p := Policy{
		"short": {Limit: 1, Window: time.Minute},
		"long":  {Limit: 1, Window: time.Hour},
	}
	l := newSharedLimiter(t, p, c, store)
	ctx := context.Background()

	l.Hit("short", "k", 1)
	l.Hit("long", "k", 1)
	require.NoError(t, l.CleanupExpired(ctx))
	require.Equal(t, 2, store.size())

	// Just short of two short windows, the row is still needed.
	c.Advance(2*time.Minute - time.Second)
	require.NoError(t, l.CleanupExpired(ctx))
	require.Equal(t, 2, store.size())

	// At the boundary the short row goes; the hour row stays.
	c.Advance(time.Second)
	require.NoError(t, l.CleanupExpired(ctx))
	require.Equal(t, 1, store.size())
	_, _, _, ok := store.count("short", "k")
	require.False(t, ok)
	_, _, _, ok = store.count("long", "k")
	require.True(t, ok)

	// Two long windows later the long row goes too.
	c.Advance(2 * time.Hour)
	require.NoError(t, l.CleanupExpired(ctx))
	require.Equal(t, 0, store.size())
}

func TestStoreBackedFailOpenOnStorageFailure(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	store.applyErr = errors.New("database is down")
	l := newSharedLimiter(t, policy(2, time.Minute), c, store)

	// Every decision admits despite the broken backend.
	require.True(t, l.Allowed("test", "k", 1).Allowed)
	require.True(t, l.Consume("test", "k", 1).Allowed)
	require.NotPanics(t, func() {
		l.Hit("test", "k", 1)
		l.Refund("test", "k", 1)
	})

	// Cleanup surfaces the error once for the caller but the limiter keeps serving.
	store.deleteErr = errors.New("cleanup conflict")
	require.Error(t, l.CleanupExpired(context.Background()))
	require.True(t, l.Consume("test", "k", 1).Allowed)
}

func TestStoreBackedUnknownScopeAndZeroCost(t *testing.T) {
	c := newClock()
	store := newFakeWindowStore()
	l := newSharedLimiter(t, policy(2, time.Minute), c, store)

	require.True(t, l.Consume("missing", "k", 1).Allowed)
	require.Equal(t, 0, store.size(), "an unlimited scope never touches the store")

	// A non-positive cost is charged as one, like the memory limiter.
	l.Hit("test", "k", 0)
	d := l.Consume("test", "k", 0)
	require.True(t, d.Allowed)
	require.False(t, l.Consume("test", "k", 0).Allowed)
}

// TestStoreBackedDecisionsMatchMemory drives the memory limiter and two
// shared-limiter "instances" with one identical randomized operation stream
// and asserts every decision is exactly the one the memory limiter returns.
func TestStoreBackedDecisionsMatchMemory(t *testing.T) {
	c := newClock()
	p := Policy{
		"short": {Limit: 3, Window: time.Minute},
		"long":  {Limit: 7, Window: 5 * time.Minute},
	}
	memory := NewMemoryLimiter(p, WithClock(c.Now))
	store := newFakeWindowStore()
	sharedA := NewStoreBackedLimiter(p, store, WithStoreClock(c.Now), WithCleanupInterval(0))
	sharedB := NewStoreBackedLimiter(p, store, WithStoreClock(c.Now), WithCleanupInterval(0))
	t.Cleanup(sharedA.Close)
	t.Cleanup(sharedB.Close)

	scopes := []Scope{"short", "long"}
	keys := []string{"k1", "k2"}
	rng := rand.New(rand.NewPCG(1, 2))

	sameDecision := func(t *testing.T, want, got Decision) {
		t.Helper()
		require.Equal(t, want.Allowed, got.Allowed)
		require.Equal(t, want.Remaining, got.Remaining)
		require.Equal(t, want.RetryAfter, got.RetryAfter)
	}

	for range 2000 {
		scope := scopes[rng.IntN(len(scopes))]
		key := keys[rng.IntN(len(keys))]
		cost := 1 + rng.IntN(4)
		op := rng.IntN(6)
		shared := sharedA
		if rng.IntN(2) == 0 {
			shared = sharedB
		}

		switch op {
		case 0, 1:
			want := memory.Allowed(scope, key, cost)
			got := shared.Allowed(scope, key, cost)
			sameDecision(t, want, got)
		case 2, 3:
			want := memory.Consume(scope, key, cost)
			got := shared.Consume(scope, key, cost)
			sameDecision(t, want, got)
		case 4:
			memory.Hit(scope, key, cost)
			shared.Hit(scope, key, cost)
		case 5:
			memory.Refund(scope, key, cost)
			shared.Refund(scope, key, cost)
		default:
			// op is drawn from 0..5, so every value is covered above.
		}
		// Advance in steps that stay inside, cross, or skip windows.
		c.Advance(time.Duration(rng.IntN(40)) * time.Second)
	}

	// A final full sweep at the resting clock agrees for every budget.
	for _, scope := range scopes {
		for _, key := range keys {
			for _, cost := range []int{1, 3, 8} {
				sameDecision(t, memory.Allowed(scope, key, cost), sharedA.Allowed(scope, key, cost))
			}
		}
	}
}
