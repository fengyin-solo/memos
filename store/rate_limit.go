package store

import (
	"context"
	"time"

	"github.com/usememos/memos/internal/ratelimit"
)

// RateLimitWindowOpKind enumerates the atomic operations a rate limiter
// applies to one persisted counter row.
type RateLimitWindowOpKind int

const (
	// RateLimitWindowPeek returns the rolled counter view without writing.
	RateLimitWindowPeek RateLimitWindowOpKind = iota
	// RateLimitWindowHit unconditionally adds the mutation's cost to the current window.
	RateLimitWindowHit
	// RateLimitWindowConsume adds the cost only when the window still admits it.
	RateLimitWindowConsume
	// RateLimitWindowRefund subtracts the cost from the current window, never below zero.
	RateLimitWindowRefund
)

// rateLimitWindowOpKinds maps the limiter-facing operation kinds onto the
// driver-facing ones; both enumerations declare the same constants in the
// same order, and a missing entry falls back to Peek.
var rateLimitWindowOpKinds = map[ratelimit.WindowOpKind]RateLimitWindowOpKind{
	ratelimit.WindowOpPeek:    RateLimitWindowPeek,
	ratelimit.WindowOpHit:     RateLimitWindowHit,
	ratelimit.WindowOpConsume: RateLimitWindowConsume,
	ratelimit.WindowOpRefund:  RateLimitWindowRefund,
}

// RateLimitWindowMutation carries one atomic operation against the counter
// for one (scope, key). Window and Now locate the fixed window; Limit is
// consulted only by RateLimitWindowConsume.
type RateLimitWindowMutation struct {
	Scope  string
	Key    string
	Op     RateLimitWindowOpKind
	Now    time.Time
	Window time.Duration
	Cost   int
	Limit  int
}

// WindowStart is the start of the fixed window containing Now.
func (m *RateLimitWindowMutation) WindowStart() time.Time {
	return m.Now.Truncate(m.Window)
}

// RateLimitWindow is the rolled counter view an atomic operation returns.
type RateLimitWindow struct {
	// Found reports whether a stored row existed.
	Found bool
	// WindowStart is the fixed window the counters were rolled to.
	WindowStart time.Time
	// Current is the count in the current fixed window.
	Current int
	// Previous is the count carried from the adjacent previous window.
	Previous int
}

// RateLimitWindowExpiry scopes a cleanup cutoff: rows for Scope whose window
// starts at or before Before are expired and may be deleted.
type RateLimitWindowExpiry struct {
	Scope  string
	Before time.Time
}

// RollRateLimitWindow advances a stored counter to windowStart, the fixed
// window containing the operation's now. A row from the adjacent window moves
// its count to previous; a row two or more windows old contributes nothing.
// It returns the rolled (current, previous) counts and mirrors the in-memory
// limiter's entry roll exactly.
func RollRateLimitWindow(storedStart, windowStart time.Time, window time.Duration, current, previous int) (int, int) {
	switch {
	case storedStart.Equal(windowStart):
		return current, previous
	case storedStart.Equal(windowStart.Add(-window)):
		return 0, current
	default:
		return 0, 0
	}
}

// RateLimitWindowEstimate is the sliding-window count at now for a rolled
// counter view: the current window plus the weighted share of the previous
// window that still overlaps the last Window of time. The truncation matches
// the in-memory limiter's estimate, so admission decisions never differ.
func RateLimitWindowEstimate(current, previous int, now, windowStart time.Time, window time.Duration) int {
	elapsed := now.Sub(windowStart)
	overlap := 1 - float64(elapsed)/float64(window)
	if overlap < 0 {
		overlap = 0
	}
	return current + int(float64(previous)*overlap)
}

// ApplyWindow implements ratelimit.WindowStore against the configured database
// driver, so every Memos process sharing the database shares one budget.
func (s *Store) ApplyWindow(ctx context.Context, scope ratelimit.Scope, key string, op ratelimit.WindowOp) (ratelimit.WindowState, bool, error) {
	mutation := &RateLimitWindowMutation{
		Scope:  string(scope),
		Key:    key,
		Now:    op.Now,
		Window: op.Rule.Window,
		Cost:   op.Cost,
		Limit:  op.Rule.Limit,
		// An unknown kind maps to the zero value, Peek, so a caller can never
		// make the driver mutate a counter through an out-of-range operation.
		Op: rateLimitWindowOpKinds[op.Kind],
	}

	state, admitted, err := s.driver.ApplyRateLimitWindow(ctx, mutation)
	if err != nil {
		return ratelimit.WindowState{}, false, err
	}
	return ratelimit.WindowState{
		Found:       state.Found,
		WindowStart: state.WindowStart,
		Current:     state.Current,
		Previous:    state.Previous,
	}, admitted, nil
}

// DeleteExpiredWindows implements ratelimit.WindowStore by deleting rows that
// can no longer overlap a sliding window for their scope.
func (s *Store) DeleteExpiredWindows(ctx context.Context, expiries []ratelimit.WindowExpiry) (int64, error) {
	if len(expiries) == 0 {
		return 0, nil
	}
	rows := make([]RateLimitWindowExpiry, 0, len(expiries))
	for _, expiry := range expiries {
		rows = append(rows, RateLimitWindowExpiry{Scope: string(expiry.Scope), Before: expiry.Before})
	}
	return s.driver.DeleteExpiredRateLimitWindows(ctx, rows)
}
