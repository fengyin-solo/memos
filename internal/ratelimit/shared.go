// This file adds the shared-backend seam to the sliding-window limiter: a
// StoreBackedLimiter charges counters through a WindowStore so every process
// in a multi-instance deployment draws from one budget while keeping the
// window rolling, weighted estimate, and RetryAfter math identical to the
// in-memory implementation.
package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// WindowOpKind enumerates the atomic operations applied to one counter.
type WindowOpKind int

const (
	// WindowOpPeek returns the rolled counter view without writing.
	WindowOpPeek WindowOpKind = iota
	// WindowOpHit unconditionally adds the op's cost to the current window.
	WindowOpHit
	// WindowOpConsume adds the cost only when the window still admits it.
	WindowOpConsume
	// WindowOpRefund subtracts the cost from the current window, never below zero.
	WindowOpRefund
)

// WindowOp is one atomic counter operation. Rule supplies the window and
// limit; Limit is consulted only for WindowOpConsume.
type WindowOp struct {
	Kind WindowOpKind
	Now  time.Time
	Rule Rule
	Cost int
}

// WindowState is the rolled counter view an atomic operation returns.
type WindowState struct {
	// Found reports whether the backend held a row for the (scope, key).
	Found bool
	// WindowStart is the fixed window Current and Previous were rolled to.
	WindowStart time.Time
	// Current is the count in the current fixed window.
	Current int
	// Previous is the count carried from the adjacent previous window.
	Previous int
}

// WindowExpiry scopes a cleanup cutoff: counters for Scope whose window starts
// at or before Before are expired.
type WindowExpiry struct {
	Scope  Scope
	Before time.Time
}

// WindowStore persists sliding-window counters so several limiter instances
// share one budget. Implementations apply every op atomically per (scope, key):
// concurrent callers must be serialized, never allowed to spend the same
// remaining units twice. A WindowOpConsume reports whether it charged.
type WindowStore interface {
	ApplyWindow(ctx context.Context, scope Scope, key string, op WindowOp) (state WindowState, admitted bool, err error)
	// DeleteExpiredWindows removes rows past their cutoff. It is called
	// periodically so the table cannot grow without bound.
	DeleteExpiredWindows(ctx context.Context, expiries []WindowExpiry) (int64, error)
}

// Defaults for the database-backed limiter.
const (
	// defaultStoreOpTimeout bounds one counter round trip so a slow database
	// fails open quickly instead of pinning request goroutines.
	defaultStoreOpTimeout = 3 * time.Second
	// defaultCleanupInterval is how often expired rows are reaped.
	defaultCleanupInterval = 10 * time.Minute
	// cleanupTimeout bounds one cleanup sweep.
	cleanupTimeout = 30 * time.Second
	// failOpenWarnInterval throttles storage-failure warnings to one per minute.
	failOpenWarnInterval = time.Minute
)

// StoreOption tunes a StoreBackedLimiter.
type StoreOption func(*StoreBackedLimiter)

// WithStoreClock replaces the time source, for tests.
func WithStoreClock(now func() time.Time) StoreOption {
	return func(l *StoreBackedLimiter) { l.now = now }
}

// WithCleanupInterval replaces the expired-row sweep interval; zero disables
// the background sweep.
func WithCleanupInterval(interval time.Duration) StoreOption {
	return func(l *StoreBackedLimiter) { l.cleanupInterval = interval }
}

// WithStoreOpTimeout replaces the per-operation database timeout.
func WithStoreOpTimeout(timeout time.Duration) StoreOption {
	return func(l *StoreBackedLimiter) { l.opTimeout = timeout }
}

// StoreBackedLimiter is a Limiter whose counters live in a shared WindowStore.
// It never refuses on a storage failure: an unavailable or contended backend
// admits the request and records a throttled warning, so a database outage
// cannot turn into a site-wide denial.
type StoreBackedLimiter struct {
	policy Policy
	store  WindowStore

	now                func() time.Time
	opTimeout          time.Duration
	cleanupInterval    time.Duration
	stopCleanup        chan struct{}
	cleanupStopped     chan struct{}
	warnMu             sync.Mutex
	lastFailOpenWarn   time.Time
	lastBackgroundWarn time.Time
}

// NewStoreBackedLimiter creates a shared limiter over policy and starts its
// expired-row sweep when the interval is positive. Call Close to stop it.
func NewStoreBackedLimiter(policy Policy, windows WindowStore, options ...StoreOption) *StoreBackedLimiter {
	limiter := &StoreBackedLimiter{
		policy:          policy,
		store:           windows,
		now:             time.Now,
		opTimeout:       defaultStoreOpTimeout,
		cleanupInterval: defaultCleanupInterval,
	}
	for _, option := range options {
		option(limiter)
	}
	if limiter.cleanupInterval > 0 {
		limiter.stopCleanup = make(chan struct{})
		limiter.cleanupStopped = make(chan struct{})
		go limiter.cleanupLoop()
	}
	return limiter
}

// Allowed reports whether cost more units fit without charging.
func (l *StoreBackedLimiter) Allowed(scope Scope, key string, cost int) Decision {
	rule, ok := l.policy.Rule(scope)
	if !ok {
		return Decision{Allowed: true}
	}
	cost = normalizeCost(cost)
	now := l.now()
	ctx, cancel := context.WithTimeout(context.Background(), l.opTimeout)
	defer cancel()

	state, _, err := l.store.ApplyWindow(ctx, scope, key, WindowOp{
		Kind: WindowOpPeek,
		Now:  now,
		Rule: rule,
		Cost: cost,
	})
	if err != nil {
		return l.failOpen(scope, rule, "read", err)
	}
	return decideFromState(rule, now, cost, state, false)
}

// Consume admits and records cost units in one backend atomic operation, so
// callers on different instances cannot both be admitted on the strength of
// the same remaining budget.
func (l *StoreBackedLimiter) Consume(scope Scope, key string, cost int) Decision {
	rule, ok := l.policy.Rule(scope)
	if !ok {
		return Decision{Allowed: true}
	}
	cost = normalizeCost(cost)
	now := l.now()
	ctx, cancel := context.WithTimeout(context.Background(), l.opTimeout)
	defer cancel()

	state, admitted, err := l.store.ApplyWindow(ctx, scope, key, WindowOp{
		Kind: WindowOpConsume,
		Now:  now,
		Rule: rule,
		Cost: cost,
	})
	if err != nil {
		return l.failOpen(scope, rule, "consume", err)
	}
	return decideFromState(rule, now, cost, state, admitted)
}

// Refund gives back cost units reserved earlier in the same window. The
// backend clamps the count at zero, so a refund after the window rolled or
// for a cost that was never charged is harmless and cannot go negative.
func (l *StoreBackedLimiter) Refund(scope Scope, key string, cost int) {
	rule, ok := l.policy.Rule(scope)
	if !ok {
		return
	}
	cost = normalizeCost(cost)
	ctx, cancel := context.WithTimeout(context.Background(), l.opTimeout)
	defer cancel()

	if _, _, err := l.store.ApplyWindow(ctx, scope, key, WindowOp{
		Kind: WindowOpRefund,
		Now:  l.now(),
		Rule: rule,
		Cost: cost,
	}); err != nil {
		l.warnFailure(scope, "refund", err, &l.lastBackgroundWarn)
	}
}

// Hit records cost units against scope and key.
func (l *StoreBackedLimiter) Hit(scope Scope, key string, cost int) {
	rule, ok := l.policy.Rule(scope)
	if !ok {
		return
	}
	cost = normalizeCost(cost)
	ctx, cancel := context.WithTimeout(context.Background(), l.opTimeout)
	defer cancel()

	if _, _, err := l.store.ApplyWindow(ctx, scope, key, WindowOp{
		Kind: WindowOpHit,
		Now:  l.now(),
		Rule: rule,
		Cost: cost,
	}); err != nil {
		l.warnFailure(scope, "hit", err, &l.lastBackgroundWarn)
	}
}

// Close stops the background cleanup sweep. It is safe to call once and may
// be skipped when the limiter was created with cleanup disabled.
func (l *StoreBackedLimiter) Close() {
	if l.stopCleanup == nil {
		return
	}
	close(l.stopCleanup)
	<-l.cleanupStopped
}

// CleanupExpired deletes counters that can no longer overlap a sliding
// window. It is exposed for deterministic testing; the background loop calls
// the same path.
func (l *StoreBackedLimiter) CleanupExpired(ctx context.Context) error {
	now := l.now()
	expiries := make([]WindowExpiry, 0, len(l.policy))
	// A row is dead once it can no longer overlap a sliding window, which is
	// two window durations after its own start; the cutoff is per scope.
	for scope, rule := range l.policy {
		expiries = append(expiries, WindowExpiry{Scope: scope, Before: now.Add(-2 * rule.Window)})
	}
	if _, err := l.store.DeleteExpiredWindows(ctx, expiries); err != nil {
		l.warnFailure("", "cleanup", err, &l.lastBackgroundWarn)
		return err
	}
	return nil
}

// cleanupLoop reaps expired rows on a fixed ticker until Close.
func (l *StoreBackedLimiter) cleanupLoop() {
	defer close(l.cleanupStopped)
	ticker := time.NewTicker(l.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopCleanup:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			// CleanupExpired logs and swallows storage failures on its own.
			_ = l.CleanupExpired(ctx)
			cancel()
		}
	}
}

// decideFromState turns the backend's rolled pre-op view into the same
// Decision the in-memory limiter would return, using the shared entry math.
func decideFromState(rule Rule, now time.Time, cost int, state WindowState, charged bool) Decision {
	view := entryFromState(rule, now, state)
	used := view.estimate(rule, now)
	remaining := max(rule.Limit-used, 0)
	admitted := charged || used+cost <= rule.Limit
	if admitted {
		return Decision{Allowed: true, Rule: rule, Remaining: remaining}
	}
	return Decision{Allowed: false, Rule: rule, Remaining: remaining, RetryAfter: view.retryAfter(rule, now, cost)}
}

// entryFromState rebuilds the in-memory entry shape from a persisted view and
// rolls it defensively; rolling an already-current window is a no-op.
func entryFromState(rule Rule, now time.Time, state WindowState) entry {
	if !state.Found {
		return entry{windowStart: now.Truncate(rule.Window)}
	}
	view := entry{windowStart: state.WindowStart, current: state.Current, previous: state.Previous}
	view.roll(rule, now)
	return view
}

// normalizeCost mirrors the in-memory limiter: non-positive calls cost one.
func normalizeCost(cost int) int {
	if cost < 1 {
		return 1
	}
	return cost
}

// failOpen admits the request and records a throttled storage warning.
func (l *StoreBackedLimiter) failOpen(scope Scope, rule Rule, action string, err error) Decision {
	l.warnFailure(scope, action, err, &l.lastFailOpenWarn)
	return Decision{Allowed: true, Rule: rule, Remaining: rule.Limit}
}

// warnFailure logs at most one storage warning per throttle window so a
// database outage cannot flood the logs.
func (l *StoreBackedLimiter) warnFailure(scope Scope, action string, err error, last *time.Time) {
	l.warnMu.Lock()
	defer l.warnMu.Unlock()
	now := l.now()
	if !last.IsZero() && now.Sub(*last) < failOpenWarnInterval {
		return
	}
	*last = now
	args := []any{slog.String("action", action), slog.String("error", err.Error())}
	if scope != "" {
		args = append(args, slog.String("scope", string(scope)))
	}
	slog.Warn("shared rate limiter storage failed; failing open", args...)
}
