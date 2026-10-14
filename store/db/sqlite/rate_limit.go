package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pkg/errors"
	msqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/usememos/memos/store"
)

// rateLimitWindowMaxAttempts bounds the retries on a lost write lock or a
// lost insert race when several processes charge one counter at once.
const rateLimitWindowMaxAttempts = 8

// ApplyRateLimitWindow rolls and atomically updates one rate-limit counter.
// SQLite transactions begin IMMEDIATE (see NewDB), so concurrent writers
// serialize on the database lock instead of overwriting each other's
// read-modify-write cycle.
func (d *DB) ApplyRateLimitWindow(ctx context.Context, mutation *store.RateLimitWindowMutation) (*store.RateLimitWindow, bool, error) {
	if mutation.Op == store.RateLimitWindowPeek {
		return peekSQLiteRateLimitWindow(ctx, d.db, mutation)
	}

	var lastErr error
	for attempt := range rateLimitWindowMaxAttempts {
		state, admitted, err := applySQLiteRateLimitWindowOnce(ctx, d.db, mutation)
		if err == nil {
			return state, admitted, nil
		}
		lastErr = err
		if !isSQLiteRateLimitWindowRetryable(err) {
			return nil, false, err
		}
		if err := sleepRateLimitWindowBackoff(ctx, attempt); err != nil {
			return nil, false, err
		}
	}
	return nil, false, errors.Wrap(lastErr, "rate limit counter update exhausted retries")
}

type sqliteRateLimitWindowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// peekSQLiteRateLimitWindow returns the rolled counter view without taking a
// write lock; concurrent reads never block.
func peekSQLiteRateLimitWindow(ctx context.Context, querier sqliteRateLimitWindowQuerier, mutation *store.RateLimitWindowMutation) (*store.RateLimitWindow, bool, error) {
	windowStart := mutation.WindowStart()
	state := &store.RateLimitWindow{WindowStart: windowStart}

	var storedStartTs int64
	var current, previous int
	err := querier.QueryRowContext(ctx, `
		SELECT window_start_ts, current_count, previous_count
		FROM rate_limit_counter
		WHERE scope = ? AND counter_key = ?
	`, mutation.Scope, mutation.Key).Scan(&storedStartTs, &current, &previous)
	switch {
	case err == nil:
		state.Found = true
		state.Current, state.Previous = store.RollRateLimitWindow(time.Unix(storedStartTs, 0), windowStart, mutation.Window, current, previous)
	case errors.Is(err, sql.ErrNoRows):
		// An unseen key starts the current window with no usage.
	default:
		return nil, false, errors.Wrap(err, "failed to read rate limit counter")
	}
	return state, false, nil
}

// applySQLiteRateLimitWindowOnce runs the read-modify-write in one IMMEDIATE
// transaction. The returned state is the rolled pre-op view; the bool reports
// whether a consume mutation charged its cost.
func applySQLiteRateLimitWindowOnce(ctx context.Context, db *sql.DB, mutation *store.RateLimitWindowMutation) (*store.RateLimitWindow, bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to begin rate limit counter transaction")
	}
	defer func() { _ = tx.Rollback() }()

	state, err := lockSQLiteRateLimitWindow(ctx, tx, mutation)
	if err != nil {
		return nil, false, err
	}

	current, previous := state.Current, state.Previous
	admitted := false
	write := true

	switch mutation.Op {
	case store.RateLimitWindowHit:
		current += mutation.Cost
	case store.RateLimitWindowConsume:
		used := store.RateLimitWindowEstimate(current, previous, mutation.Now, state.WindowStart, mutation.Window)
		if used+mutation.Cost > mutation.Limit {
			// A refusal charges nothing and writes nothing.
			write = false
			break
		}
		admitted = true
		current += mutation.Cost
	case store.RateLimitWindowRefund:
		if !state.Found {
			// Nothing reserved for an unseen key; mirror the in-memory no-op.
			write = false
			break
		}
		current = max(current-mutation.Cost, 0)
	default:
		// An unknown operation is a read-only no-op.
		write = false
	}

	if write {
		if err := writeSQLiteRateLimitWindow(ctx, tx, mutation, state.Found, current, previous); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, errors.Wrap(err, "failed to commit rate limit counter transaction")
	}

	// The returned view is intentionally the pre-op state captured under the
	// lock; the limiter computes Remaining and RetryAfter from it.
	return state, admitted, nil
}

// lockSQLiteRateLimitWindow reads the row under the transaction's write lock
// and rolls it to the mutation's window.
func lockSQLiteRateLimitWindow(ctx context.Context, tx *sql.Tx, mutation *store.RateLimitWindowMutation) (*store.RateLimitWindow, error) {
	windowStart := mutation.WindowStart()
	state := &store.RateLimitWindow{WindowStart: windowStart}

	var storedStartTs int64
	var current, previous int
	err := tx.QueryRowContext(ctx, `
		SELECT window_start_ts, current_count, previous_count
		FROM rate_limit_counter
		WHERE scope = ? AND counter_key = ?
	`, mutation.Scope, mutation.Key).Scan(&storedStartTs, &current, &previous)
	switch {
	case err == nil:
		state.Found = true
		state.Current, state.Previous = store.RollRateLimitWindow(time.Unix(storedStartTs, 0), windowStart, mutation.Window, current, previous)
	case errors.Is(err, sql.ErrNoRows):
		// An unseen key starts the current window with no usage.
	default:
		return nil, errors.Wrap(err, "failed to read rate limit counter")
	}
	return state, nil
}

// writeSQLiteRateLimitWindow inserts the row when this transaction created
// the key and updates it otherwise. The CHECK constraints keep both counts
// non-negative even against a buggy caller.
func writeSQLiteRateLimitWindow(ctx context.Context, tx *sql.Tx, mutation *store.RateLimitWindowMutation, found bool, current, previous int) error {
	windowStartTs := mutation.WindowStart().Unix()
	updatedTs := mutation.Now.Unix()
	if !found {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO rate_limit_counter
				(scope, counter_key, window_start_ts, current_count, previous_count, updated_ts)
			VALUES (?, ?, ?, ?, ?, ?)
		`, mutation.Scope, mutation.Key, windowStartTs, current, previous, updatedTs); err != nil {
			return errors.Wrap(err, "failed to insert rate limit counter")
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE rate_limit_counter
		SET window_start_ts = ?, current_count = ?, previous_count = ?, updated_ts = ?
		WHERE scope = ? AND counter_key = ?
	`, windowStartTs, current, previous, updatedTs, mutation.Scope, mutation.Key); err != nil {
		return errors.Wrap(err, "failed to update rate limit counter")
	}
	return nil
}

// DeleteExpiredRateLimitWindows removes rows past their scope cutoff.
func (d *DB) DeleteExpiredRateLimitWindows(ctx context.Context, expiries []store.RateLimitWindowExpiry) (int64, error) {
	if len(expiries) == 0 {
		return 0, nil
	}
	clauses := make([]string, 0, len(expiries))
	args := make([]any, 0, len(expiries)*2)
	for _, expiry := range expiries {
		clauses = append(clauses, "(scope = ? AND window_start_ts <= ?)")
		args = append(args, expiry.Scope, expiry.Before.Unix())
	}
	result, err := d.db.ExecContext(ctx, `
		DELETE FROM rate_limit_counter
		WHERE `+strings.Join(clauses, " OR "),
		args...,
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to delete expired rate limit counters")
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to count deleted rate limit counters")
	}
	return deleted, nil
}

// isSQLiteRateLimitWindowRetryable reports a lost write lock or insert race
// the caller may repeat; every other error (a malformed statement, a full
// disk) fails through to the limiter's fail-open path.
func isSQLiteRateLimitWindowRetryable(err error) bool {
	var sqliteErr *msqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_CONSTRAINT_UNIQUE:
		return true
	default:
		return false
	}
}

// sleepRateLimitWindowBackoff waits a growing, context-aware pause before a retry.
func sleepRateLimitWindowBackoff(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
