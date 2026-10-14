package objectcleanup

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffConfig computes the next attempt time after a retryable failure.
type BackoffConfig struct {
	// Base is the delay before the second attempt (attempt == 1).
	Base time.Duration
	// Factor doubles the delay on every subsequent attempt.
	Factor float64
	// Cap bounds the delay.
	Cap time.Duration
	// Jitter randomizes the delay within +/-Jitter fraction to spread retries.
	// Zero disables jitter (deterministic tests).
	Jitter float64
}

// DefaultBackoff returns the production policy: 30s base, doubling, 1h cap,
// +/-20% jitter.
func DefaultBackoff() BackoffConfig {
	return BackoffConfig{Base: 30 * time.Second, Factor: 2, Cap: time.Hour, Jitter: 0.2}
}

// Delay returns the wait for a task whose failed attempt count is attempt
// (1-based: the first failure reschedules after Base).
func (c BackoffConfig) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	factor := c.Factor
	if factor <= 1 {
		factor = 2
	}
	delay := float64(c.Base) * math.Pow(factor, float64(attempt-1))
	capSeconds := float64(c.Cap)
	if capSeconds > 0 && delay > capSeconds {
		delay = capSeconds
	}
	if c.Jitter > 0 {
		// Uniform in [1-Jitter, 1+Jitter).
		delta := (rand.Float64()*2 - 1) * c.Jitter
		delay *= 1 + delta
		if delay < 0 {
			delay = 0
		}
		if capSeconds > 0 && delay > capSeconds {
			delay = capSeconds
		}
	}
	return time.Duration(delay)
}

// NextRun is the timestamp at which a task with the given attempt count
// becomes due again.
func (c BackoffConfig) NextRun(attempt int, now time.Time) time.Time {
	return now.Add(c.Delay(attempt))
}
