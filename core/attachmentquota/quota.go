// Package attachmentquota holds the pure business rules for per-user
// attachment byte quotas. It must stay free of HTTP, SQL, and store
// dependencies: callers pass primitive settings and receive primitive
// decisions or a plain error type.
package attachmentquota

// EffectiveLimit resolves the effective per-user attachment quota.
//
//   - A non-nil override always enables enforcement with that limit. An
//     override of zero explicitly blocks every new upload.
//   - With no override, instanceDefaultBytes enables enforcement when it is
//     positive; zero (the default) disables quotas.
//   - Negative inputs other than nil are treated as "unset" by callers and
//     must not reach this function.
func EffectiveLimit(instanceDefaultBytes int64, override *int64) (limit int64, enabled bool) {
	if override != nil {
		return *override, true
	}
	return instanceDefaultBytes, instanceDefaultBytes > 0
}

// Remaining returns the bytes still writable under limit, clamped at zero.
// Callers must only use it when a quota is enabled; a user already over limit
// gets zero, never a negative allowance.
func Remaining(usedBytes, limitBytes int64) int64 {
	if usedBytes >= limitBytes {
		return 0
	}
	return limitBytes - usedBytes
}

// Fits reports whether requestedBytes may be added on top of usedBytes
// without exceeding limitBytes. Equality (a write that fills the quota
// exactly) is allowed. A zero limit rejects every write, including empty
// files, since it explicitly bans uploads.
func Fits(usedBytes, limitBytes, requestedBytes int64) bool {
	return limitBytes > 0 && usedBytes+requestedBytes <= limitBytes
}

// ExceededError is the transport-agnostic quota rejection. The server layer
// translates it into a gRPC ResourceExhausted status carrying structured
// quota details.
type ExceededError struct {
	// UsedBytes is the persisted usage observed at rejection.
	UsedBytes int64
	// LimitBytes is the effective limit enforced at rejection.
	LimitBytes int64
	// RequestedBytes is the size the rejected write attempted to add.
	RequestedBytes int64
	// LimitOverridden reports whether the limit came from a per-user override.
	LimitOverridden bool
}

// Error implements the error interface.
func (e *ExceededError) Error() string {
	return "attachment quota exceeded"
}
