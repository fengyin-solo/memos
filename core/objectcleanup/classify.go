package objectcleanup

import (
	storepb "github.com/usememos/memos/proto/gen/store"
)

// Outcome is the worker's decision for one failed object deletion attempt.
type Outcome int

const (
	// OutcomeRetry reschedules the task with exponential backoff. The object
	// is still expected to exist.
	OutcomeRetry Outcome = iota
	// OutcomeSucceed treats the attempt as successful (used when the object is
	// already gone).
	OutcomeSucceed
	// OutcomeFailTerminal records a FAILED terminal state; the task is never
	// claimed again.
	OutcomeFailTerminal
)

// terminalS3ErrorCodes names deterministic S3 API refusals that retries
// cannot fix: credentials/permissions, malformed requests and a missing
// bucket. Transient codes (5xx, throttling, timeouts) surface as non-API
// errors or other codes and stay retryable.
var terminalS3ErrorCodes = map[string]struct{}{
	"AccessDenied":    {},
	"InvalidArgument": {},
	"NoSuchBucket":    {},
}

// ClassifyDeleteError maps a deletion failure to an outcome.
//
// apiErrorCode is the storage API error code (e.g. a smithy APIError code)
// extracted by the caller; it is empty when the request never produced an API
// response (DNS failure, connection reset, timeout, context cancellation).
// Local filesystem errors other than "not found" (which the store layer
// already treats as success) remain retryable: permission or IO errors can be
// transient on networked filesystems.
func ClassifyDeleteError(storageType storepb.AttachmentStorageType, apiErrorCode string) Outcome {
	if apiErrorCode == "" {
		return OutcomeRetry
	}
	if storageType != storepb.AttachmentStorageType_S3 {
		return OutcomeRetry
	}
	if _, terminal := terminalS3ErrorCodes[apiErrorCode]; terminal {
		return OutcomeFailTerminal
	}
	return OutcomeRetry
}
