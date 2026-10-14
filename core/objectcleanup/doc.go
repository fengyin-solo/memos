// Package objectcleanup holds the storage-agnostic rules for retrying
// external attachment object deletion and for reconciling the cleanup ledger
// against actual storage state.
//
// It must stay free of HTTP and SQL concerns: server workers and store
// drivers supply the inputs and apply the decisions.
package objectcleanup
