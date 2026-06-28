// Package clusterstore is a small in-process KV/change-log used for local
// registry projections such as WATCH_LIST logs. route_link, node_link, and build
// execution state live in the registry-owned replicated kernel; this store stays
// opaque and process-local.
package clusterstore

import (
	"context"
	"errors"
)

// EventType is the kind of mutation a Watch reports.
type EventType int

const (
	// EventPut is a create or update.
	EventPut EventType = iota + 1
	// EventDelete is a removal (explicit or via lease expiry).
	EventDelete
)

// Event is a single mutation delivered on a Watch channel, in revision order.
type Event struct {
	Type  EventType
	Key   string
	Value []byte // the new value for EventPut; nil for EventDelete
	Rev   int64  // store revision at which this mutation happened
}

// KV is a key and its current value + the revision it was last modified at.
type KV struct {
	Key    string
	Value  []byte
	ModRev int64
}

// LeaseID identifies a lease. Keys written with PutLeased are deleted when the
// lease expires (no KeepAlive within its TTL) or is Revoked.
type LeaseID int64

// ErrCompacted is returned by Watch when fromRev is older than the retained
// change-log: the caller must Range the prefix for a full snapshot and watch
// from the current revision instead.
var ErrCompacted = errors.New("clusterstore: revision compacted")

// Store is the cluster registry's KV contract. Implementations are safe for
// concurrent use. Values are opaque; callers marshal their records.
type Store interface {
	// Get returns the value at key; found is false if the key is absent.
	Get(ctx context.Context, key string) (kv KV, found bool, err error)

	// Put sets key=val (unleased), returning the new store revision.
	Put(ctx context.Context, key string, val []byte) (rev int64, err error)

	// PutLeased sets key=val bound to lease; the key is deleted when the lease
	// expires or is revoked. A zero lease behaves like Put.
	PutLeased(ctx context.Context, key string, val []byte, lease LeaseID) (rev int64, err error)

	// Delete removes key (no-op if absent). Returns the new revision, or 0 if
	// nothing was removed.
	Delete(ctx context.Context, key string) (rev int64, err error)

	// CAS sets key=val only if the key's current ModRev equals expectRev.
	// expectRev == 0 means create-only (succeed iff the key is absent).
	// Returns (newRev, true) on success or (0, false) on a revision mismatch.
	CAS(ctx context.Context, key string, expectRev int64, val []byte) (rev int64, ok bool, err error)

	// Range streams every KV whose key has the given prefix, in key order. fn
	// must be read-only with respect to the store (collect, then act after
	// Range returns). A non-nil fn error stops iteration and is returned.
	Range(ctx context.Context, prefix string, fn func(KV) error) error

	// Watch streams mutations under prefix. fromRev <= 0 streams live events
	// from the current revision onward; fromRev > 0 first replays the
	// change-log strictly after fromRev (ErrCompacted if it has been pruned),
	// then continues live. The channel closes when ctx is done or the watcher
	// falls too far behind (a full-resync signal — Range then re-Watch).
	Watch(ctx context.Context, prefix string, fromRev int64) (<-chan Event, error)

	// Rev returns the current store revision.
	Rev(ctx context.Context) (int64, error)

	// Grant creates a lease with the given TTL (seconds).
	Grant(ctx context.Context, ttlSec int64) (LeaseID, error)
	// KeepAlive resets a lease's TTL. Returns an error if the lease is gone.
	KeepAlive(ctx context.Context, lease LeaseID, ttlSec int64) error
	// Revoke deletes a lease and all keys bound to it (emitting EventDelete).
	Revoke(ctx context.Context, lease LeaseID) error

	// Close releases the backend (stops the lease sweeper, closes the db).
	Close() error
}
