// Package storage defines the backend-independent ordered transaction contract.
// Drivers know bytes, transactions and invalidations, never metadata entities.
package storage

import "context"

// Engine callbacks execute once. Update returns ErrRetry only when definitely
// aborted; the caller decides whether its original preconditions permit retry.
// Unknown commits must never be automatically replayed. Callbacks must not do I/O
// outside this transaction or publish results before Update returns successfully.
type Engine interface {
	View(context.Context, func(ReadTx) error) error
	Update(context.Context, func(Tx) error) error
	Close() error
}

type Value struct {
	Exists bool
	Bytes  []byte
}

// Range is a byte-lexicographic [Begin, End) interval; nil End is unbounded.
// After, when non-nil, is exclusive and must be inside the interval.
type Range struct{ Begin, End, After []byte }
type Limit struct{ Rows, Bytes int }
type Row struct{ Key, Value []byte }
type Page struct {
	Rows []Row
	More bool
}

// ReadTx has a consistent snapshot. Update additionally protects missing keys
// and scanned ranges against serialization anomalies and reads its own writes.
// GetMany preserves input order, duplicates and explicit absent values.
// Scan returns at least one row when nonempty, even if it exceeds the byte limit.
// Returned bytes belong to the caller. Transactions/cursors do not escape the
// callback; a page cursor carries no snapshot across transactions.
type ReadTx interface {
	Get([]byte) (Value, error)
	GetMany([][]byte) ([]Value, error)
	Scan(Range, Limit) (Page, error)
}
type Tx interface {
	ReadTx
	Put([]byte, []byte) error
	Delete([]byte) error
}

type Condition struct {
	Key   []byte
	Value Value
}
type Invalidation interface {
	Done() <-chan struct{}
	Err() error
	Cancel() // waits for resource reclamation
}

// Observer is a separate generic capability. It compares persistent values when
// arming, then invalidates on a change or loss of observation continuity. It is
// not an event stream. Read-modify-read tokens must not be reused (ABA).
type Observer interface {
	Observe(context.Context, []Condition) (Invalidation, error)
	ReserveObservation(context.Context) (context.Context, func(), error)
}
