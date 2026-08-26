// Package prefetch implements the node-ctl ahead-of-time OS page-cache warming
// hint. The control plane (Nacre Agent) signals that a snapshot template is
// expected to be activated soon; node-ctl asynchronously pulls the snapshot
// artifacts into the kernel page cache so the later restore reads from resident
// pages instead of a cold NFS read.
//
// The interface (Warmer) is deliberately stable: it is what survives the future
// chunked-cache/dedup backend. The resolver and the page-inner are the
// disposable local stopgap and may be replaced wholesale without touching the
// contract. Scheduling remains entirely with the caller; node-ctl is best-effort
// and never blocks a later activation.
package prefetch

import (
	"context"
	"time"
)

// PrefetchReq is the control-plane hint. Reference is intentionally opaque: the
// resolver owns interpreting it. Today only the e2b-snp-... template-id form is
// understood; a future branch (e.g. migration token) adds another resolver case
// without changing the API shape.
type PrefetchReq struct {
	Reference string `json:"reference"`
}

// Result is the best-effort outcome of a hint. It is intentionally minimal and
// backend-agnostic (a future chunked-cache backend may not be file-shaped).
type Result struct {
	RequestID   string     `json:"requestID"`
	State       string     `json:"state"` // queued | fetching | fetched | failed
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// Warmer is the data-plane-facing capability node-ctl exposes to the control
// plane. Implementations are swappable; the surface is stable.
type Warmer interface {
	// Warm queues the prefetch for reference and returns its tracked request.
	// Concurrent duplicate references coalesce to the same request ID.
	Warm(ctx context.Context, req PrefetchReq) (Result, error)
	// Status returns the tracked request, if known.
	Status(requestID string) (Result, bool)
	// Close releases the worker pool. Idempotent.
	Close() error
}
