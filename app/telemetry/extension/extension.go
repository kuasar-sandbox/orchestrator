// Package extension defines provider-neutral contracts for one trusted,
// statically linked telemetry extension. Collector-specific APIs live in the
// sibling otel package, not in this ordinary extension surface.
package extension

import (
	"context"
	"time"
)

// Extension starts once before receivers, query serving and route subscription.
// Shutdown is called after ingress and the Collector stop, including when Start
// fails. Both methods must honor their context; retained work belongs to ctx.
type Extension interface {
	Start(context.Context, Host) error
	Shutdown(context.Context) error
}

type Host interface {
	// Reader returns the selected primary reader, or nil in forwarding-only mode.
	Reader() Reader
}

// Field is an envd/E2B guest resource observation, not a host resource quota.
type Field uint8

const (
	CPUCount Field = iota
	CPUUsedPct
	MemTotal
	MemUsed
	MemCache
	DiskTotal
	DiskUsed
	FieldCount
)

// Point contains an observed value; absent points never mean zero.
type Point struct {
	Timestamp time.Time
	Field     Field
	Value     float64
}

type Query struct {
	SandboxID  string
	Start, End time.Time
	Step       time.Duration
}

// Reader is the metrics history domain boundary. SandboxID is always exact;
// implementations must not fall back to StableID. Query may return raw points
// or independently MAX-aggregated fields in epoch-aligned Step buckets.
type Reader interface {
	Bounds(context.Context, string) (start, end time.Time, found bool, err error)
	Query(context.Context, Query) ([]Point, error)
}

// Sample is the canonical scalar storage representation produced exclusively
// by the core Collector exporter. Labels include the trusted sandbox.id and
// sandbox.stable_id. Histogram components are represented as scalar series.
type Sample struct {
	Metric    string
	Labels    map[string]string
	Timestamp time.Time
	Value     float64
}

// Storage is a primary backend: both Collector writes and history reads are
// required. Core calls Write only from its Collector exporter, never a receiver.
// Shutdown is called after all query readers and Collector exporters stop.
type Storage interface {
	Reader
	Write(context.Context, []Sample) error
	Shutdown(context.Context) error
}

// HealthReporter optionally reports an unrecoverable background storage error.
// A report revokes the query registration and stops the telemetry component.
// Transient write/read failures belong to the corresponding operation instead.
type HealthReporter interface{ Errors() <-chan error }
