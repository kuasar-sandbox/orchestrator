// Package extension defines provider-neutral contracts for one trusted,
// statically linked telemetry extension. Collector-specific APIs live in the
// sibling otel package, not in this ordinary extension surface.
package extension

import (
	"context"
	"net/http"
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
	// Reader returns the selected query reader, or nil in write-only mode.
	Reader() Reader
}

// Point contains an observed value; absent points never mean zero.
type Point struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// Selection uses exact metric names and equality attributes. Empty Metrics
// selects all metrics within the exact SandboxID; there is no SQL/PromQL input.
// Attribute names follow the selected backend's documented label mapping.
type Selection struct {
	SandboxID  string
	Metrics    []string
	Attributes map[string]string
}

type Aggregation string

const (
	Raw Aggregation = "raw"
	Max Aggregation = "max"
)

type Query struct {
	Selection
	Start, End  time.Time
	Step        time.Duration
	Aggregation Aggregation
}

// Series contains one complete attribute set. Raw points retain observed time;
// Max independently aggregates this series in epoch-aligned Step buckets after
// filtering raw points to the inclusive range. No gap filling or lookback occurs.
type Series struct {
	Metric     string            `json:"metric"`
	Attributes map[string]string `json:"attributes"`
	Points     []Point           `json:"points"`
}

// Reader is independent of ingestion. SandboxID is always exact, never a
// StableID alias. Bounds applies the same selection to retained observations.
type Reader interface {
	Bounds(context.Context, Selection) (start, end time.Time, found bool, err error)
	Query(context.Context, Query) ([]Series, error)
}

// QueryBackend owns only a reader and its resources. A remote/custom query
// backend is never required to implement Collector writes.
type QueryBackend interface {
	Reader
	Shutdown(context.Context) error
}

// QueryScope is supplied by the HTTP adapter after conductor authorization.
// Reader is permanently bound to SandboxID for this request: a client selector
// cannot replace it, even if the handler uses a different context.
type QueryScope struct {
	SandboxID string
	Reader    Reader
}

// MetricsHandler creates a standard HTTP handler for the trusted request scope.
// Factories are statically linked and should keep request construction cheap.
// A custom response is its own HTTP contract, not automatically E2B compatible.
type MetricsHandler func(QueryScope) http.Handler

// Sample is the local Collector exporter's scalar representation. Sandbox
// sources include accepted identity labels; infrastructure sources may omit them.
// Histogram components are represented as scalar series.
type Sample struct {
	Metric    string
	Labels    map[string]string
	Timestamp time.Time
	Value     float64
}

// HealthReporter optionally reports an unrecoverable background storage error.
// A report revokes the query registration and stops the telemetry component.
// Transient write/read failures belong to the corresponding operation instead.
type HealthReporter interface{ Errors() <-chan error }
