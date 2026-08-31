package extension

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrTrafficUnavailable means the route identity or complete worker
	// contribution set is not currently available.
	ErrTrafficUnavailable = errors.New("proxy extension: traffic unavailable")
	// ErrTrafficConflict means the current route state cannot produce a traffic
	// observation.
	ErrTrafficConflict = errors.New("proxy extension: traffic state conflict")
)

// TrafficInflight is the aggregate number of open proxy flows.
type TrafficInflight struct {
	Parking uint64
	Egress  uint64
}

// ServiceTrafficView is one service's traffic observation.
type ServiceTrafficView struct {
	Parking   uint64
	Egress    uint64
	IdleSince *time.Time
}

// TrafficView combines the current route identity with the in-process master
// traffic aggregate. All maps and pointers are independent copies.
type TrafficView struct {
	SandboxID string
	RunID     string
	Profile   Profile
	State     RouteState
	Inflight  TrafficInflight
	IdleSince *time.Time
	Services  map[string]ServiceTrafficView
}

// TrafficSource provides point-in-time traffic observations from the proxy
// master's in-process aggregate. A route retained during reconnect can still
// be queried; callers that require freshness must also inspect Routes().
// SyncState(). V1 intentionally has no traffic Watch.
type TrafficSource interface {
	Get(context.Context, string) (TrafficView, error)
}
