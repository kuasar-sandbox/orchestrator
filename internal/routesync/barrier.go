package routesync

import "context"

// RouteBarrier is one ephemeral completion fence for the proxy route stream.
// ID is sent after the route Upsert on the same ordered down stream. Wait only
// observes ACK completion; Commit revalidates that every ACKing registration is
// still the current lease when Create or standalone Connect returns successfully.
type RouteBarrier interface {
	ID() string
	Wait(ctx context.Context) error
	Commit() error
	Cancel()
}

// RouteBarrierCoordinator captures the current traffic-serving proxy leases and
// creates an all-of RouteBarrier for them.
type RouteBarrierCoordinator interface {
	BeginProxyRouteBarrier() (RouteBarrier, error)
}

// PauseActivity is sampled synchronously by each serving worker after its
// pausing gate is visible. Async statistics caches cannot certify idle.
type PauseActivity struct {
	Parking   uint64 `json:"parking"`
	Connected uint64 `json:"connected"`
}

type PauseAck struct {
	Activity map[string]PauseActivity `json:"activity"`
	Error    string                   `json:"error,omitempty"`
}

func (a PauseAck) Idle() bool {
	for _, s := range a.Activity {
		if s.Parking != 0 || s.Connected != 0 {
			return false
		}
	}
	return true
}

type PauseBarrier interface {
	RouteBarrier
	Activity() PauseAck
}

type PauseBarrierCoordinator interface {
	BeginProxyPauseBarrier() (PauseBarrier, error)
}

// PauseSink gates new traffic before synchronously sampling every worker.
type PauseSink interface {
	PreparePause(context.Context, string, RouteEntry) PauseAck
}
