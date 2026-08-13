package routesync

import "context"

// RouteBarrier is one ephemeral completion fence for the proxy route stream.
// ID is sent after the route Upsert on the same ordered down stream. Wait only
// observes ACK completion; Commit revalidates that every ACKing registration is
// still the current lease at the Create linearization point.
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
