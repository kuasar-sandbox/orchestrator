// Package extension defines the public runtime-extension contract for a
// statically linked proxy. Extensions are trusted, in-process code; these
// interfaces organize access to core state without exposing internal packages.
package extension

import (
	"context"
	"net/http"
)

// MasterExtension is the one statically linked runtime extension for a proxy
// master process. Start is called exactly once after shared state is created
// and before listeners, route synchronization, or workers start. Cancellation
// of ctx is the extension's process-shutdown notification.
type MasterExtension interface {
	Start(context.Context, MasterHost) error
}

// MasterHost exposes proxy-master-owned route and traffic sources.
type MasterHost interface {
	Routes() RouteSource
	Traffic() TrafficSource
}

// ManagementWrapper is an optional capability implemented by the same
// MasterExtension object. WrapManagement is called once after Start succeeds.
// The returned handler may add, rewrite, or replace any stats-socket route;
// returning nil fails startup.
type ManagementWrapper interface {
	WrapManagement(http.Handler) http.Handler
}
