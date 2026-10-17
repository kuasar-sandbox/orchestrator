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
// The returned handler may add, rewrite, or replace stats-socket routes except
// the core-owned /readyz and /status paths;
// returning nil fails startup.
type ManagementWrapper interface {
	WrapManagement(http.Handler) http.Handler
}

// Shutdown is an optional capability of the configured master or worker
// extension. After all data and MMDS business has drained, the core cancels the
// Start context and calls Shutdown once to join background work. ctx is the
// remaining original shutdown budget, not a new timeout. Implementations must
// return by its deadline; an expired or failed callback forces process cleanup.
// InplaceRestart requires this capability even when signal triggering is off.
// Ordinary startup remains compatible with extensions implementing only Start.
type Shutdown interface {
	Shutdown(context.Context) error
}
