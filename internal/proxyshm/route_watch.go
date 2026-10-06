package proxyshm

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// WatchRoute observes only this admitted ordinary HTTP transport. Connector
// generation remains responsible for packet isolation; this local cancellation
// keeps obsolete dials and exchanges from waiting for network timeouts.
func (v *WorkerView) WatchRoute(ctx context.Context, expected proxy.RouteBinding, current proxy.Route) (context.Context, context.CancelFunc) {
	transportCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	rev := v.table.Rev()
	valid := v.transportRouteCurrent(expected, current)
	if !valid {
		cancel(proxy.ErrRouteChanged)
		close(done)
	} else {
		go func() {
			defer close(done)
			for transportCtx.Err() == nil {
				// Notifications normally wake this immediately. Re-sample the
				// cheap SHM revision even if a notification was coalesced/lost;
				// this is not a timeout for an otherwise healthy HTTP exchange.
				if !v.waitChange(transportCtx, time.Now().Add(50*time.Millisecond), rev) {
					continue
				}
				rev = v.table.Rev()
				if !v.transportRouteCurrent(expected, current) {
					cancel(proxy.ErrRouteChanged)
					return
				}
			}
		}()
	}
	return transportCtx, func() {
		cancel(context.Canceled)
		<-done
	}
}

func (v *WorkerView) transportRouteCurrent(expected proxy.RouteBinding, current proxy.Route) bool {
	r, found := v.table.Lookup(expected.SandboxID)
	binding, present := workerRouteBinding(r, found, expected.Target)
	// Admission and policy changes govern later acquires, not a transport that
	// already owns its flow. Observe only the current target's authorization;
	// unrelated service credentials, limits and arena generations may change.
	// Re-activation still uses the existing full binding/admission validation.
	return present && r.State == routesync.StateRunning &&
		binding.SandboxID == expected.SandboxID && binding.StableID == expected.StableID &&
		binding.Profile == expected.Profile && binding.Target == expected.Target &&
		binding.Kind == expected.Kind && binding.ExpectedAccessToken == expected.ExpectedAccessToken &&
		workerDialRoute(r, expected) == current
}

var _ proxy.RouteWatcher = (*WorkerView)(nil)
