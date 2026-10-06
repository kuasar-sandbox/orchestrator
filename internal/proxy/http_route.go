package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// dialHTTPRoute may select a new transport only while no HTTP bytes have been
// written to a guest. Its returned context also closes a started exchange when
// that route is withdrawn, but ForwardHTTP never replays an exchange.
func (p *Proxy) dialHTTPRoute(w http.ResponseWriter, r *http.Request, binding RouteBinding, route Route, request AuthorizedForwardRequest) (net.Conn, *http.Request, context.CancelFunc, bool) {
	for {
		if !p.revalidateForward(w, r, request) {
			return nil, nil, nil, false
		}
		ctx, stop := r.Context(), func() {}
		if watcher, ok := p.router.(RouteWatcher); ok {
			ctx, stop = watcher.WatchRoute(ctx, binding, route)
		}
		backend, err := p.dial(ctx, route)
		if err == nil && ctx.Err() == nil {
			return backend, r.WithContext(ctx), stop, true
		}
		// A dial can complete concurrently with invalidation. Do not hand its
		// connection to ForwardHTTP after observing that its route was retired.
		if backend != nil {
			_ = backend.Close()
		}
		changed := errors.Is(context.Cause(ctx), ErrRouteChanged)
		stop()
		if r.Context().Err() != nil || !changed {
			p.mx.Inc(`data_requests_total{result="upstream_error"}`)
			writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
			return nil, nil, nil, false
		}
		var ok bool
		route, ok = p.resolveActiveRoute(w, r, binding)
		if !ok {
			return nil, nil, nil, false
		}
	}
}

func (p *Proxy) revalidateForward(w http.ResponseWriter, r *http.Request, request AuthorizedForwardRequest) bool {
	if request.Revalidate == nil {
		return true
	}
	if err := request.Revalidate(r.Context()); err != nil {
		if p.log != nil {
			p.log.Warn("proxy extension private authorization became stale",
				"sandbox_id", request.SandboxID, "service", request.Target.Service,
				"port", request.Target.Port, "err", err)
		}
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusConflict, "private authorization became stale", ProxyErrorStale)
		return false
	}
	return true
}
