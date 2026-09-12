package configsock

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

var errTelemetryLease = errors.New("telemetry query lease unavailable")

func validLocalSocket(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && len(path) <= 107 &&
		!strings.ContainsAny(path, "\x00\r\n") && path != "/"
}

// telemetryTarget snapshots an endpoint and its revocable lease together. A
// pathname alone is not authority: even a previously returned target is invalid
// as soon as the registration disconnects or is replaced.
func (r *Registry) telemetryTarget() (string, context.Context, bool) {
	if r == nil {
		return "", nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.m[routesync.TelemetryPluginID]
	if p == nil || p.lease == nil || p.lease.Err() != nil || p.Caps.Telemetry == nil || p.Caps.Telemetry.API == nil ||
		!validLocalSocket(p.Caps.Telemetry.API.Path) {
		return "", nil, false
	}
	return p.Caps.Telemetry.API.Path, p.lease, true
}

// TelemetryAPI forwards opaque HTTP requests to the current trusted telemetry
// UDS. It owns no metrics schema, query interpretation, or sandbox lifecycle.
// Callers MUST perform normal API authentication and exact sandbox ownership
// lookup before invoking it.
func (r *Registry) TelemetryAPI() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		path, lease, ok := r.telemetryTarget()
		if !ok {
			telemetryUnavailable(w)
			return
		}
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		stop := context.AfterFunc(lease, cancel)
		defer stop()
		// A transport belongs to exactly one request and registration. It cannot
		// pool a connection across lease epochs, even when the UDS path is reused.
		transport := &http.Transport{
			MaxConnsPerHost:       1,
			DisableCompression:    true,
			ResponseHeaderTimeout: 30 * time.Second,
			DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
				if lease.Err() != nil {
					return nil, errTelemetryLease
				}
				conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", path)
				if err != nil {
					return nil, err
				}
				if lease.Err() != nil {
					_ = conn.Close()
					return nil, errTelemetryLease
				}
				return conn, nil
			},
		}
		defer transport.CloseIdleConnections()
		proxy := httputil.ReverseProxy{
			Transport: transport,
			Rewrite: func(p *httputil.ProxyRequest) {
				p.Out.URL.Scheme, p.Out.URL.Host = "http", "telemetry"
				// This is a query channel, never a protocol-upgrade tunnel.
				p.Out.Header.Del("Connection")
				p.Out.Header.Del("Upgrade")
				p.Out.Header.Del("Te")
				// Rewrite normally cleans malformed query strings. This boundary
				// deliberately leaves all query validation to telemetry.
				p.Out.URL.RawQuery = p.In.URL.RawQuery
			},
			ModifyResponse: func(response *http.Response) error {
				if response.StatusCode == http.StatusSwitchingProtocols {
					return errors.New("telemetry query channel cannot upgrade")
				}
				if lease.Err() != nil {
					return errTelemetryLease
				}
				return ctx.Err()
			},
			ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
				telemetryUnavailable(w)
			},
		}
		proxy.ServeHTTP(w, request.WithContext(ctx))
	})
}

func telemetryUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"message":"telemetry unavailable"}` + "\n"))
}
