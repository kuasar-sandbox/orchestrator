package router

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TestRouteLinkClientTLS verifies the router dials a TLS route_link endpoint over https/h2 when
// given a route_link TLS config (the cross-host mTLS path; mutual auth itself is
// covered by clustercfg.TestMTLSHandshake).
func TestRouteLinkClientTLS(t *testing.T) {
	routeLink := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/route" {
			_ = json.NewEncoder(w).Encode(routerTestRouteResolve(t, "sb-1", "/g", "rk", "10.0.0.1:1", types.ProfileE2B))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	routeLink.EnableHTTP2 = true
	routeLink.StartTLS()
	defer routeLink.Close()

	pool := x509.NewCertPool()
	pool.AddCert(routeLink.Certificate())
	routeTLS := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	addr := strings.TrimPrefix(routeLink.URL, "https://")
	rt := New(addr, "test.local", 0, routeTLS, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if !strings.HasPrefix(rt.routeLinkBase, "https://") {
		t.Fatalf("routeLinkBase should be https for a TLS route_link endpoint, got %q", rt.routeLinkBase)
	}
	rr := rt.resolveRoute(context.Background(), "/g", "rk", "sb-1")
	if rr == nil || rr.SandboxID != "sb-1" || rr.NodeSandboxID != "sb-1-g0" || rr.DataEndpoint != "10.0.0.1:1" {
		t.Fatalf("resolveRoute over TLS: %+v", rr)
	}
}
