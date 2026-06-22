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
)

// TestControlClientTLS verifies the router dials a TLS control endpoint over https/h2 when
// given an control TLS config (the cross-host control mTLS path; mutual auth itself is
// covered by clustercfg.TestMTLSHandshake).
func TestControlClientTLS(t *testing.T) {
	control := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/control/route" {
			_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", DataEndpoint: "10.0.0.1:1", State: "ready"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	control.EnableHTTP2 = true
	control.StartTLS()
	defer control.Close()

	pool := x509.NewCertPool()
	pool.AddCert(control.Certificate())
	controlTLS := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	addr := strings.TrimPrefix(control.URL, "https://")
	rt := New(addr, "test.local", 0, controlTLS, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if !strings.HasPrefix(rt.controlBase, "https://") {
		t.Fatalf("controlBase should be https for a TLS control endpoint, got %q", rt.controlBase)
	}
	rr := rt.resolveRoute(context.Background(), "sb-1")
	if rr == nil || rr.SID != "sb-1" || rr.DataEndpoint != "10.0.0.1:1" {
		t.Fatalf("resolveRoute over TLS: %+v", rr)
	}
}
