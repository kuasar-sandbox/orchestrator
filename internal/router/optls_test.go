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

// TestOpClientTLS verifies the router dials a TLS op endpoint over https/h2 when
// given an op TLS config (the cross-host op-mTLS path; mutual auth itself is
// covered by clustercfg.TestMTLSHandshake).
func TestOpClientTLS(t *testing.T) {
	op := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/op/route" {
			_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", DataEndpoint: "10.0.0.1:1", State: "ready"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	op.EnableHTTP2 = true
	op.StartTLS()
	defer op.Close()

	pool := x509.NewCertPool()
	pool.AddCert(op.Certificate())
	opTLS := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	addr := strings.TrimPrefix(op.URL, "https://")
	rt := New(addr, "test.local", 0, opTLS, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if !strings.HasPrefix(rt.opBase, "https://") {
		t.Fatalf("opBase should be https for a TLS op endpoint, got %q", rt.opBase)
	}
	rr := rt.resolveRoute(context.Background(), "sb-1")
	if rr == nil || rr.SID != "sb-1" || rr.DataEndpoint != "10.0.0.1:1" {
		t.Fatalf("resolveRoute over TLS: %+v", rr)
	}
}
