package membergroup

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPMemberlistJoinAndMetaUpdate(t *testing.T) {
	hubA, srvA := testHub(t)
	hubB, srvB := testHub(t)
	gA, err := New(Options{
		Label: "registry.1.test", Name: "a", Hub: hubA, FastTimers: true,
		Seeds: map[string]string{"b": srvB.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "a", Advertise: srvA.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gA.Shutdown() })
	gB, err := New(Options{
		Label: "registry.1.test", Name: "b", Hub: hubB, FastTimers: true,
		Seeds: map[string]string{"a": srvA.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "b", Advertise: srvB.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gB.Shutdown() })
	if n, err := gB.Join("a"); err != nil || n != 1 {
		t.Fatalf("join n=%d err=%v", n, err)
	}
	waitFor(t, func() bool { return gA.Alive("b") && gB.Alive("a") })

	if err := gB.UpdateMeta(Meta{
		Role: RoleScaler, ID: "b", Advertise: srvB.URL,
		Ready: true, ReadyLabel: "registry.2.test",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		scalers := gA.ReadyScalers("registry.2.test")
		return len(scalers) == 1 && scalers[0].ID == "b" && scalers[0].Advertise == srvB.URL
	})
}

func TestHTTPMemberlistLabelsAreIsolated(t *testing.T) {
	hubA, srvA := testHub(t)
	hubB, srvB := testHub(t)
	gA, err := New(Options{
		Label: "registry.1.test", Name: "a", Hub: hubA, FastTimers: true,
		Seeds: map[string]string{"b": srvB.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "a", Advertise: srvA.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gA.Shutdown() })
	gB, err := New(Options{
		Label: "registry.2.test", Name: "b", Hub: hubB, FastTimers: true,
		Seeds: map[string]string{"a": srvA.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "b", Advertise: srvB.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gB.Shutdown() })
	if n, err := gB.Join("a"); err == nil || n != 0 {
		t.Fatalf("cross-label join n=%d err=%v, want failure", n, err)
	}
}

func TestHTTPMemberlistJoinOverTLS(t *testing.T) {
	hubA, srvA := testTLSHub(t)
	hubB, srvB := testTLSHub(t)
	pool := x509.NewCertPool()
	pool.AddCert(srvA.Certificate())
	pool.AddCert(srvB.Certificate())
	tlsCfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2", "http/1.1"}}
	gA, err := New(Options{
		Label: "registry.1.test", Name: "a", Hub: hubA, FastTimers: true, TLSConfig: tlsCfg,
		Seeds: map[string]string{"b": srvB.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "a", Advertise: srvA.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gA.Shutdown() })
	gB, err := New(Options{
		Label: "registry.1.test", Name: "b", Hub: hubB, FastTimers: true, TLSConfig: tlsCfg,
		Seeds: map[string]string{"a": srvA.URL},
		Meta:  Meta{Role: RoleRegistry, ID: "b", Advertise: srvB.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gB.Shutdown() })
	if n, err := gB.Join("a"); err != nil || n != 1 {
		t.Fatalf("tls join n=%d err=%v", n, err)
	}
	waitFor(t, func() bool { return gA.Alive("b") && gB.Alive("a") })
}

func testHub(t *testing.T) (*Hub, *httptest.Server) {
	t.Helper()
	hub := NewHub()
	mux := http.NewServeMux()
	hub.Mount(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return hub, srv
}

func testTLSHub(t *testing.T) (*Hub, *httptest.Server) {
	t.Helper()
	hub := NewHub()
	mux := http.NewServeMux()
	hub.Mount(mux)
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return hub, srv
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
