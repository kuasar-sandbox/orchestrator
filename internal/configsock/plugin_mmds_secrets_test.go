package configsock

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// fakeRouteSource is a minimal routesync.Source with an empty route set and
// no live events -- enough for handlePluginRegister to accept a registration
// and start streaming (a Hello + immediate empty-snapshot bookmark), without
// needing a real orchestrator/node-link behind it.
type fakeRouteSource struct{}

func (fakeRouteSource) Range(context.Context, func(routesync.RouteEntry) error) error { return nil }
func (fakeRouteSource) Subscribe() (<-chan routesync.Event, func()) {
	return make(chan routesync.Event), func() {}
}
func (fakeRouteSource) OnWake(context.Context, string) {}
func (fakeRouteSource) Policy() routesync.Policy       { return routesync.Policy{} }

// registerAttempt PUTs a raw Register frame at the plugin plane's
// registration path and returns the response status -- 403 means
// handlePluginRegister rejected it outright (checked before any streaming
// begins); any other code means the registration was accepted and
// ServeStream started (h2c flushes headers before the body is fully
// written, so Do returns promptly either way).
func registerAttempt(t *testing.T, client *http.Client, id string, reg routesync.Register) int {
	t.Helper()
	var buf bytes.Buffer
	if err := routesync.WriteMsg(&buf, &routesync.Msg{Type: routesync.TypeRegister, Register: &reg}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost"+routesync.PluginRegisterPath(id), &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func proxyShapedRegister() routesync.Register {
	return routesync.Register{
		Subscribe:   &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:       &routesync.Proxy{Socket: routesync.Socket{Path: "/tmp/proxy.sock"}},
		MMDSSecrets: true,
	}
}

func TestMMDSSecretsRejectsNonProxyIdentity(t *testing.T) {
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry()})

	reg := proxyShapedRegister() // right shape, wrong id -- still not the proxy
	code := registerAttempt(t, client, "not-the-proxy", reg)
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (id != routesync.ProxyPluginID)", code)
	}
}

func TestMMDSSecretsRejectsMissingProxyCapability(t *testing.T) {
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry()})

	reg := routesync.Register{
		Subscribe:   &routesync.Subscribe{Kind: routesync.KindRouteWake},
		MMDSSecrets: true, // right id, but no Proxy capability -- an observer shape
	}
	code := registerAttempt(t, client, routesync.ProxyPluginID, reg)
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (reg.Proxy == nil)", code)
	}
}

func TestMMDSSecretsRejectsNonRouteWakeSubscribe(t *testing.T) {
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry()})

	reg := routesync.Register{
		Subscribe:   &routesync.Subscribe{Kind: routesync.KindRoute}, // observer kind, not route_wake
		Proxy:       &routesync.Proxy{Socket: routesync.Socket{Path: "/tmp/proxy.sock"}},
		MMDSSecrets: true,
	}
	code := registerAttempt(t, client, routesync.ProxyPluginID, reg)
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (Subscribe.Kind != route_wake)", code)
	}
}

// TestMMDSSecretsAcceptsMatchingProxyShapeWithoutPidfile proves an
// unconfigured mmds_secrets_pidfile does not, on its own, let an ordinary
// route observer through: the registration-shape checks (id/Proxy/Subscribe)
// still gate it. Only a registration that matches all three succeeds.
func TestMMDSSecretsAcceptsMatchingProxyShapeWithoutPidfile(t *testing.T) {
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry()})

	code := registerAttempt(t, client, routesync.ProxyPluginID, proxyShapedRegister())
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (matching proxy shape, no pidfile configured)", code)
	}
}

func TestMMDSSecretsPidfileGate(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "mmds-secrets.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n") // excludes this test process
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry(), MMDSSecretsPidfile: pf})

	code := registerAttempt(t, client, routesync.ProxyPluginID, proxyShapedRegister())
	if code != http.StatusForbidden {
		t.Fatalf("excluded pid: status = %d, want 403", code)
	}

	mustWrite(t, pf, strconv.Itoa(os.Getpid())+"\n") // now allowlisted
	code = registerAttempt(t, client, routesync.ProxyPluginID, proxyShapedRegister())
	if code != http.StatusOK {
		t.Fatalf("allowlisted pid: status = %d, want 200", code)
	}
}

// TestMMDSSecretsFalseNeverGated proves a registration that never asks for
// the capability (MMDSSecrets: false, the ordinary route-observer case) is
// completely unaffected by any of the above -- the shape/pidfile checks only
// ever run when MMDSSecrets is actually requested.
func TestMMDSSecretsFalseNeverGated(t *testing.T) {
	pf := filepath.Join(t.TempDir(), "mmds-secrets.pids")
	mustWrite(t, pf, strconv.Itoa(os.Getpid()+1)+"\n") // excludes this test process; irrelevant here
	_, client := startTestServer(t, Deps{RouteSource: fakeRouteSource{}, Plugins: NewRegistry(), MMDSSecretsPidfile: pf})

	reg := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}} // plain observer, no MMDSSecrets
	code := registerAttempt(t, client, "platform-agent", reg)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (MMDSSecrets not requested, no gating applies)", code)
	}
}
