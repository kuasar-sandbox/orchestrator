package orch

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const testMMDSRouteSpec = `{"version":1,"secrets":[{"name":"key1"}],"services":[{"name":"svc1","target":"svc1"}],"routes":[` +
	`{"path":"/static","type":"static","content_type":"text/plain","data":"D"},` +
	`{"path":"/secret","type":"secret","secret_name":"key1"},` +
	`{"path":"/service","type":"service","service_name":"svc1"}]}`

// startFakeMMDSServiceForOrch serves handler over a real Unix domain socket
// and returns a mmdssvc.Registry with one entry, "svc1" -- matching the
// service target testMMDSRouteSpec's "/service" route resolves to.
func startFakeMMDSServiceForOrch(t *testing.T, handler http.HandlerFunc, timeout time.Duration) mmdssvc.Registry {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "svc.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return mmdssvc.Registry{"svc1": {SocketPath: sockPath, Timeout: timeout, MaxResponseBytes: 64 * 1024}}
}

func cachedTestSandboxForMMDSRoute(t *testing.T, o *Orchestrator, id string, parkTimeout time.Duration) *types.Sandbox {
	t.Helper()
	mk := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID:          id,
		Profile:     types.ProfileBare,
		TemplateID:  "bare-img-" + strings.Repeat("b", 64),
		State:       types.StateRunning,
		APISecret:   deriveTestAPISecret(t, mk),
		ManifestKey: mk,
		RunDir:      filepath.Join(t.TempDir(), "run"),
		BaseDir:     filepath.Join(t.TempDir(), "lib"),
		CreatedUnix: 1,
		Metadata:    map[string]string{sandboxcfg.NsMMDS: testMMDSRouteSpec},
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	if parkTimeout > 0 {
		o.secretWait = newMMDSSecretWaiter(parkTimeout)
	}
	return sb
}

func TestMMDSRouteUnknownSandbox(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	_, ok, err := o.MMDSRoute("no-such-sandbox", "/static")
	if ok || err != nil {
		t.Fatalf("ok=%t err=%v, want false/nil", ok, err)
	}
}

func TestMMDSRouteUnspecifiedPath(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	_, ok, err := o.MMDSRoute("sbx-1", "/nope")
	if ok || err != nil {
		t.Fatalf("ok=%t err=%v, want false/nil", ok, err)
	}
}

func TestMMDSRouteStaticServesDirectlyNoStoreInvolved(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	route, ok, err := o.MMDSRoute("sbx-1", "/static")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Type != "static" || route.ContentType != "text/plain" || route.Data != "D" {
		t.Fatalf("route = %+v", route)
	}
}

func TestMMDSRouteSecretServesConfiguredValue(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("sh-sh"), "text/plain", 0); err != nil {
		t.Fatal(err)
	}
	route, ok, err := o.MMDSRoute("sbx-1", "/secret")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if !route.Present || route.Data != "sh-sh" || route.ContentType != "text/plain" {
		t.Fatalf("route = %+v", route)
	}
}

func TestMMDSRouteRequiresRunningState(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	sb := cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("sh-sh"), "text/plain", 0); err != nil {
		t.Fatal(err)
	}
	sb.State = types.StatePaused
	o.cache(sb)

	if _, ok, err := o.MMDSRoute("sbx-1", "/static"); ok || err != nil {
		t.Fatalf("static: ok=%t err=%v, want false/nil while paused", ok, err)
	}
	if _, ok, err := o.MMDSRoute("sbx-1", "/secret"); ok || err != nil {
		t.Fatalf("secret: ok=%t err=%v, want false/nil while paused", ok, err)
	}
}

func TestMMDSRouteSecretRevokedReturnsAbsentImmediately(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 2*time.Second) // long park -- must NOT be waited out
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("v"), "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := o.DeleteMMDSSecret(context.Background(), "sbx-1", "key1"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	route, ok, err := o.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected a revoked secret to be absent")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("revoked secret should return immediately, took %v", elapsed)
	}
}

func TestMMDSRouteSecretExpiredReturnsAbsentImmediately(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 2*time.Second) // long park -- must NOT be waited out
	past := time.Now().Add(-time.Hour).Unix()
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("stale"), "text/plain", past); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	route, ok, err := o.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected an expired secret to be absent")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expired secret should return immediately, took %v", elapsed)
	}
}

func TestMMDSRouteSecretNeverConfiguredTimesOutTo404(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 30*time.Millisecond)

	start := time.Now()
	route, ok, err := o.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected never-configured secret to be absent")
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("expected to wait out the park timeout, took %v", elapsed)
	}
}

func TestMMDSRouteSecretNeverConfiguredWokenByPut(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 2*time.Second)

	type result struct {
		route mmds.MMDSRoute
		ok    bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		route, ok, err := o.MMDSRoute("sbx-1", "/secret")
		done <- result{route, ok, err}
	}()

	time.Sleep(30 * time.Millisecond) // let the goroutine reach the park
	start := time.Now()
	if _, err := o.PutMMDSSecret(context.Background(), "sbx-1", "key1", []byte("just-in-time"), "text/plain", 0); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-done:
		if r.err != nil || !r.ok {
			t.Fatalf("ok=%t err=%v", r.ok, r.err)
		}
		if !r.route.Present || r.route.Data != "just-in-time" {
			t.Fatalf("route result = %+v", r.route)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("PutMMDSSecret should wake the parked MMDSRoute promptly, took %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MMDSRoute did not return after PutMMDSSecret")
	}
}

// TestMMDSRouteSecretConcurrentPutNeverMissesWakeup stresses the exact
// interleaving that used to lose a wakeup: MMDSRoute's own store-check racing
// against PutMMDSSecret's write+Notify, with no delay between starting the
// two goroutines. Each iteration uses a fresh sandbox (so its secret starts
// at revision==0, the only state MMDSRoute ever parks on) and fires the GET
// and the PUT concurrently; every iteration must observe the value promptly.
// Before the register-before-recheck fix, this reliably caught the race
// within a handful of iterations (the PUT's Notify landing in the old
// check-then-register gap, forcing MMDSRoute to sit out the full timeout).
func TestMMDSRouteSecretConcurrentPutNeverMissesWakeup(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	o.secretWait = newMMDSSecretWaiter(3 * time.Second) // long -- a miss must be obvious, not masked by a short timeout

	for i := 0; i < 30; i++ {
		id := fmt.Sprintf("sbx-race-%d", i)
		cachedTestSandboxForMMDSRoute(t, o, id, 0)

		type result struct {
			route mmds.MMDSRoute
			ok    bool
			err   error
		}
		done := make(chan result, 1)
		go func() {
			route, ok, err := o.MMDSRoute(id, "/secret")
			done <- result{route, ok, err}
		}()
		go func() {
			_, _ = o.PutMMDSSecret(context.Background(), id, "key1", []byte("v"), "text/plain", 0)
		}()

		select {
		case r := <-done:
			if r.err != nil || !r.ok {
				t.Fatalf("iteration %d: ok=%t err=%v", i, r.ok, r.err)
			}
			if !r.route.Present {
				t.Fatalf("iteration %d: secret not present after concurrent PUT", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: MMDSRoute missed the concurrent PUT's wakeup (lost-wakeup regression)", i)
		}
	}
}

func TestMMDSRouteServiceWorkingPassesThroughResponse(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	var gotHeaders http.Header
	o.mmdsServices = startFakeMMDSServiceForOrch(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}, 0)

	route, ok, err := o.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Type != "service" || route.StatusCode != 200 || route.Data != "hello" || route.ContentType != "text/plain" {
		t.Fatalf("route = %+v", route)
	}
	if gotHeaders.Get(mmdssvc.HeaderService) != "svc1" {
		t.Fatalf("HeaderService = %q, want svc1", gotHeaders.Get(mmdssvc.HeaderService))
	}
	if gotHeaders.Get(mmdssvc.HeaderSandboxID) != "sbx-1" {
		t.Fatalf("HeaderSandboxID = %q, want sbx-1", gotHeaders.Get(mmdssvc.HeaderSandboxID))
	}
}

func TestMMDSRouteServiceDownReturns503(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	o.mmdsServices = mmdssvc.Registry{"svc1": {SocketPath: filepath.Join(t.TempDir(), "nope.sock"), Timeout: time.Second, MaxResponseBytes: 1024}}

	route, ok, err := o.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503", route.StatusCode)
	}
}

func TestMMDSRouteServiceSlowReturns504(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	o.mmdsServices = startFakeMMDSServiceForOrch(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}, 30*time.Millisecond)

	route, ok, err := o.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("StatusCode = %d, want 504", route.StatusCode)
	}
}

func TestMMDSRouteServiceOversizedReturns502(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	reg := startFakeMMDSServiceForOrch(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 200))
	}, 0)
	reg["svc1"] = mmdssvc.Entry{SocketPath: reg["svc1"].SocketPath, Timeout: reg["svc1"].Timeout, MaxResponseBytes: 100}
	o.mmdsServices = reg

	route, ok, err := o.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusBadGateway {
		t.Fatalf("StatusCode = %d, want 502", route.StatusCode)
	}
}

func TestMMDSRouteServiceUnresolvableTargetReturns503(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	cachedTestSandboxForMMDSRoute(t, o, "sbx-1", 0)
	// mmdsServices left nil -- the registered service name resolves fine, but
	// nothing is registered on this node at all.

	route, ok, err := o.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503", route.StatusCode)
	}
}
