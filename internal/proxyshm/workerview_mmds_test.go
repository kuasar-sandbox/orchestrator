package proxyshm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func base64Encode(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// mmdsRPCTestHandler mirrors cmd/node-ctl/proxy.go's mmdsRPCHandler: answer a
// worker's request from the master's in-heap MMDSRoutes/MMDSSecrets stores.
func mmdsRPCTestHandler(routes *MMDSRoutes, secrets *MMDSSecrets) mmdsrpc.Handler {
	return func(sid, path string) (mmdsrpc.Route, bool) {
		canonical, ok := routes.Get(sid)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		route, ok := sandboxcfg.LookupMMDSRoute(map[string]string{sandboxcfg.NsMMDS: canonical}, path)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		if route.Type != sandboxcfg.MMDSRouteSecret {
			return mmdsrpc.Route{Type: route.Type, ContentType: route.ContentType, Body: route.Data}, true
		}
		if !secrets.Synced() {
			return mmdsrpc.Route{Type: route.Type, Unavailable: true}, true
		}
		blobJSON, ok := secrets.Get(sid)
		if !ok {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: true}, true
		}
		var blob store.MMDSSecretBlob
		if err := json.Unmarshal([]byte(blobJSON), &blob); err != nil {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: true}, true
		}
		value, present := blob.Values[route.SecretName]
		if present && store.MMDSSecretExpired(value) {
			present = false
		}
		if !present {
			return mmdsrpc.Route{Type: route.Type, Present: false, Retryable: blob.Revision == 0}, true
		}
		return mmdsrpc.Route{Type: route.Type, ContentType: value.ContentType, Body: value.BodyBase64, Present: true}, true
	}
}

func TestWorkerViewMMDSRouteOverRPC(t *testing.T) {
	master := NewMasterView(nil, 0, nil) // ApplyUpsert would need a real Table; we drive v.mmds directly instead.
	master.mmds.Upsert("sbx-1", `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`)

	a, b := net.Pipe()
	server := mmdsrpc.NewServer(b, mmdsRPCTestHandler(master.MMDSRoutes(), master.MMDSSecrets()), nil)
	go server.Serve()
	client := mmdsrpc.NewClient(a)
	defer client.Close()

	worker := NewWorkerView(nil, nil, nil, 0, client, 0)

	route, ok, err := worker.MMDSRoute("sbx-1", "/x")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || route.Type != "static" || route.Data != "hi" || route.ContentType != "text/plain" {
		t.Fatalf("unexpected route: %+v, ok=%t", route, ok)
	}

	if _, ok, err := worker.MMDSRoute("sbx-1", "/unspecified"); err != nil || ok {
		t.Fatalf("unspecified path: ok=%t err=%v", ok, err)
	}
	if _, ok, err := worker.MMDSRoute("unknown-sid", "/x"); err != nil || ok {
		t.Fatalf("unknown sid: ok=%t err=%v", ok, err)
	}
}

// TestWorkerViewMMDSRouteRespectsConfiguredTimeout proves the timeout passed
// into NewWorkerView (ultimately config.ProxyFileConfig.ProxyRPCTimeout) is
// actually wired into MMDSRoute's request context, not just stored and
// ignored: against a master that never responds, MMDSRoute must fail close
// to the configured bound, not the unrelated defaultMMDSRPCTimeout (2s).
func TestWorkerViewMMDSRouteRespectsConfiguredTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	// Drain requests without ever responding, so the client's write completes
	// (net.Pipe is unbuffered/synchronous) and it blocks waiting for a
	// response that never comes -- exactly what the configured timeout guards.
	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := b.Read(buf); err != nil {
				return
			}
		}
	}()
	client := mmdsrpc.NewClient(a)
	defer client.Close()

	const configured = 50 * time.Millisecond
	worker := NewWorkerView(nil, nil, nil, 0, client, configured)

	start := time.Now()
	_, ok, err := worker.MMDSRoute("sbx-1", "/x")
	elapsed := time.Since(start)

	if err == nil || ok {
		t.Fatalf("expected a timeout error, got ok=%t err=%v", ok, err)
	}
	if elapsed >= defaultMMDSRPCTimeout {
		t.Fatalf("MMDSRoute took %v, want close to the configured %v (not the unrelated %v default)", elapsed, configured, defaultMMDSRPCTimeout)
	}
}

func TestWorkerViewMMDSRouteNilClient(t *testing.T) {
	worker := &WorkerView{mmdsClient: nil}
	route, ok, err := worker.MMDSRoute("sbx-1", "/x")
	if err != nil || ok || route != (mmds.MMDSRoute{}) {
		t.Fatalf("nil client should report not-found without error: route=%+v ok=%t err=%v", route, ok, err)
	}
}

const testMMDSSecretRouteSpec = `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/secret","type":"secret","secret_name":"key1"}]}`

// newMMDSSecretTestPair wires a real shared-memory Table (master + worker,
// via Create/Open, unlike the net.Pipe-only tests above) plus an mmdsrpc
// pipe between them -- MMDSRoute's secret-retry loop needs a real table so
// v.table.Rev()/Policy() have something to observe. The returned applyUpsert
// both writes through the master and bumps the worker's local Updates --
// production wires that signal over a real notify-pipe FD (Broadcaster ->
// Updates.readLoop); this test drives Updates.bump() directly instead,
// exactly like exec_test.go's pattern, since there's no real pipe here.
func newMMDSSecretTestPair(t *testing.T, mmdsParkTimeoutMS int) (master *MasterView, worker *WorkerView, applyUpsert func(routesync.RouteEntry)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.shm")
	masterTable, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { masterTable.Close() })
	workerTable, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { workerTable.Close() })

	master = NewMasterView(masterTable, 0, nil)
	master.SetPolicy(routesync.Policy{MMDSParkTimeoutMS: mmdsParkTimeoutMS})
	// Simulate a completed (trivially empty) initial full sync -- production
	// only ever considers MMDSSecrets synced once a BeginSync/Bookmark pair
	// has completed; without this, every secret lookup below would see
	// Synced()==false and fail closed regardless of what applyUpsert does
	// afterward (which only ever calls the live-update path, ApplyUpsert,
	// not another BeginSync/Bookmark cycle).
	master.BeginSync()
	master.Bookmark()

	a, b := net.Pipe()
	server := mmdsrpc.NewServer(b, mmdsRPCTestHandler(master.MMDSRoutes(), master.MMDSSecrets()), nil)
	go server.Serve()
	client := mmdsrpc.NewClient(a)
	t.Cleanup(func() { client.Close() })

	updates := &Updates{ch: make(chan struct{})}
	worker = NewWorkerView(workerTable, updates, nil, 0, client, 0)
	applyUpsert = func(e routesync.RouteEntry) {
		master.ApplyUpsert(e)
		updates.bump()
	}
	return master, worker, applyUpsert
}

func mmdsSecretTestEntry(sid string, blob *store.MMDSSecretBlob) routesync.RouteEntry {
	e := routesync.RouteEntry{
		SandboxID: sid, Profile: "bare", State: routesync.StateRunning,
		MMDSRoutes: testMMDSSecretRouteSpec,
	}
	if blob != nil {
		b, err := json.Marshal(blob)
		if err != nil {
			panic(err)
		}
		e.MMDSSecrets = string(b)
	}
	return e
}

func TestWorkerViewMMDSRouteSecretServesConfiguredValue(t *testing.T) {
	_, w, applyUpsert := newMMDSSecretTestPair(t, 2000)
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
		"key1": {BodyBase64: base64Encode("sh-sh-secret"), ContentType: "text/plain"},
	}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if !route.Present || route.Data != "sh-sh-secret" || route.ContentType != "text/plain" {
		t.Fatalf("route = %+v", route)
	}
}

func TestWorkerViewMMDSRouteSecretRevokedReturnsAbsentImmediately(t *testing.T) {
	_, w, applyUpsert := newMMDSSecretTestPair(t, 2000) // long park -- must NOT be waited out
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 2, Values: map[string]store.MMDSSecretValue{}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	start := time.Now()
	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected a revoked secret to be absent")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("revoked secret should return immediately, took %v", elapsed)
	}
}

// TestWorkerViewMMDSRouteSecretUnsyncedFailsClosedImmediately proves a
// disconnect (BeginSync without a matching Bookmark, simulating a resync in
// progress) makes MMDSRoute fail with an error -- not a stale value, not a
// false "never configured" 404-equivalent (Present=false with no error), and
// not a park-timeout wait -- for a secret that's actually configured and was
// already being served before the disconnect.
func TestWorkerViewMMDSRouteSecretUnsyncedFailsClosedImmediately(t *testing.T) {
	master, w, applyUpsert := newMMDSSecretTestPair(t, 2000) // long park -- must NOT be waited out
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
		"key1": {BodyBase64: base64Encode("sh-sh-secret"), ContentType: "text/plain"},
	}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	if err != nil || !ok || !route.Present {
		t.Fatalf("sanity check before disconnect: route=%+v ok=%t err=%v", route, ok, err)
	}

	master.MMDSSecrets().BeginSync() // disconnect/resync begins; no matching Bookmark yet

	start := time.Now()
	_, ok, err = w.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected an error while the secret view is unsynced, got ok=%t err=%v", ok, err)
	}
	if ok {
		t.Fatal("expected ok=false alongside the error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("unsynced should fail closed immediately, not park, took %v", elapsed)
	}
}

// TestWorkerViewMMDSRouteSecretInvalidateSyncFailsClosedImmediately mirrors
// TestWorkerViewMMDSRouteSecretUnsyncedFailsClosedImmediately but drives the
// disconnect through MasterView.InvalidateSync (the routesync.Sink hook a
// real dropped connection now triggers) instead of calling
// MMDSSecrets.BeginSync directly, proving the production entry point wires
// through to the same fail-closed behavior.
func TestWorkerViewMMDSRouteSecretInvalidateSyncFailsClosedImmediately(t *testing.T) {
	master, w, applyUpsert := newMMDSSecretTestPair(t, 2000) // long park -- must NOT be waited out
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
		"key1": {BodyBase64: base64Encode("sh-sh-secret"), ContentType: "text/plain"},
	}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	if err != nil || !ok || !route.Present {
		t.Fatalf("sanity check before disconnect: route=%+v ok=%t err=%v", route, ok, err)
	}

	master.InvalidateSync() // simulates a dropped connection, not a fresh BeginSync

	if master.MMDSSecrets().Synced() {
		t.Fatal("expected InvalidateSync to immediately mark the secret view unsynced")
	}

	start := time.Now()
	_, ok, err = w.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected an error while the secret view is unsynced, got ok=%t err=%v", ok, err)
	}
	if ok {
		t.Fatal("expected ok=false alongside the error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("unsynced should fail closed immediately, not park, took %v", elapsed)
	}
}

// TestMasterViewInvalidateSyncLeavesRouteDeclarationsUntouched proves
// InvalidateSync's scope is secret-only: a static/secret/service route
// declaration (held in MMDSRoutes) upserted before the disconnect must
// remain readable afterward, since only secret plaintext needs to fail
// closed immediately -- MMDSRoutes keeps tolerating staleness across a
// disconnect exactly as it already did before InvalidateSync existed. A
// regression here (e.g. someone extending InvalidateSync to also touch
// MMDSRoutes without updating this test) would silently widen the guest-
// visible outage every reconnect causes.
func TestMasterViewInvalidateSyncLeavesRouteDeclarationsUntouched(t *testing.T) {
	master, _, applyUpsert := newMMDSSecretTestPair(t, 2000)
	applyUpsert(mmdsSecretTestEntry("sbx-1", nil))

	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("sanity check: expected sbx-1's MMDS route declaration to be present before InvalidateSync")
	}

	master.InvalidateSync()

	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("expected InvalidateSync to leave MMDSRoutes untouched (declarations tolerate staleness)")
	}
}

func TestWorkerViewMMDSRouteSecretExpiredReturnsAbsentImmediately(t *testing.T) {
	_, w, applyUpsert := newMMDSSecretTestPair(t, 2000) // long park -- must NOT be waited out
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
		"key1": {BodyBase64: base64Encode("stale"), ContentType: "text/plain", ExpiresUnix: time.Now().Add(-time.Hour).Unix()},
	}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	start := time.Now()
	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected an expired secret to be absent")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expired secret should return immediately, took %v", elapsed)
	}
}

func TestWorkerViewMMDSRouteSecretNeverConfiguredTimesOut(t *testing.T) {
	_, w, applyUpsert := newMMDSSecretTestPair(t, 200)
	applyUpsert(mmdsSecretTestEntry("sbx-1", nil)) // specified, no secrets blob synced at all

	start := time.Now()
	route, ok, err := w.MMDSRoute("sbx-1", "/secret")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.Present {
		t.Fatal("expected never-configured secret to be absent")
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("expected to wait out the park timeout, took %v", elapsed)
	}
}

func TestWorkerViewMMDSRouteSecretNeverConfiguredWokenByLateUpsert(t *testing.T) {
	_, w, applyUpsert := newMMDSSecretTestPair(t, 5000)
	applyUpsert(mmdsSecretTestEntry("sbx-1", nil))

	type result struct {
		route mmds.MMDSRoute
		ok    bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		route, ok, err := w.MMDSRoute("sbx-1", "/secret")
		done <- result{route, ok, err}
	}()

	time.Sleep(100 * time.Millisecond) // let the goroutine reach the park
	start := time.Now()
	blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
		"key1": {BodyBase64: base64Encode("just-in-time"), ContentType: "text/plain"},
	}}
	applyUpsert(mmdsSecretTestEntry("sbx-1", blob))

	select {
	case r := <-done:
		if r.err != nil || !r.ok {
			t.Fatalf("ok=%t err=%v", r.ok, r.err)
		}
		if !r.route.Present || r.route.Data != "just-in-time" {
			t.Fatalf("route = %+v", r.route)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("ApplyUpsert should wake the parked MMDSRoute promptly, took %v", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("MMDSRoute did not return after the late ApplyUpsert")
	}
}

// TestWorkerViewMMDSRouteSecretConcurrentApplyNeverMissesWakeup stresses the
// exact interleaving that used to lose a wakeup: MMDSRoute sampling the table
// revision *after* its RPC call raced against an ApplyUpsert landing in that
// gap. Each iteration uses a fresh sandbox (declared but never configured, so
// revision==0 -- the only state MMDSRoute ever parks on) and fires the GET
// and the sync apply concurrently, with no delay between starting the two
// goroutines; every iteration must observe the value promptly, never falling
// back to the full park timeout.
func TestWorkerViewMMDSRouteSecretConcurrentApplyNeverMissesWakeup(t *testing.T) {
	master, w, applyUpsert := newMMDSSecretTestPair(t, 3000) // long -- a miss must be obvious, not masked by a short timeout

	for i := 0; i < 30; i++ {
		sid := fmt.Sprintf("sbx-race-%d", i)
		applyUpsert(mmdsSecretTestEntry(sid, nil)) // declare the route; secret starts at revision==0

		blob := &store.MMDSSecretBlob{Version: 1, Revision: 1, Values: map[string]store.MMDSSecretValue{
			"key1": {BodyBase64: base64Encode("v"), ContentType: "text/plain"},
		}}

		type result struct {
			route mmds.MMDSRoute
			ok    bool
			err   error
		}
		done := make(chan result, 1)
		go func() {
			route, ok, err := w.MMDSRoute(sid, "/secret")
			done <- result{route, ok, err}
		}()
		go func() {
			applyUpsert(mmdsSecretTestEntry(sid, blob))
		}()

		select {
		case r := <-done:
			if r.err != nil || !r.ok {
				t.Fatalf("iteration %d: ok=%t err=%v", i, r.ok, r.err)
			}
			if !r.route.Present {
				t.Fatalf("iteration %d: secret not present after concurrent apply", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: MMDSRoute missed the concurrent apply's wakeup (lost-wakeup regression)", i)
		}

		master.ApplyDelete(sid) // free the shared table's slot (fixed capacity 16) for the next iteration
	}
}
