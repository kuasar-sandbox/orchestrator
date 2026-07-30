package proxyshm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdssvc"
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
		switch route.Type {
		case sandboxcfg.MMDSRouteStatic:
			return mmdsrpc.Route{Type: route.Type, ContentType: route.ContentType, Body: route.Data}, true
		case sandboxcfg.MMDSRouteService:
			if !routes.Synced() {
				return mmdsrpc.Route{Type: route.Type, Unavailable: true}, true
			}
			target, _ := sandboxcfg.LookupMMDSService(map[string]string{sandboxcfg.NsMMDS: canonical}, route.ServiceName)
			return mmdsrpc.Route{Type: route.Type, Target: target, ServiceName: route.ServiceName}, true
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

	worker := NewWorkerView(nil, nil, nil, 0, client, 0, nil)

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

// TestWorkerViewMMDSRouteStaticSurvivesInvalidateSync proves a static route
// stays servable end to end (real master/worker RPC round trip, not just a
// direct MMDSRoutes.Get check) through the same disconnect that fails a
// service route closed -- static content is immutable and non-sensitive, so
// InvalidateSync must not interrupt it, unlike secret/service.
func TestWorkerViewMMDSRouteStaticSurvivesInvalidateSync(t *testing.T) {
	master := NewMasterView(nil, 0, nil)
	master.mmds.Upsert("sbx-1", `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`)

	a, b := net.Pipe()
	server := mmdsrpc.NewServer(b, mmdsRPCTestHandler(master.MMDSRoutes(), master.MMDSSecrets()), nil)
	go server.Serve()
	client := mmdsrpc.NewClient(a)
	defer client.Close()

	worker := NewWorkerView(nil, nil, nil, 0, client, 0, nil)

	route, ok, err := worker.MMDSRoute("sbx-1", "/x")
	if err != nil || !ok || route.Data != "hi" {
		t.Fatalf("sanity check before disconnect: route=%+v ok=%t err=%v", route, ok, err)
	}

	master.InvalidateSync() // simulates a dropped connection

	route, ok, err = worker.MMDSRoute("sbx-1", "/x")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || route.Type != "static" || route.Data != "hi" || route.ContentType != "text/plain" {
		t.Fatalf("expected the static route to survive InvalidateSync unchanged, got route=%+v ok=%t", route, ok)
	}
}

const testMMDSServiceRouteSpec = `{"version":1,"services":[{"name":"svc1","target":"svc1"}],"routes":[{"path":"/service","type":"service","service_name":"svc1"}]}`

// startFakeMMDSServiceForWorkerView serves handler over a real Unix domain
// socket and returns a mmdssvc.Registry with one entry, "svc1" -- matching
// the target testMMDSServiceRouteSpec's "/service" route resolves to.
func startFakeMMDSServiceForWorkerView(t *testing.T, handler http.HandlerFunc, timeout time.Duration) mmdssvc.Registry {
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

// newMMDSServiceTestWorker wires a master (in-heap MMDSRoutes only, mirroring
// TestWorkerViewMMDSRouteOverRPC's nil-table pattern -- the service path
// needs no real shared-memory Table) and a worker with services as its local
// registry, connected over a real net.Pipe RPC round trip, proving master
// and worker agree on the resolved Target/ServiceName.
func newMMDSServiceTestWorker(t *testing.T, services mmdssvc.Registry) (*MasterView, *WorkerView) {
	t.Helper()
	master := NewMasterView(nil, 0, nil)
	// Simulate a completed (trivially empty) initial full sync directly on
	// mmds -- master.BeginSync()/Bookmark() would also touch the (nil, in
	// this fixture) table. Must run BEFORE the Upsert below: Bookmark's
	// mark-and-sweep drops any entry whose syncGen predates the generation
	// it just bumped to, so upserting first would have this immediately
	// swept away. Without this pair at all, every service lookup below
	// would see Synced()==false and fail closed (503) regardless of the
	// declared route.
	master.mmds.BeginSync()
	master.mmds.Bookmark()
	master.mmds.Upsert("sbx-1", testMMDSServiceRouteSpec)

	a, b := net.Pipe()
	server := mmdsrpc.NewServer(b, mmdsRPCTestHandler(master.MMDSRoutes(), master.MMDSSecrets()), nil)
	go server.Serve()
	client := mmdsrpc.NewClient(a)
	t.Cleanup(func() { client.Close() })

	return master, NewWorkerView(nil, nil, nil, 0, client, 0, services)
}

func TestWorkerViewMMDSRouteServiceWorkingPassesThroughResponse(t *testing.T) {
	var gotHeaders http.Header
	services := startFakeMMDSServiceForWorkerView(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}, 0)
	_, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
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

func TestWorkerViewMMDSRouteServiceDownReturns503(t *testing.T) {
	services := mmdssvc.Registry{"svc1": {SocketPath: filepath.Join(t.TempDir(), "nope.sock"), Timeout: time.Second, MaxResponseBytes: 1024}}
	_, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503", route.StatusCode)
	}
}

func TestWorkerViewMMDSRouteServiceSlowReturns504(t *testing.T) {
	services := startFakeMMDSServiceForWorkerView(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}, 30*time.Millisecond)
	_, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("StatusCode = %d, want 504", route.StatusCode)
	}
}

func TestWorkerViewMMDSRouteServiceOversizedReturns502(t *testing.T) {
	services := startFakeMMDSServiceForWorkerView(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 200))
	}, 0)
	services["svc1"] = mmdssvc.Entry{SocketPath: services["svc1"].SocketPath, Timeout: services["svc1"].Timeout, MaxResponseBytes: 100}
	_, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusBadGateway {
		t.Fatalf("StatusCode = %d, want 502", route.StatusCode)
	}
}

// TestWorkerViewMMDSRouteServiceUnsyncedFailsClosedImmediately proves a
// disconnect (BeginSync without a matching Bookmark, simulating a resync in
// progress) makes a service route fail closed (503) rather than dial the
// registered service using a possibly-stale declaration -- distinct from
// static routes, which stay servable through the same window (see
// MMDSRoutes's doc comment).
func TestWorkerViewMMDSRouteServiceUnsyncedFailsClosedImmediately(t *testing.T) {
	services := startFakeMMDSServiceForWorkerView(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}, 0)
	master, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok || route.StatusCode != 200 {
		t.Fatalf("sanity check before disconnect: route=%+v ok=%t err=%v", route, ok, err)
	}

	master.MMDSRoutes().BeginSync() // disconnect/resync begins; no matching Bookmark yet

	start := time.Now()
	route, ok, err = worker.MMDSRoute("sbx-1", "/service")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("unsynced service route: ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503 while unsynced", route.StatusCode)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("unsynced should fail closed immediately, not dial the service, took %v", elapsed)
	}
}

// TestWorkerViewMMDSRouteServiceInvalidateSyncFailsClosedImmediately mirrors
// TestWorkerViewMMDSRouteServiceUnsyncedFailsClosedImmediately but drives the
// disconnect through MasterView.InvalidateSync (the routesync.Sink hook a
// real dropped connection now triggers) instead of calling
// MMDSRoutes.BeginSync directly, proving the production entry point wires
// through to the same fail-closed behavior for service routes, not just
// secret ones.
func TestWorkerViewMMDSRouteServiceInvalidateSyncFailsClosedImmediately(t *testing.T) {
	services := startFakeMMDSServiceForWorkerView(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}, 0)
	master, worker := newMMDSServiceTestWorker(t, services)

	route, ok, err := worker.MMDSRoute("sbx-1", "/service")
	if err != nil || !ok || route.StatusCode != 200 {
		t.Fatalf("sanity check before disconnect: route=%+v ok=%t err=%v", route, ok, err)
	}

	master.InvalidateSync() // simulates a dropped connection, not a fresh BeginSync

	if master.MMDSRoutes().Synced() {
		t.Fatal("expected InvalidateSync to immediately mark MMDSRoutes unsynced")
	}

	start := time.Now()
	route, ok, err = worker.MMDSRoute("sbx-1", "/service")
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("unsynced service route: ok=%t err=%v", ok, err)
	}
	if route.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503 while unsynced", route.StatusCode)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("unsynced should fail closed immediately, not dial the service, took %v", elapsed)
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
	worker := NewWorkerView(nil, nil, nil, 0, client, configured, nil)

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
	worker = NewWorkerView(workerTable, updates, nil, 0, client, 0, nil)
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

// TestMasterViewInvalidateSyncFlipsRouteSyncedButKeepsDeclarations proves
// InvalidateSync's two-tier MMDSRoutes behavior: it immediately flips
// Synced() to false (gating "service" routes closed -- a real dial must not
// act on a possibly-stale declaration), but does NOT clear byID, so a
// "static" route declaration upserted before the disconnect stays readable
// (static content is immutable/non-sensitive and tolerates staleness). A
// regression collapsing this into either extreme -- clearing declarations
// outright, or never flipping Synced() -- would violate option A's agreed
// split (see MasterView.InvalidateSync's doc comment).
func TestMasterViewInvalidateSyncFlipsRouteSyncedButKeepsDeclarations(t *testing.T) {
	master, _, applyUpsert := newMMDSSecretTestPair(t, 2000)
	applyUpsert(mmdsSecretTestEntry("sbx-1", nil))

	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("sanity check: expected sbx-1's MMDS route declaration to be present before InvalidateSync")
	}
	if !master.MMDSRoutes().Synced() {
		t.Fatal("sanity check: expected MMDSRoutes to be synced before InvalidateSync")
	}

	master.InvalidateSync()

	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("expected InvalidateSync to leave the declaration itself readable (static routes tolerate staleness)")
	}
	if master.MMDSRoutes().Synced() {
		t.Fatal("expected InvalidateSync to immediately mark MMDSRoutes unsynced (gating service routes closed)")
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
