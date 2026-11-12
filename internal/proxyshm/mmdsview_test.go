package proxyshm

import (
	"bytes"
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const testMMDSRoutes = `{"routes":[{"path":"/static","data":"x"},{"path":"/secret","type":"secret","secret":"key"},{"path":"/svc","type":"service","service":"local"},{"type":"hold-traffic","path":"/hold"}]}`

func routeValues(values map[string][]byte) *routesync.MMDSRouteSecretValues {
	converted := routesync.MMDSRouteSecretValues(values)
	return &converted
}

func TestMMDSViewAtomicRotationDeleteAndSyncFailure(t *testing.T) {
	view := NewMMDSView(2)
	view.BeginSync()
	if got := view.Resolve("sandbox-1", "/secret"); !got.Unavailable {
		t.Fatal("pre-bookmark response metadata mismatch")
	}
	if err := view.SetServices(map[string]string{"local": "unix:///run/kuasar/service.sock"}); err != nil {
		t.Fatal(err)
	}
	entry := routesync.RouteEntry{SandboxID: "sandbox-1", MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{"key": {0, 1, 2}})}
	if err := view.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	view.Bookmark()

	secret := view.Resolve("sandbox-1", "/secret")
	if !secret.Found || !secret.Present || !bytes.Equal(secret.Body, []byte{0, 1, 2}) || secret.ContentType != "text/plain" {
		t.Fatalf("initial secret response metadata mismatch")
	}
	service := view.Resolve("sandbox-1", "/svc")
	if !service.Found || service.Service != "local" || service.ServiceSocket != "/run/kuasar/service.sock" {
		t.Fatal("service response metadata mismatch")
	}

	entry.MMDSRouteSecretValues = routeValues(map[string][]byte{"key": {3, 4}})
	if err := view.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	rotated := view.Resolve("sandbox-1", "/secret")
	if !rotated.Present || !bytes.Equal(rotated.Body, []byte{3, 4}) {
		t.Fatal("rotation was not atomically visible")
	}

	entry.MMDSRouteSecretValues = routeValues(map[string][]byte{})
	if err := view.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	deleted := view.Resolve("sandbox-1", "/secret")
	if !deleted.Found || deleted.Present || len(deleted.Body) != 0 {
		t.Fatal("deleted secret response metadata mismatch")
	}

	view.BeginSync()
	if got := view.Resolve("sandbox-1", "/static"); !got.Unavailable {
		t.Fatal("disconnected response metadata mismatch")
	}
}

func TestMMDSViewNilSecretProjectionFailsClosed(t *testing.T) {
	view := NewMMDSView(1)
	view.BeginSync()
	if err := view.Upsert(routesync.RouteEntry{SandboxID: "sandbox-1", MMDSRoutes: testMMDSRoutes}); err != nil {
		t.Fatal(err)
	}
	view.Bookmark()
	if got := view.Resolve("sandbox-1", "/secret"); !got.Unavailable {
		t.Fatal("incomplete full-sync response metadata mismatch")
	}
	for _, path := range []string{"/static", "/svc", "/hold"} {
		if got := view.Resolve("sandbox-1", path); !got.Found || got.Unavailable {
			t.Fatalf("%s unavailable along with secret blob", path)
		}
	}
	if got := view.Resolve("sandbox-1", "/unknown"); got.Found || got.Unavailable {
		t.Fatal("unknown route should stay absent")
	}
}

func TestMasterNeverWritesMMDSRouteValuesToSHM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	table, err := Create(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	master := NewMasterView(table, time.Second, nil)
	master.BeginSync()
	value := []byte("opaque-route-value-7fd")
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "sandbox-1", State: routesync.StateRunning, RunID: "run-1",
		MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{"key": value}),
	})
	master.Bookmark()
	route, ok := table.Lookup("sandbox-1")
	if !ok || route.MMDSRoutes != "" || route.MMDSRouteSecretValues != nil || route.RunID != "run-1" {
		t.Fatalf("fixed route projection metadata mismatch")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, value) {
		t.Fatal("plaintext MMDS route value found in SHM")
	}
}

func TestMasterRevokesConfidentialViewOnPauseAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	table, err := Create(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	master := NewMasterView(table, time.Second, nil)
	master.BeginSync()
	entry := routesync.RouteEntry{
		SandboxID: "sandbox-1", State: routesync.StateRunning, RunID: "run-1",
		MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{"key": []byte("one")}),
	}
	master.ApplyUpsert(entry)
	master.Bookmark()
	if got := master.ResolveMMDS(entry.SandboxID, "/secret"); !got.Found || !got.Present {
		t.Fatal("active confidential view missing")
	}

	entry.State = routesync.StatePaused
	master.ApplyUpsert(entry)
	if got := master.ResolveMMDS(entry.SandboxID, "/secret"); got.Found || got.Unavailable {
		t.Fatal("paused confidential view was retained")
	}
	if route, ok := table.Lookup(entry.SandboxID); !ok || route.State != routesync.StatePaused {
		t.Fatal("paused SHM state did not converge")
	}

	entry.State = routesync.StateRunning
	master.ApplyUpsert(entry)
	master.ApplyDelete(entry.SandboxID)
	if got := master.ResolveMMDS(entry.SandboxID, "/secret"); got.Found || got.Unavailable {
		t.Fatal("deleted confidential view was retained")
	}
	if _, ok := table.Lookup(entry.SandboxID); ok {
		t.Fatal("deleted SHM state was retained")
	}
}

func TestWorkerSeesRotationDeleteAndDisconnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	table, err := Create(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	master := NewMasterView(table, time.Second, nil)
	master.BeginSync()
	entry := routesync.RouteEntry{
		SandboxID: "sandbox-1", State: routesync.StateRunning, RunID: "run-1",
		MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{"key": []byte("one")}),
	}
	master.ApplyUpsert(entry)
	master.Bookmark()

	serverConn, clientConn := net.Pipe()
	go mmdsrpc.NewServer(serverConn, master.ResolveMMDS, nil).Serve()
	client := mmdsrpc.NewClient(clientConn)
	defer client.Close()
	worker := NewMMDSWorkerView(table, nil, nil, nil, time.Second, client)

	resolve := func() (body string, present bool, err error) {
		route, found, err := worker.MMDSRoute(context.Background(), "sandbox-1", "/secret")
		if !found && err == nil {
			t.Fatal("secret route unexpectedly absent")
		}
		return string(route.Body), route.Present, err
	}
	if body, present, err := resolve(); err != nil || !present || body != "one" {
		t.Fatalf("initial resolution metadata mismatch: present=%t err=%v", present, err)
	}
	entry.MMDSRouteSecretValues = routeValues(map[string][]byte{"key": []byte("two")})
	master.ApplyUpsert(entry)
	if body, present, err := resolve(); err != nil || !present || body != "two" {
		t.Fatalf("rotated resolution metadata mismatch: present=%t err=%v", present, err)
	}
	entry.MMDSRouteSecretValues = routeValues(map[string][]byte{})
	master.ApplyUpsert(entry)
	if body, present, err := resolve(); err != nil || present || body != "" {
		t.Fatalf("deleted resolution metadata mismatch: present=%t err=%v", present, err)
	}
	master.InvalidateSync()
	if worker.MMDSAvailable() {
		t.Fatal("disconnected worker still reports MMDS ready")
	}
	if _, _, err := resolve(); err == nil {
		t.Fatal("disconnected view did not fail closed")
	}
}

func TestExternalWorkerRelaysServiceFromConductorPolicy(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "service.sock")
	serviceListener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer serviceListener.Close()
	requestSeen := make(chan struct{}, 1)
	serviceServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/svc" || r.Header.Get("E2b-Sandbox-Id") != "sandbox-1" || r.Header.Get("E2b-Sandbox-Service") != "local" {
			t.Errorf("unexpected service request metadata")
		}
		requestSeen <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"external":true}`))
	})}
	go serviceServer.Serve(serviceListener)
	defer serviceServer.Close()

	path := filepath.Join(t.TempDir(), "routes.shm")
	table, err := Create(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	master := NewMasterView(table, time.Second, nil)
	master.BeginSync()
	master.SetPolicy(routesync.Policy{MMDS: &routesync.MMDSProxyPolicy{
		Enabled: true, Listen: "127.0.0.1:19254",
		Services: map[string]string{"local": "unix://" + socket},
	}})
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "sandbox-1", State: routesync.StateRunning, RunID: "run-1",
		MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{}),
	})
	master.Bookmark()

	serverConn, clientConn := net.Pipe()
	go mmdsrpc.NewServer(serverConn, master.ResolveMMDS, nil).Serve()
	client := mmdsrpc.NewClient(clientConn)
	defer client.Close()
	worker := NewMMDSWorkerView(table, nil, nil, nil, time.Second, client)
	route, found, err := worker.MMDSRoute(context.Background(), "sandbox-1", "/svc")
	if err != nil || !found || route.ReadService == nil {
		t.Fatalf("external service lookup: found=%t err=%v", found, err)
	}
	select {
	case <-requestSeen:
		t.Fatal("lookup invoked the external provider")
	default:
	}
	result := route.ReadService(context.Background())
	if result.StatusCode != http.StatusAccepted || result.ContentType != "application/json" || string(result.Body) != `{"external":true}` {
		t.Fatalf("service GET = %+v", result)
	}
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("service did not receive request")
	}
}

// Exercise the real guest handler, floating-IP/token binding, shared worker
// view, master route lookup and hold RPC together, without a real guest VM.
func TestHoldGuestAcrossWorkerMasterAndIncarnationChange(t *testing.T) {
	_, wt, master, _ := newMMDSSourceTestViews(t, 4)
	master.BeginSync()
	r := testMMDSSourceRoute("sandbox-1", routesync.StateRunning, testMMDSSourceBase())
	r.MmdsSecret = "00010203"
	r.MMDSRoutes = testMMDSRoutes // includes a secret with unavailable values
	applyMMDSSourceRoute(t, master, r)
	master.Bookmark()
	s, w := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var current int64
	go mmdsrpc.NewServer(s, master.ResolveMMDS, nil).WithHold(ctx, func(_ context.Context, sid, run, path string, value, match *int64) (*int64, int) {
		if sid != "sandbox-1" || run != "run-sandbox-1" || path != "/hold" {
			t.Errorf("wrong guest binding: %q %q %q", sid, run, path)
			return nil, 409
		}
		if match != nil && current != *match {
			return &current, 409
		}
		if value != nil {
			current = *value
		}
		return &current, 200
	}).Serve()
	client := mmdsrpc.NewClient(w)
	defer client.Close()
	worker := NewMMDSWorkerView(wt, nil, nil, nil, time.Second, client)
	handler := mmds.New(worker, time.Second, nil).Handler()
	call := func(method, path, token, body string) (int, string) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.RemoteAddr = r.FloatingIP + ":3333"
		if method == http.MethodGet {
			req.Body = http.NoBody
		}
		req.Header.Set("X-metadata-token-ttl-seconds", "60")
		req.Header.Set("X-metadata-token", token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	status, token := call("PUT", "/latest/api/token", "", "")
	if status != 200 {
		t.Fatalf("mint failed: %d", status)
	}
	if s, b := call("PUT", "/hold?if-match=%30", token, "1"); s != 200 || b != "1" {
		t.Fatalf("hold write = %d %q", s, b)
	}
	if s, b := call("GET", "/hold", token, ""); s != 200 || b != "1" {
		t.Fatalf("hold read = %d %q", s, b)
	}
	if s, _ := call("GET", "/secret", token, ""); s != 503 {
		t.Fatalf("unavailable secret = %d", s)
	}
	r.RunID = "run-2"
	applyMMDSSourceRoute(t, master, r)
	if s, _ := call("PUT", "/hold", token, "0"); s != 401 {
		t.Fatalf("stale guest token status = %d", s)
	}
}
