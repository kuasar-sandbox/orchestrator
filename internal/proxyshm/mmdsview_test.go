package proxyshm

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const testMMDSRoutes = `{"routes":[{"path":"/static","data":"x"},{"path":"/secret","type":"secret","secret":"key"},{"path":"/svc","type":"service","service":"local"}]}`

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
	worker := NewMMDSWorkerView(table, nil, nil, time.Second, client)

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
	worker := NewMMDSWorkerView(table, nil, nil, time.Second, client)
	route, found, err := worker.MMDSRoute(context.Background(), "sandbox-1", "/svc")
	if err != nil || !found || route.StatusCode != http.StatusAccepted || route.ContentType != "application/json" || string(route.Body) != `{"external":true}` {
		t.Fatalf("external service route metadata mismatch: found=%t status=%d content-type=%q err=%v", found, route.StatusCode, route.ContentType, err)
	}
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("service did not receive request")
	}
}
