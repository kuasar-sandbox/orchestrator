package proxyshm

import (
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmds"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrpc"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// mmdsRPCTestHandler mirrors cmd/node-ctl/proxy.go's mmdsRPCHandler: answer a
// worker's request from the master's in-heap MMDSRoutes store.
func mmdsRPCTestHandler(mmds *MMDSRoutes) mmdsrpc.Handler {
	return func(sid, path string) (mmdsrpc.Route, bool) {
		canonical, ok := mmds.Get(sid)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		route, ok := sandboxcfg.LookupMMDSRoute(canonical, path)
		if !ok {
			return mmdsrpc.Route{}, false
		}
		return mmdsrpc.Route{Type: route.Type, ContentType: route.ContentType, Body: route.Data}, true
	}
}

func TestWorkerViewMMDSRouteOverRPC(t *testing.T) {
	master := NewMasterView(nil, 0, nil) // ApplyUpsert would need a real Table; we drive v.mmds directly instead.
	master.mmds.Upsert("sbx-1", `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`)

	a, b := net.Pipe()
	server := mmdsrpc.NewServer(b, mmdsRPCTestHandler(master.MMDSRoutes()), nil)
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
