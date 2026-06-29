package scaler

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func TestScalerDirectPlace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	kv := clusterstore.OpenMemory(1000)
	defer kv.Close()
	stores := registry.NewStores(kv)
	stores.PutNode(ctx, &registry.NodeRecord{NodeID: "n1", Labels: map[string]string{"pool": "p"}, Counts: 5})
	stores.PutNode(ctx, &registry.NodeRecord{NodeID: "n2", Labels: map[string]string{"pool": "p"}, Counts: 0})

	reg := registry.New(stores, nil, 0, discard)
	placer := registry.NewHTTPScalePlacer(reg, 2, 2*time.Second)
	reg.SetPlacer(placer)

	mux := http.NewServeMux()
	reg.ServeScaleLink(mux)
	controlSrv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer controlSrv.Close()
	defer cancel()

	svc := NewRemote(strings.TrimPrefix(controlSrv.URL, "http://"), nil, clustercfg.PlacementConfig{Candidates: 2}, 30, discard)
	scalerMux := http.NewServeMux()
	svc.ServeScaleLink(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	defer scalerSrv.Close()
	svc.groups.replace([]clusterstate.SandboxGroupRecord{{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}}})
	svc.Start(ctx)
	go svc.RegisterLoop(ctx, "s1", scalerSrv.URL, "")

	var placement *registry.Placement
	var err error
	for i := 0; i < 300; i++ {
		if placement, err = placer.Place(ctx, registry.PlaceRequest{Group: "/g", RouteKey: "rk"}); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || (placement.NodeID != "n1" && placement.NodeID != "n2") {
		t.Fatalf("direct Place = %+v err=%v (want n1 or n2)", placement, err)
	}

	svc.groups.replace([]clusterstate.SandboxGroupRecord{{Group: "/x", NodeSelectors: []map[string]string{{"pool": "absent"}}}})
	for i := 0; i < 300; i++ {
		_, perr := placer.Place(ctx, registry.PlaceRequest{Group: "/x", RouteKey: "rk"})
		if perr == registry.ErrNoNode {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("unplaceable group never returned ErrNoNode")
}

func TestScalerUsesSingleNodeListSourceAndRegistersAllRegistryMembers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	reg1, srv1 := testScaleRegistry(t, ctx, "n1")
	defer srv1.Close()
	reg2, srv2 := testScaleRegistry(t, ctx, "n2")
	defer srv2.Close()

	svc := NewRemoteLinks([]RegistryLink{
		{Name: "r1", BaseURL: srv1.URL, Client: srv1.Client()},
		{Name: "r2", BaseURL: srv2.URL, Client: srv2.Client()},
	}, clustercfg.PlacementConfig{Candidates: 1}, 30, discard)
	scalerMux := http.NewServeMux()
	svc.ServeScaleLink(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	defer scalerSrv.Close()
	defer cancel()
	svc.ImportGroups([]clusterstate.SandboxGroupRecord{{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}}})
	svc.Start(ctx)
	go svc.RegisterLoop(ctx, "s1", scalerSrv.URL, "")

	for i := 0; i < 300; i++ {
		if len(svc.nodes.values()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(svc.nodes.values()); got != 1 {
		t.Fatalf("single-source node_list size=%d, want 1", got)
	}

	for name, placer := range map[string]registry.Placer{
		"reg1": registry.NewHTTPScalePlacer(reg1, 1, 2*time.Second),
		"reg2": registry.NewHTTPScalePlacer(reg2, 1, 2*time.Second),
	} {
		var placement *registry.Placement
		var err error
		for i := 0; i < 300; i++ {
			placement, err = placer.Place(ctx, registry.PlaceRequest{Group: "/g", RouteKey: "rk"})
			if err == nil && placement != nil && placement.NodeID != "" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil || placement == nil || placement.NodeID == "" {
			t.Fatalf("%s Place = %+v err=%v", name, placement, err)
		}
	}
}

func testScaleRegistry(t *testing.T, ctx context.Context, nodeID string) (*registry.Registry, *httptest.Server) {
	t.Helper()
	kv := clusterstore.OpenMemory(1000)
	t.Cleanup(func() { kv.Close() })
	stores := registry.NewStores(kv)
	if err := stores.PutNode(ctx, &registry.NodeRecord{NodeID: nodeID, Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	reg := registry.New(stores, nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	reg.ServeScaleLink(mux)
	return reg, httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
}

func TestHTTPScalePlacerNoScaler(t *testing.T) {
	kv := clusterstore.OpenMemory(0)
	defer kv.Close()
	reg := registry.New(registry.NewStores(kv), nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	placer := registry.NewHTTPScalePlacer(reg, 1, 200*time.Millisecond)
	if _, err := placer.Place(context.Background(), registry.PlaceRequest{Group: "/g"}); err != registry.ErrNoNode {
		t.Fatalf("no scaler → want ErrNoNode, got %v", err)
	}
}
