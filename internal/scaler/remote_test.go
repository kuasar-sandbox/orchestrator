package scaler

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	src := testGroupSource(t,
		clusterstate.SandboxGroupRecord{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}},
		clusterstate.SandboxGroupRecord{Group: "/x", NodeSelectors: []map[string]string{{"pool": "absent"}}},
	)
	svc := NewRemoteLinksWithGroups([]RegistryLink{registryLinkFromAddress("registry", strings.TrimPrefix(controlSrv.URL, "http://"), nil)}, src, src, clustercfg.PlacementConfig{Candidates: 2}, 30, discard)
	scalerMux := http.NewServeMux()
	svc.ServeScaleLink(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	defer scalerSrv.Close()
	reg.SetScalerPeerSource(func(string) []registry.ScalerPeer {
		return []registry.ScalerPeer{{ID: "s1", Advertise: scalerSrv.URL}}
	})
	svc.Start(ctx)

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

	src := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}})
	svc := NewRemoteLinksWithGroups([]RegistryLink{
		{Name: "r1", BaseURL: srv1.URL, Client: srv1.Client()},
		{Name: "r2", BaseURL: srv2.URL, Client: srv2.Client()},
	}, src, src, clustercfg.PlacementConfig{Candidates: 1}, 30, discard)
	scalerMux := http.NewServeMux()
	svc.ServeScaleLink(scalerMux)
	scalerSrv := httptest.NewServer(scalerMux)
	defer scalerSrv.Close()
	reg1.SetScalerPeerSource(func(string) []registry.ScalerPeer {
		return []registry.ScalerPeer{{ID: "s1", Advertise: scalerSrv.URL}}
	})
	reg2.SetScalerPeerSource(func(string) []registry.ScalerPeer {
		return []registry.ScalerPeer{{ID: "s1", Advertise: scalerSrv.URL}}
	})
	defer cancel()
	svc.Start(ctx)

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

func TestSetNodeListLinksSameLinksPreservesReadyView(t *testing.T) {
	svc := NewRemoteLinks([]RegistryLink{{Name: "r1", BaseURL: "http://r1", Client: http.DefaultClient}},
		clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	seedNodeListView(t, svc, "n1")
	if !svc.nodes.ready() || len(svc.nodes.values()) != 1 {
		t.Fatal("seeded node_list view is not ready")
	}

	svc.SetNodeListLinks(context.Background(), []RegistryLink{{Name: "r1", BaseURL: "http://r1", Client: http.DefaultClient}})

	if !svc.nodes.ready() || len(svc.nodes.values()) != 1 {
		t.Fatalf("unchanged node_list links reset a ready view: ready=%v values=%v", svc.nodes.ready(), svc.nodes.values())
	}
}

func TestSetNodeListLinksChangedLinksResetsReadyView(t *testing.T) {
	svc := NewRemoteLinks([]RegistryLink{{Name: "r1", BaseURL: "http://r1", Client: http.DefaultClient}},
		clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	seedNodeListView(t, svc, "n1")

	svc.SetNodeListLinks(context.Background(), []RegistryLink{{Name: "r2", BaseURL: "http://r2", Client: http.DefaultClient}})

	if svc.nodes.ready() || len(svc.nodes.values()) != 0 {
		t.Fatalf("changed node_list links kept stale view: ready=%v values=%v", svc.nodes.ready(), svc.nodes.values())
	}
}

func TestSubscribeOnceUsesOpaqueWatchToken(t *testing.T) {
	var rawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rawQuery = req.URL.RawQuery
		writeViewFrameForTest(t, w, &registry.ViewEvent{Type: "bookmark", Token: "registry.1.test:8"})
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	token, err := svc.subscribeOnce(context.Background(),
		RegistryLink{Name: "r1", BaseURL: srv.URL, Client: srv.Client()},
		"/watch", "registry.1.test:7", svc.nodes.source("node_list"))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("subscribeOnce err=%v, want EOF after test stream closes", err)
	}
	if token != "registry.1.test:8" {
		t.Fatalf("token=%q, want updated event token", token)
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	if values.Get("from") != "registry.1.test:7" {
		t.Fatalf("query %q did not carry opaque from token", rawQuery)
	}
}

func TestRegisterLoopReportsMemberlistSeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan registry.ScalerRegister, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != registry.ScaleLinkRegisterPath {
			http.NotFound(w, req)
			return
		}
		var in registry.ScalerRegister
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case got <- in:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	svc := NewRemoteLinks([]RegistryLink{{Name: "r1", BaseURL: srv.URL, Client: srv.Client()}},
		clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go svc.RegisterLoopDynamic(ctx, "s1", srv.URL, "scaler.default", srv.URL)

	select {
	case reg := <-got:
		if reg.MemberlistLabel != "scaler.default" || reg.MemberlistAdvertise != srv.URL {
			t.Fatalf("scaler registered wrong memberlist seed: %+v", reg)
		}
	case <-time.After(time.Second):
		t.Fatal("scaler did not register")
	}
}

func writeViewFrameForTest(t *testing.T, w io.Writer, ev *registry.ViewEvent) {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
}

func seedNodeListView(t *testing.T, svc *Service, nodeID string) {
	t.Helper()
	sink := svc.nodes.source("node_list")
	sink.reset()
	raw, err := json.Marshal(clusterstate.NodeListEntry{
		NodeID: nodeID, Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	sink.put(nodeID, raw)
	sink.bookmark()
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
