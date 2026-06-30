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
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func TestScalerDirectPlace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	stores := registry.NewStores()
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
	svc := NewRemoteLinksWithGroups([]RegistryLink{registryLinkFromAddress("registry", strings.TrimPrefix(controlSrv.URL, "http://"), nil)}, src, testImportSources("test", src), clustercfg.PlacementConfig{Candidates: 2}, 30, discard)
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
	}, src, testImportSources("test", src), clustercfg.PlacementConfig{Candidates: 1}, 30, discard)
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

func TestRegisterLoopDynamicRetriesFailedRegisterQuickly(t *testing.T) {
	oldRetry := scalerRegisterRetryInterval
	scalerRegisterRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { scalerRegisterRetryInterval = oldRetry })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	gotSecond := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != registry.ScaleLinkRegisterPath {
			http.NotFound(w, req)
			return
		}
		call := calls.Add(1)
		if call == 1 {
			http.Error(w, "join failed", http.StatusServiceUnavailable)
			return
		}
		select {
		case gotSecond <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	svc := NewRemoteLinks([]RegistryLink{{Name: "r1", BaseURL: srv.URL, Client: srv.Client()}},
		clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go svc.RegisterLoopDynamic(ctx, "s1", srv.URL, "scaler.default", srv.URL)

	select {
	case <-gotSecond:
	case <-time.After(time.Second):
		t.Fatalf("register calls=%d, want retry after failure", calls.Load())
	}
}

func TestImportSourceLeaseAllowsOnlyOneScalerToRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src1 := newCountingGroupSource("/g")
	src2 := newCountingGroupSource("/g")
	cfg := clustercfg.PlacementConfig{
		Candidates: 1, ImportSourceOwnerCount: 2, ImportSourceLeaseTTL: "500ms", SelectorPatchRefresh: "50ms",
	}
	link := RegistryLink{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}
	svc1 := NewRemoteLinksWithGroups([]RegistryLink{link}, src1, testImportSources("counting-source", src1), cfg, 30, discard)
	svc2 := NewRemoteLinksWithGroups([]RegistryLink{link}, src2, testImportSources("counting-source", src2), cfg, 30, discard)
	peers := func() []string { return []string{"s1", "s2"} }
	svc1.SetImportSourceOwnerSource("s1", peers)
	svc2.SetImportSourceOwnerSource("s2", peers)
	seedNodeListView(t, svc1, "n1")
	seedNodeListView(t, svc2, "n1")
	svc1.Start(ctx)
	svc2.Start(ctx)

	time.Sleep(180 * time.Millisecond)
	c1, c2 := src1.rangeCalls.Load(), src2.rangeCalls.Load()
	if (c1 > 0 && c2 > 0) || (c1 == 0 && c2 == 0) {
		t.Fatalf("Range calls svc1=%d svc2=%d, want exactly one source owner", c1, c2)
	}
}

func TestScalerRefreshesUnchangedNodeLinkKeyCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src := newCountingGroupSource("/g")
	cfg := clustercfg.PlacementConfig{
		Candidates: 1, ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "500ms", SelectorPatchRefresh: "25ms",
	}
	svc := NewRemoteLinksWithGroups(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		src, testImportSources("counting-source", src), cfg, 30, discard,
	)
	seedNodeListView(t, svc, "n1")
	svc.Start(ctx)

	waitForNodeKey(t, ctx, reg, "n1", true)
	time.Sleep(180 * time.Millisecond)
	waitForNodeKey(t, ctx, reg, "n1", true)
	if calls := src.rangeCalls.Load(); calls < 2 {
		t.Fatalf("Range calls=%d, want repeated refresh cycles", calls)
	}
}

func TestReconcileKeyAllocationsRunsWhenNodeListBecomesReady(t *testing.T) {
	oldPoll := scalerReadyPollInterval
	scalerReadyPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { scalerReadyPollInterval = oldPoll })

	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src := newCountingGroupSource("/g")
	svc := NewRemoteLinksWithGroups(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		src, testImportSources("counting-source", src),
		clustercfg.PlacementConfig{Candidates: 1, ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "500ms", SelectorPatchRefresh: "1h"},
		30,
		discard,
	)
	go svc.reconcileSelectorPatches(ctx)

	time.Sleep(30 * time.Millisecond)
	if calls := src.rangeCalls.Load(); calls != 0 {
		t.Fatalf("Range calls before node_list ready=%d, want 0", calls)
	}
	seedNodeListView(t, svc, "n1")
	for i := 0; i < 100; i++ {
		if src.rangeCalls.Load() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reconcile did not run after node_list became ready")
}

func TestReconcileIntervalUsesSelectorPatchRefreshCadence(t *testing.T) {
	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{SelectorPatchRefresh: "1m"}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got := svc.reconcileInterval(); got != time.Minute {
		t.Fatalf("reconcile interval=%v, want selector_patch_refresh_interval", got)
	}
}

func TestSelectorPatchSignatureCanonicalizesSelectors(t *testing.T) {
	links := []RegistryLink{{Name: "r2", BaseURL: "http://r2"}, {Name: "r1", BaseURL: "http://r1"}}
	a := selectorPatchSignature([]map[string]string{{"b": "2", "a": "1"}, {"zone": "east"}}, []string{"n2", "n1"}, "fp", "inline", "mk", "", registryLinkSignature(links))
	b := selectorPatchSignature([]map[string]string{{"a": "1", "b": "2"}, {"zone": "east"}}, []string{"n1", "n2"}, "fp", "inline", "mk", "", registryLinkSignature(links))
	if a != b {
		t.Fatalf("canonical signatures differ:\n%s\n%s", a, b)
	}
}

func TestReconcileImportSourcePushesCompletedPagesBeforeLaterPageError(t *testing.T) {
	ctx := context.Background()
	src := &pagedFailingGroupSource{}
	var patches []string
	cursor := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case registry.ScaleLinkImportSourcePath:
			var in registry.ImportSourceLeaseRequest
			if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(registry.ImportSourceLeaseResponse{
				Acquired: true,
				Lease: registry.ImportSourceLease{
					SourceID: in.SourceID, OwnerID: in.OwnerID, RunID: in.RunID, Term: 1, Cursor: cursor,
					ExpiresUnixMs: time.Now().Add(time.Second).UnixMilli(),
				},
			})
		case registry.ScaleLinkSourceCursorPath:
			var in registry.ImportSourceCursorRequest
			if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cursor = in.Cursor
			w.WriteHeader(http.StatusNoContent)
		case registry.ScaleLinkSelectorPatchPath:
			var patch struct {
				Group string `json:"group"`
			}
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			patches = append(patches, patch.Group)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, req)
		}
	}))
	defer srv.Close()

	svc := NewRemoteLinksWithGroups(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		src, testImportSources("paged-source", src),
		clustercfg.PlacementConfig{Candidates: 1, ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "1s", SelectorPatchRefresh: "1m"},
		30,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	nodes := []*registry.NodeRecord{{NodeID: "n1", Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}}

	svc.reconcileImportSources(ctx, nodes, map[string]selectorPatchState{})

	if got := src.rangeCalls.Load(); got != 2 {
		t.Fatalf("Range calls=%d, want first page plus failing second page", got)
	}
	if len(patches) != 1 || patches[0] != "/g1" {
		t.Fatalf("patches=%v, want first completed page to be pushed before later page error", patches)
	}
}

func TestReconcileImportSourceDoesNotAdvanceCursorWhenPatchFails(t *testing.T) {
	ctx := context.Background()
	src := &pagedFailingGroupSource{}
	var cursorUpdates atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case registry.ScaleLinkImportSourcePath:
			var in registry.ImportSourceLeaseRequest
			if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(registry.ImportSourceLeaseResponse{
				Acquired: true,
				Lease: registry.ImportSourceLease{
					SourceID: in.SourceID, OwnerID: in.OwnerID, RunID: in.RunID, Term: 1,
					ExpiresUnixMs: time.Now().Add(time.Second).UnixMilli(),
				},
			})
		case registry.ScaleLinkSourceCursorPath:
			cursorUpdates.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case registry.ScaleLinkSelectorPatchPath:
			http.Error(w, "patch failed", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, req)
		}
	}))
	defer srv.Close()

	svc := NewRemoteLinksWithGroups(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		src, testImportSources("paged-source", src),
		clustercfg.PlacementConfig{Candidates: 1, ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "1s", SelectorPatchRefresh: "1m"},
		30,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	nodes := []*registry.NodeRecord{{NodeID: "n1", Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}}

	svc.reconcileImportSources(ctx, nodes, map[string]selectorPatchState{})

	if got := src.rangeCalls.Load(); got != 1 {
		t.Fatalf("Range calls=%d, want only first page", got)
	}
	if got := cursorUpdates.Load(); got != 0 {
		t.Fatalf("cursor updates=%d, want 0 after failed patch", got)
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

func waitForNodeKey(t *testing.T, ctx context.Context, reg *registry.Registry, nodeID string, want bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		node, found, err := reg.Stores().GetNode(ctx, nodeID)
		if err == nil && found && (len(node.ManifestKeys) > 0) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	node, found, err := reg.Stores().GetNode(ctx, nodeID)
	t.Fatalf("node key presence=%v found=%v err=%v keys=%+v, want %v", len(node.ManifestKeys) > 0, found, err, node.ManifestKeys, want)
}

type countingGroupSource struct {
	group      string
	rangeCalls atomic.Int32
}

func testImportSources(sourceID string, importer clusterstate.SandboxGroupImporter) []ImportSource {
	return []ImportSource{{SourceID: sourceID, Importer: importer}}
}

type pagedFailingGroupSource struct {
	rangeCalls atomic.Int32
}

func (s *pagedFailingGroupSource) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{Group: "/g1"}, true, nil
}

func (s *pagedFailingGroupSource) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{NodeSelectors: []map[string]string{{"pool": "p"}}}, true, nil
}

func (s *pagedFailingGroupSource) GetKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey}, true, nil
}

func (s *pagedFailingGroupSource) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey}, true, nil
}

func (s *pagedFailingGroupSource) Range(_ context.Context, cursor string, _ int) (clusterstate.GroupPage, error) {
	s.rangeCalls.Add(1)
	if cursor == "" {
		return clusterstate.GroupPage{Groups: []string{"/g1"}, NextCursor: "next"}, nil
	}
	return clusterstate.GroupPage{}, errors.New("second page failed")
}

func newCountingGroupSource(group string) *countingGroupSource {
	return &countingGroupSource{group: group}
}

func (s *countingGroupSource) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{Group: s.group}, true, nil
}

func (s *countingGroupSource) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{NodeSelectors: []map[string]string{{"pool": "p"}}}, true, nil
}

func (s *countingGroupSource) GetKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey}, true, nil
}

func (s *countingGroupSource) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey}, true, nil
}

func (s *countingGroupSource) Range(context.Context, string, int) (clusterstate.GroupPage, error) {
	s.rangeCalls.Add(1)
	return clusterstate.GroupPage{Groups: []string{s.group}}, nil
}

func testScaleRegistry(t *testing.T, ctx context.Context, nodeID string) (*registry.Registry, *httptest.Server) {
	t.Helper()
	stores := registry.NewStores()
	if err := stores.PutNode(ctx, &registry.NodeRecord{NodeID: nodeID, Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	reg := registry.New(stores, nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	reg.ServeScaleLink(mux)
	return reg, httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
}

func TestHTTPScalePlacerNoScaler(t *testing.T) {
	reg := registry.New(registry.NewStores(), nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	placer := registry.NewHTTPScalePlacer(reg, 1, 200*time.Millisecond)
	if _, err := placer.Place(context.Background(), registry.PlaceRequest{Group: "/g"}); err != registry.ErrNoNode {
		t.Fatalf("no scaler → want ErrNoNode, got %v", err)
	}
}
