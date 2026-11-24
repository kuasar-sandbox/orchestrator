package placer

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
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
)

func TestPlacerUsesSingleNodeListSourceAndRegistersAllRegistryMembers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, srv1 := testScaleRegistry(t, ctx, "n1")
	defer srv1.Close()
	_, srv2 := testScaleRegistry(t, ctx, "n2")
	defer srv2.Close()

	src := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}})
	svc := NewRemoteLinksWithImports([]RegistryLink{
		{Name: "r1", BaseURL: srv1.URL, Client: srv1.Client()},
		{Name: "r2", BaseURL: srv2.URL, Client: srv2.Client()},
	}, testImportSources("test", src), clustercfg.PlacementConfig{}, discard)
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

}

func TestRemoteLinksWithGroupsDefaultsNilLogger(t *testing.T) {
	svc := NewRemoteLinksWithImports(nil, nil, clustercfg.PlacementConfig{}, nil)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RegisterLoop with nil logger panic: %v", r)
		}
	}()
	svc.RegisterLoop(context.Background(), "", "", "")
}

func TestSetNodeListLinksSameLinksPreservesReadyView(t *testing.T) {
	svc := NewRemoteLinks([]RegistryLink{{Name: "r1", BaseURL: "http://r1", Client: http.DefaultClient}},
		clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
		clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	got := make(chan registry.PlacerRegister, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != registry.PlacerLinkRegisterPath {
			http.NotFound(w, req)
			return
		}
		var in registry.PlacerRegister
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
		clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.RegisterLoop(ctx, "s1", srv.URL, "placer.default")
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("placer register loop did not stop")
		}
	}()

	select {
	case reg := <-got:
		if reg.MemberlistLabel != "placer.default" || reg.Advertise != srv.URL {
			t.Fatalf("placer registered wrong seed: %+v", reg)
		}
	case <-time.After(time.Second):
		t.Fatal("placer did not register")
	}
}

func TestRegisterLoopRetriesFailedRegisterQuickly(t *testing.T) {
	oldRetry := placerRegisterRetryInterval
	placerRegisterRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { placerRegisterRetryInterval = oldRetry })

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	gotSecond := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != registry.PlacerLinkRegisterPath {
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
		clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.RegisterLoop(ctx, "s1", srv.URL, "placer.default")
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("placer register loop did not stop")
		}
	}()

	select {
	case <-gotSecond:
	case <-time.After(time.Second):
		t.Fatalf("register calls=%d, want retry after failure", calls.Load())
	}
}

func TestImportSourceLeaseAllowsOnlyOnePlacerToRange(t *testing.T) {
	for _, delay := range []time.Duration{0, 220 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			discard := slog.New(slog.NewTextHandler(io.Discard, nil))
			_, srv := testScaleRegistry(t, ctx, "n1")
			defer srv.Close()
			defer cancel()

			src1 := newCountingGroupSource("/g")
			src2 := newCountingGroupSource("/g")
			cfg := clustercfg.PlacementConfig{
				ImportSourceOwnerCount: 2, ImportSourceLeaseTTL: "500ms", ImportRefresh: "50ms",
			}
			server := srv
			if delay > 0 {
				// Model a healthy lease request taking longer than the old startup sleep.
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if req.URL.Path == registry.PlacerLinkImportSourcePath {
						select {
						case <-time.After(delay):
						case <-req.Context().Done():
							return
						}
					}
					srv.Config.Handler.ServeHTTP(w, req)
				}))
				defer func() {
					cancel()
					server.Close()
				}()
			}
			link := RegistryLink{Name: "registry", BaseURL: server.URL, Client: server.Client()}
			svc1 := NewRemoteLinksWithImports([]RegistryLink{link}, testImportSources("counting-source", src1), cfg, discard)
			svc2 := NewRemoteLinksWithImports([]RegistryLink{link}, testImportSources("counting-source", src2), cfg, discard)
			peers := func() []string { return []string{"s1", "s2"} }
			svc1.SetImportSourceOwnerSource("s1", peers)
			svc2.SetImportSourceOwnerSource("s2", peers)
			seedNodeListView(t, svc1, "n1")
			seedNodeListView(t, svc2, "n1")
			svc1.Start(ctx)
			svc2.Start(ctx)

			// Start is asynchronous: observe an acquired lease before checking
			// exclusivity, including when registration or acquisition is slow.
			deadline := time.Now().Add(5 * time.Second)
			for src1.rangeCalls.Load() == 0 && src2.rangeCalls.Load() == 0 {
				if time.Now().After(deadline) {
					t.Fatal("timed out waiting for a source owner to call Range")
				}
				time.Sleep(5 * time.Millisecond)
			}
			// Keep the full observation window after startup. Counts never reset,
			// so a second source owner at any point still fails the assertion.
			time.Sleep(180 * time.Millisecond)
			c1, c2 := src1.rangeCalls.Load(), src2.rangeCalls.Load()
			if (c1 > 0 && c2 > 0) || (c1 == 0 && c2 == 0) {
				t.Fatalf("Range calls svc1=%d svc2=%d, want exactly one source owner", c1, c2)
			}
		})
	}
}

func TestPlacerStartIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src := newCountingGroupSource("/g")
	svc := NewRemoteLinksWithImports(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		testImportSources("counting-source", src),
		clustercfg.PlacementConfig{ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "500ms", ImportRefresh: "1h"},
		discard,
	)
	svc.SetNodeListLinks(ctx, nil)
	seedNodeListView(t, svc, "n1")
	svc.Start(ctx)
	svc.Start(ctx)

	for i := 0; i < 100; i++ {
		if src.rangeCalls.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if calls := src.rangeCalls.Load(); calls != 1 {
		t.Fatalf("Range calls after duplicate Start=%d, want 1", calls)
	}
}

func TestReconcileGroupImportsRunsWhenNodeListBecomesReady(t *testing.T) {
	oldPoll := placerReadyPollInterval
	placerReadyPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { placerReadyPollInterval = oldPoll })

	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src := newCountingGroupSource("/g")
	svc := NewRemoteLinksWithImports(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		testImportSources("counting-source", src),
		clustercfg.PlacementConfig{ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "500ms", ImportRefresh: "1h"},
		discard,
	)
	go svc.reconcileGroups(ctx)

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

func TestReconcileRunsWhenReadyNodeListChanges(t *testing.T) {
	oldPoll := placerReadyPollInterval
	placerReadyPollInterval = time.Hour
	t.Cleanup(func() { placerReadyPollInterval = oldPoll })

	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, srv := testScaleRegistry(t, ctx, "n1")
	defer srv.Close()
	defer cancel()

	src := newCountingGroupSource("/g")
	svc := NewRemoteLinksWithImports(
		[]RegistryLink{{Name: "registry", BaseURL: srv.URL, Client: srv.Client()}},
		testImportSources("counting-source", src),
		clustercfg.PlacementConfig{ImportSourceOwnerCount: 1, ImportSourceLeaseTTL: "500ms", ImportRefresh: "1h"},
		discard,
	)
	empty := svc.nodes.source("node_list")
	empty.reset()
	empty.bookmark()
	go svc.reconcileGroups(ctx)

	for i := 0; i < 100; i++ {
		if src.rangeCalls.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls := src.rangeCalls.Load(); calls == 0 {
		t.Fatal("initial ready empty node_list did not reconcile")
	}

	sink := svc.nodes.sourceWithNotify("node_list", svc.notifyNodeListChanged)
	raw, err := json.Marshal(clusterstate.NodeListEntry{
		Version: nodeVersionForTest(1, 0),
		NodeID:  "n1", Labels: map[string]string{"pool": "p"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink.put("n1", raw)
	for i := 0; i < 100; i++ {
		if src.rangeCalls.Load() > 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node_list change did not trigger reconcile; Range calls=%d", src.rangeCalls.Load())
}

func TestReconcileIntervalUsesImportRefreshCadence(t *testing.T) {
	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{ImportRefresh: "1m"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got := svc.reconcileInterval(); got != time.Minute {
		t.Fatalf("reconcile interval=%v, want import_refresh_interval", got)
	}
}

func TestReadyForLabelRequiresMatchingNodeListSnapshot(t *testing.T) {
	svc := NewRemoteLinks(nil, clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	oldSink := svc.nodes.sourceWithNotifyLabel("node_list", "registry.1.old", nil)
	oldSink.reset()
	oldSink.bookmark()

	if !svc.Ready() {
		t.Fatal("Ready() should accept any completed node_list snapshot")
	}
	if svc.ReadyForLabel("registry.2.new") {
		t.Fatal("ReadyForLabel accepted a node_list snapshot from a different registry label")
	}

	newSink := svc.nodes.sourceWithNotifyLabel("node_list", "registry.2.new", nil)
	newSink.reset()
	newSink.bookmark()
	if !svc.ReadyForLabel("registry.2.new") {
		t.Fatal("ReadyForLabel did not accept matching node_list snapshot")
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
		Version: nodeVersionForTest(1, 0),
		NodeID:  nodeID, Labels: map[string]string{"pool": "p"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink.put(nodeID, raw)
	sink.bookmark()
}

func TestDecodeNodeListPreservesSplitEndpoints(t *testing.T) {
	raw, err := json.Marshal(clusterstate.NodeListEntry{
		Version:      nodeVersionForTest(1, 0),
		NodeID:       "n1",
		APIEndpoint:  "10.0.0.1:7443",
		DataEndpoint: "10.0.0.1:8443",
	})
	if err != nil {
		t.Fatal(err)
	}
	node, ok := decodeNodeList(raw)
	if !ok || node.APIEndpoint != "10.0.0.1:7443" || node.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("decoded node=%+v ok=%v", node, ok)
	}
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

func (s *pagedFailingGroupSource) Read(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return clusterstate.SandboxGroupRecord{Group: "/g1", NodeSelectors: []map[string]string{{"pool": "p"}}, ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey}, APISecret: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret}}, true, nil
}

func (s *countingGroupSource) Read(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return clusterstate.SandboxGroupRecord{Group: s.group, NodeSelectors: []map[string]string{{"pool": "p"}}, ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey}, APISecret: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret}}, true, nil
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

func (s *countingGroupSource) Range(context.Context, string, int) (clusterstate.GroupPage, error) {
	s.rangeCalls.Add(1)
	return clusterstate.GroupPage{Groups: []string{s.group}}, nil
}

func testScaleRegistry(t *testing.T, ctx context.Context, nodeID string) (*registry.Registry, *httptest.Server) {
	t.Helper()
	stores := testMetadataStores(t)
	if err := stores.PutNode(ctx, &registry.NodeRecord{NodeID: nodeID, Labels: map[string]string{"pool": "p"}, LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	reg := registry.New(stores, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	reg.ServePlacerLink(mux)
	return reg, httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
}

func testMetadataStores(t *testing.T) *registry.Stores {
	t.Helper()
	db := metadatastore.New(t)
	stores, err := registry.NewStores(db.Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	return stores
}
