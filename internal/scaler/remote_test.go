package scaler

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// TestScalerLinkReverseCall drives the full P2 reverse-call in-process: a registry
// (op + scaler-link over h2c) + a standalone scaler dialing it; the registry's
// channelPlacer reverse-requests placement and the scaler answers over its view.
func TestScalerLinkReverseCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "reg.db"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	stores := registry.NewStores(kv, nil)
	stores.PutNode(ctx, &registry.NodeRecord{NodeID: "n1", Labels: map[string]string{"pool": "p"}, Counts: 5})
	stores.PutNode(ctx, &registry.NodeRecord{NodeID: "n2", Labels: map[string]string{"pool": "p"}, Counts: 0})
	stores.PutGroup(ctx, &registry.GroupConfig{Group: "/g", NodeSelectors: []map[string]string{{"pool": "p"}}})

	reg := registry.New(stores, nil, 0, discard)
	placer := registry.NewChannelPlacer(reg, 2*time.Second)
	reg.SetPlacer(placer)

	mux := http.NewServeMux()
	reg.ServeOp(mux)
	mux.HandleFunc("/internal/scaler-link", reg.ServeScalerLink)
	opSrv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer opSrv.Close()
	defer cancel() // LIFO: cancel before closing the server so the scaler tears down its streams first

	svc := NewRemote(strings.TrimPrefix(opSrv.URL, "http://"), nil, clustercfg.ScalerConfig{PlaceCandidates: 2}, 30, discard)
	svc.Start(ctx)

	// Poll until the scaler-link + views are up and placement resolves to the colder
	// node (n2, Counts=0) via the reverse-call.
	var node string
	for i := 0; i < 300; i++ {
		if node, err = placer.Place(ctx, registry.PlaceRequest{Group: "/g", RouteKey: "rk"}); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || node != "n2" {
		t.Fatalf("reverse-call Place = %q err=%v (want colder n2)", node, err)
	}

	// An unplaceable group (no matching nodes) → ErrNoNode through the reverse-call.
	stores.PutGroup(ctx, &registry.GroupConfig{Group: "/x", NodeSelectors: []map[string]string{{"pool": "absent"}}})
	for i := 0; i < 300; i++ {
		_, perr := placer.Place(ctx, registry.PlaceRequest{Group: "/x", RouteKey: "rk"})
		if perr == registry.ErrNoNode {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("unplaceable group never returned ErrNoNode")
}

// TestChannelPlacerNoScaler: with no scaler attached, placement stalls (ErrNoNode),
// not a hang — the data plane (router cache) is unaffected (cluster.md §11).
func TestChannelPlacerNoScaler(t *testing.T) {
	kv, err := clusterstore.Open(filepath.Join(t.TempDir(), "r.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	reg := registry.New(registry.NewStores(kv, nil), nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	placer := registry.NewChannelPlacer(reg, 200*time.Millisecond)
	if _, err := placer.Place(context.Background(), registry.PlaceRequest{Group: "/g"}); err != registry.ErrNoNode {
		t.Fatalf("no scaler → want ErrNoNode, got %v", err)
	}
}
