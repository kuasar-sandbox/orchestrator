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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// TestRemoteScalerViewPlace drives the full standalone-scaler path: the scaler
// subscribes the registry's node/group view watches, builds a local view, and the
// registry's remotePlacer gets a placement over /scaler/place.
func TestRemoteScalerViewPlace(t *testing.T) {
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
	opMux := http.NewServeMux()
	reg.ServeOp(opMux)
	opSrv := httptest.NewServer(opMux)
	defer opSrv.Close()
	defer cancel() // LIFO: cancel before opSrv.Close so the scaler closes its live watch conns first

	svc := NewRemote(strings.TrimPrefix(opSrv.URL, "http://"), nil, clustercfg.ScalerConfig{PlaceCandidates: 2}, discard)
	svc.Start(ctx)

	// Poll until both views sync, then placement must pick the colder eligible node
	// (n2, Counts=0) — P2C without replacement over 2 nodes is deterministic.
	var node string
	for i := 0; i < 200; i++ {
		if node, err = svc.Place("/g", "rk"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("scaler view never synced: %v", err)
	}
	if node != "n2" {
		t.Fatalf("Place over view = %q, want colder n2", node)
	}

	// remotePlacer (registry side) over the scaler's /scaler/place op.
	scalerSrv := httptest.NewServer(svc.Handler())
	defer scalerSrv.Close()
	rp := registry.NewRemotePlacer(strings.TrimPrefix(scalerSrv.URL, "http://"), nil, time.Second)
	rnode, rerr := rp.PlaceSandbox(ctx, "/g", "rk")
	if rerr != nil || rnode != "n2" {
		t.Fatalf("remotePlacer: node=%q err=%v (want n2)", rnode, rerr)
	}

	// An unplaceable group (no matching nodes) maps to ErrNoNode through the op.
	stores.PutGroup(ctx, &registry.GroupConfig{Group: "/x", NodeSelectors: []map[string]string{{"pool": "absent"}}})
	for i := 0; i < 200; i++ {
		if _, perr := svc.Place("/x", "rk"); perr == registry.ErrNoNode {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, rerr := rp.PlaceSandbox(ctx, "/x", "rk"); rerr != registry.ErrNoNode {
		t.Fatalf("remotePlacer unplaceable group: err=%v, want ErrNoNode", rerr)
	}
}
