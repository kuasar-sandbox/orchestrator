package store

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCASExecutionBinding(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	oldOpaque := testOpaqueBinding(t, "generation-1", "s1", "n1", 7)
	newOpaque := testOpaqueBinding(t, "generation-2", "s1", "n1", 7)
	if err := st.Put(ctx, &types.Sandbox{
		ID: "s1", State: types.StatePaused, ManifestKey: strings.Repeat("1", 64),
		Metadata: map[string]string{"user": "value", clusterstate.ObjectMetadataKey: oldOpaque},
	}); err != nil {
		t.Fatal(err)
	}
	oldDigest, err := clusterstate.ExecutionBindingDigest(oldOpaque)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, newOpaque)
	if err != nil || !changed {
		t.Fatalf("CAS changed=%v err=%v", changed, err)
	}
	changed, err = st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, newOpaque)
	if err != nil || !changed {
		t.Fatalf("idempotent CAS changed=%v err=%v", changed, err)
	}
	got, err := st.Get(ctx, "s1")
	if err != nil || got.Metadata[clusterstate.ObjectMetadataKey] != newOpaque || got.Metadata["user"] != "value" {
		t.Fatalf("sandbox after CAS = %+v err=%v", got, err)
	}

	third := testOpaqueBinding(t, "generation-3", "s1", "n1", 7)
	if changed, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", oldDigest, third); err != nil || changed {
		t.Fatalf("stale CAS changed=%v err=%v", changed, err)
	}
	wrongEpoch := testOpaqueBinding(t, "generation-3", "s1", "n1", 8)
	newDigest, _ := clusterstate.ExecutionBindingDigest(newOpaque)
	if _, err := st.CASExecutionBinding(ctx, clusterstate.ExecutionKindSandbox, "s1", newDigest, wrongEpoch); err == nil {
		t.Fatal("CAS changed NodeEpoch")
	}
}

func testOpaqueBinding(t *testing.T, generation, objectID, nodeID string, nodeEpoch uint64) string {
	t.Helper()
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		StorageGeneration: generation, Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: objectID, Group: "/g", RouteKey: "rk", NodeID: nodeID, NodeEpoch: nodeEpoch,
		DemandDigest: sha256.Sum256([]byte("demand")), DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	})
	if err != nil {
		t.Fatal(err)
	}
	return opaque
}
