package orch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSandboxRouteFenceProjectsOpaqueBinding(t *testing.T) {
	sb, request := sandboxWithBinding(t)
	if kind, failed := validateSandboxRouteFence(sb, request); failed {
		t.Fatalf("exact fence failed with kind %v", kind)
	}
	o := &Orchestrator{}
	route := o.routeEntry(sb)
	if route.NodeID != request.ExpectedNodeID || route.NodeEpoch != request.ExpectedNodeEpoch ||
		route.StorageGeneration != request.ExpectedStorageGeneration || route.BindingDigest != request.ExpectedBindingDigest {
		t.Fatalf("route projection = %+v, request = %+v", route, request)
	}

	stale := request
	stale.ExpectedBindingDigest = "old"
	if kind, failed := validateSandboxRouteFence(sb, stale); !failed || kind != proxy.KindWrongBinding {
		t.Fatalf("stale binding failure = (%v,%v)", kind, failed)
	}
	stale = request
	stale.ExpectedNodeEpoch--
	if kind, failed := validateSandboxRouteFence(sb, stale); !failed || kind != proxy.KindWrongNodeEpoch {
		t.Fatalf("stale epoch failure = (%v,%v)", kind, failed)
	}
}

func TestClusterCommandInstallsOnlyValidatedBinding(t *testing.T) {
	demand := sha256.Sum256([]byte("demand"))
	dispatch := sha256.Sum256([]byte("dispatch"))
	binding := clusterstate.ExecutionBinding{
		StorageGeneration: "generation-1", Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: "s1", Group: "/g", RouteKey: "rk", NodeID: "n1", NodeEpoch: 7,
		DemandDigest: demand, DispatchSpecDigest: dispatch,
	}
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	cmd := &routesync.Command{
		SID: "s1", NodeEpoch: 7, SessionSeq: 3, StorageGeneration: "generation-1",
		Binding: opaque, BindingDigest: digest, DemandDigest: hex.EncodeToString(demand[:]),
		DispatchSpecDigest: hex.EncodeToString(dispatch[:]),
		Config:             map[string]string{"user": "value", clusterstate.ObjectMetadataKey: "forged"},
	}
	metadata, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindSandbox, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if metadata[clusterstate.ObjectMetadataKey] != opaque || metadata["user"] != "value" {
		t.Fatalf("metadata = %#v", metadata)
	}
	cmd.BindingDigest = "wrong"
	if _, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindSandbox, "s1"); err == nil {
		t.Fatal("binding digest mismatch accepted")
	}
	cmd.BindingDigest = digest
	if _, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindBuild, "s1"); err == nil {
		t.Fatal("binding kind mismatch accepted")
	}
}

func TestRebindClusterExecutionUsesDigestCAS(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	demand := sha256.Sum256([]byte("demand"))
	dispatch := sha256.Sum256([]byte("dispatch"))
	makeBinding := func(generation string) string {
		opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
			StorageGeneration: generation, Kind: clusterstate.ExecutionKindSandbox,
			ObjectID: "s1", Group: "/g", RouteKey: "rk", NodeID: "n1", NodeEpoch: 7,
			DemandDigest: demand, DispatchSpecDigest: dispatch,
		})
		if err != nil {
			t.Fatal(err)
		}
		return opaque
	}
	oldOpaque := makeBinding("g1")
	newOpaque := makeBinding("g2")
	oldDigest, _ := clusterstate.ExecutionBindingDigest(oldOpaque)
	newDigest, _ := clusterstate.ExecutionBindingDigest(newOpaque)
	if err := o.st.Put(ctx, &types.Sandbox{
		ID: "s1", State: types.StatePaused, ManifestKey: strings.Repeat("1", 64),
		Metadata: map[string]string{clusterstate.ObjectMetadataKey: oldOpaque},
	}); err != nil {
		t.Fatal(err)
	}
	cmd := &routesync.Command{
		SID: "s1", NodeEpoch: 7, SessionSeq: 3, StorageGeneration: "g2",
		Binding: newOpaque, BindingDigest: newDigest, OldBindingDigest: oldDigest,
		DemandDigest: hex.EncodeToString(demand[:]), DispatchSpecDigest: hex.EncodeToString(dispatch[:]),
	}
	routeEvents, cancel := o.Subscribe()
	defer cancel()
	if err := o.rebindClusterExecution(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-routeEvents:
		if event.Kind != routesync.TypeUpsert || event.Route.SandboxID != "s1" ||
			event.Route.StorageGeneration != "g2" || event.Route.BindingDigest != newDigest {
			t.Fatalf("rebind route event = %+v", event)
		}
	default:
		t.Fatal("successful rebind did not publish the new route fence")
	}
	if err := o.rebindClusterExecution(ctx, cmd); err != nil {
		t.Fatalf("idempotent rebind: %v", err)
	}
	if sb := o.lookup("s1"); sb == nil || sb.Metadata[clusterstate.ObjectMetadataKey] != newOpaque {
		t.Fatalf("cached sandbox after rebind = %+v", sb)
	}
	if err := o.verifySandboxCommandBinding(ctx, &routesync.Command{
		SID: "s1", NodeEpoch: 7, StorageGeneration: "g1", BindingDigest: oldDigest,
	}); !errors.Is(err, errWrongExecutionBinding) {
		t.Fatalf("old command fence error = %v", err)
	}
	if err := o.verifySandboxCommandBinding(ctx, &routesync.Command{
		SID: "s1", NodeEpoch: 7, StorageGeneration: "g2", BindingDigest: newDigest,
	}); err != nil {
		t.Fatalf("new command fence: %v", err)
	}
}

func TestMalformedVersionedBindingFailsClosed(t *testing.T) {
	sb := &types.Sandbox{
		ID: "s1",
		Metadata: map[string]string{
			clusterstate.ObjectMetadataKey: clusterstate.ExecutionBindingPrefix + "malformed",
		},
	}
	if kind, failed := validateSandboxRouteFence(sb, proxy.RouteRequest{SandboxID: sb.ID}); !failed || kind != proxy.KindWrongBinding {
		t.Fatalf("malformed binding failure = (%v,%v)", kind, failed)
	}
	route := (&Orchestrator{}).routeEntry(sb)
	if route.BindingDigest != "invalid" {
		t.Fatalf("malformed route projection = %+v", route)
	}
}

func sandboxWithBinding(t *testing.T) (*types.Sandbox, proxy.RouteRequest) {
	t.Helper()
	binding := clusterstate.ExecutionBinding{
		StorageGeneration:  "generation-1",
		Kind:               clusterstate.ExecutionKindSandbox,
		ObjectID:           "s1",
		Group:              "/g",
		RouteKey:           "rk",
		NodeID:             "n1",
		NodeEpoch:          7,
		DemandDigest:       sha256.Sum256([]byte("demand")),
		DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	}
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return &types.Sandbox{
			ID: "s1", TemplateID: "e2b-snp-template", State: types.StatePaused,
			Metadata: map[string]string{clusterstate.ObjectMetadataKey: opaque},
		}, proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedStorageGeneration: "generation-1", ExpectedBindingDigest: digest,
		}
}
