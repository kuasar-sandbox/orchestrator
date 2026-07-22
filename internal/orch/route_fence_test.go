package orch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

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
		route.RegistryGeneration != request.ExpectedRegistryGeneration || route.BindingDigest != request.ExpectedBindingDigest {
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
		RegistryGeneration: "generation-1", Kind: clusterstate.ExecutionKindSandbox,
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
		SID: "s1", NodeEpoch: 7, SessionSeq: 3, RegistryGeneration: "generation-1",
		Binding: opaque, BindingDigest: digest, DemandDigest: hex.EncodeToString(demand[:]),
		DispatchSpecDigest: hex.EncodeToString(dispatch[:]),
		Config:             map[string]string{"user": "value", clusterstate.ObjectMetadataKey: "forged"},
	}
	metadata, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindSandbox, "s1", "n1")
	if err != nil {
		t.Fatal(err)
	}
	if metadata[clusterstate.ObjectMetadataKey] != opaque || metadata["user"] != "value" {
		t.Fatalf("metadata = %#v", metadata)
	}
	cmd.BindingDigest = "wrong"
	if _, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindSandbox, "s1", "n1"); err == nil {
		t.Fatal("binding digest mismatch accepted")
	}
	cmd.BindingDigest = digest
	if _, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindBuild, "s1", "n1"); err == nil {
		t.Fatal("binding kind mismatch accepted")
	}
	if _, err := clusterCommandMetadata(cmd, clusterstate.ExecutionKindSandbox, "s1", "n2"); err == nil {
		t.Fatal("binding for another node accepted")
	}
}

func TestRebindClusterExecutionUsesDigestCAS(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	if _, err := o.st.EnrollClusterIdentity(ctx, "n1", "boot-1", "10.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
	demand := sha256.Sum256([]byte("demand"))
	dispatch := sha256.Sum256([]byte("dispatch"))
	makeBinding := func(generation string) string {
		opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
			RegistryGeneration: generation, Kind: clusterstate.ExecutionKindSandbox,
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
		ID: "s1", State: types.StatePaused,
		AuthKey: strings.Repeat("2", 64), ManifestKey: strings.Repeat("1", 64),
		Metadata: map[string]string{clusterstate.ObjectMetadataKey: oldOpaque},
	}); err != nil {
		t.Fatal(err)
	}
	cmd := &routesync.Command{
		SID: "s1", NodeEpoch: 7, SessionSeq: 3, RegistryGeneration: "g2",
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
			event.Route.RegistryGeneration != "g2" || event.Route.BindingDigest != newDigest {
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
		SID: "s1", NodeEpoch: 7, RegistryGeneration: "g1", BindingDigest: oldDigest,
	}); !errors.Is(err, errWrongExecutionBinding) {
		t.Fatalf("old command fence error = %v", err)
	}
	if err := o.verifySandboxCommandBinding(ctx, &routesync.Command{
		SID: "s1", NodeEpoch: 7, RegistryGeneration: "g2", BindingDigest: newDigest,
	}); err != nil {
		t.Fatalf("new command fence: %v", err)
	}
}

func TestRebindClusterExecutionWaitsForSandboxLifecycle(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	if _, err := o.st.EnrollClusterIdentity(ctx, "n1", "boot-1", "10.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
	demand := sha256.Sum256([]byte("demand"))
	dispatch := sha256.Sum256([]byte("dispatch"))
	makeBinding := func(generation string) string {
		opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
			RegistryGeneration: generation, Kind: clusterstate.ExecutionKindSandbox,
			ObjectID: "s1", Group: "/g", RouteKey: "rk", NodeID: "n1", NodeEpoch: 7,
			DemandDigest: demand, DispatchSpecDigest: dispatch,
		})
		if err != nil {
			t.Fatal(err)
		}
		return opaque
	}
	oldOpaque, newOpaque := makeBinding("g1"), makeBinding("g2")
	oldDigest, _ := clusterstate.ExecutionBindingDigest(oldOpaque)
	newDigest, _ := clusterstate.ExecutionBindingDigest(newOpaque)
	if err := o.st.Put(ctx, &types.Sandbox{
		ID: "s1", State: types.StatePaused,
		AuthKey: strings.Repeat("2", 64), ManifestKey: strings.Repeat("1", 64),
		Metadata: map[string]string{clusterstate.ObjectMetadataKey: oldOpaque},
	}); err != nil {
		t.Fatal(err)
	}
	command := &routesync.Command{
		SID: "s1", NodeEpoch: 7, SessionSeq: 3, RegistryGeneration: "g2",
		Binding: newOpaque, BindingDigest: newDigest, OldBindingDigest: oldDigest,
		DemandDigest: hex.EncodeToString(demand[:]), DispatchSpecDigest: hex.EncodeToString(dispatch[:]),
	}

	held, release := make(chan struct{}), make(chan struct{})
	go func() {
		_ = o.lifecycle.Do("s1", func() error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	done := make(chan error, 1)
	go func() { done <- o.rebindClusterExecution(ctx, command) }()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("rebind bypassed the in-flight Sandbox lifecycle: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMalformedVersionedBindingFailsClosed(t *testing.T) {
	for _, opaque := range []string{
		clusterstate.ExecutionBindingPrefix + "malformed",
		"keb2.future-format",
	} {
		sb := &types.Sandbox{
			ID:       "s1",
			Metadata: map[string]string{clusterstate.ObjectMetadataKey: opaque},
		}
		if kind, failed := validateSandboxRouteFence(sb, proxy.RouteRequest{SandboxID: sb.ID}); !failed || kind != proxy.KindWrongBinding {
			t.Fatalf("malformed binding %q failure = (%v,%v)", opaque, kind, failed)
		}
		route := (&Orchestrator{}).routeEntry(sb)
		if route.BindingDigest != "invalid" {
			t.Fatalf("malformed route projection = %+v", route)
		}
	}
}

func TestResumeIfPausedSurfacesConcurrentRebind(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	old, staleRequest := sandboxWithBinding(t)
	current := *old
	currentBinding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-2", Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: current.ID, Group: "/g", RouteKey: "rk", NodeID: "n1", NodeEpoch: 7,
		DemandDigest: sha256.Sum256([]byte("demand")), DispatchSpecDigest: sha256.Sum256([]byte("dispatch")),
	}
	opaque, err := clusterstate.EncodeExecutionBinding(currentBinding)
	if err != nil {
		t.Fatal(err)
	}
	current.Metadata = map[string]string{clusterstate.ObjectMetadataKey: opaque}
	if err := o.st.Put(ctx, &current); err != nil {
		t.Fatal(err)
	}
	o.cache(&current)
	if err := o.resumeIfPaused(ctx, current.ID, staleRequest); !errors.Is(err, errSandboxRouteFenceChanged) {
		t.Fatalf("stale resume error = %v", err)
	}
}

func sandboxWithBinding(t *testing.T) (*types.Sandbox, proxy.RouteRequest) {
	t.Helper()
	binding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1",
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
			AuthKey: strings.Repeat("a", 64), ManifestKey: strings.Repeat("b", 64),
			Metadata: map[string]string{clusterstate.ObjectMetadataKey: opaque},
		}, proxy.RouteRequest{
			SandboxID: "s1", Port: 49983, ExpectedNodeID: "n1", ExpectedNodeEpoch: 7,
			ExpectedRegistryGeneration: "generation-1", ExpectedBindingDigest: digest,
		}
}
