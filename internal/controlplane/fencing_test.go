package controlplane

import (
	"context"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestReserveFencesSelectedStartingExecutionAfterNewerNodeEpoch(t *testing.T) {
	service, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
	request := sandboxMutationRequest(t, store)
	if response, err := service.ReserveSandbox(context.Background(), request); err != nil || response.Outcome != routeapi.MutationPending {
		t.Fatalf("initial Reserve = %+v, %v", response, err)
	}
	before, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || before == nil || before.Starting == nil || before.Starting.Binding == nil {
		t.Fatalf("selected STARTING = %+v, %v", before, err)
	}
	oldSID := before.Starting.SandboxID
	binding := *before.Starting.Binding
	consensus := store.runtime.(*serviceConsensus)
	consensus.system.LastApplied = 20
	consensus.system.NodeEnrollments[binding.NodeID] = raftstore.NodeEnrollmentRecord{
		NodeID: binding.NodeID, EnrollmentID: "enrollment-new", MaxNodeEpoch: binding.NodeEpoch + 1,
		DataEndpoint: binding.DataEndpoint, EnrollmentIndex: 10, LastAppliedIndex: 20,
	}

	request.MinRouteRevision = before.Revision.LogIndex + 1
	if response, err := service.ReserveSandbox(context.Background(), request); err != nil || response.Outcome != routeapi.MutationPending {
		t.Fatalf("Reserve after NodeEpoch advance = %+v, %v", response, err)
	}
	after, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || after == nil || after.State != clusterstate.WorkflowRouteStarting ||
		after.Starting == nil || after.Starting.SandboxID == oldSID {
		t.Fatalf("replacement STARTING = %+v, %v", after, err)
	}
	if consensus.fenceCount() != 1 {
		t.Fatalf("execution fence count = %d, want 1", consensus.fenceCount())
	}
	fence := findExecutionFence(t, consensus, oldSID)
	if fence.LastEventSeq != 0 || fence.Proof.Kind != clusterstate.ProofNewerNodeEpoch ||
		fence.Proof.FencedNodeID != binding.NodeID || fence.Proof.FencedNodeEpoch != binding.NodeEpoch ||
		fence.Proof.ObservedNodeEpoch != binding.NodeEpoch+1 || fence.Proof.SystemCommitIndex != 20 ||
		fence.Proof.EnrollmentID != "enrollment-new" || fence.Proof.EnrollmentCommitIndex != 20 {
		t.Fatalf("newer-NodeEpoch execution fence = %+v", fence)
	}
}

func TestReserveDoesNotFenceWithoutStrictlyNewerNodeEpoch(t *testing.T) {
	service, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
	request := sandboxMutationRequest(t, store)
	if _, err := service.ReserveSandbox(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	before, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || before == nil || before.Starting == nil || before.Starting.Binding == nil {
		t.Fatalf("selected STARTING = %+v, %v", before, err)
	}
	binding := *before.Starting.Binding
	consensus := store.runtime.(*serviceConsensus)
	consensus.system.LastApplied = 20
	consensus.system.NodeEnrollments[binding.NodeID] = raftstore.NodeEnrollmentRecord{
		NodeID: binding.NodeID, EnrollmentID: "enrollment-same", MaxNodeEpoch: binding.NodeEpoch,
		DataEndpoint: binding.DataEndpoint, EnrollmentIndex: 10, LastAppliedIndex: 20,
	}

	request.MinRouteRevision = before.Revision.LogIndex + 1
	if response, err := service.ReserveSandbox(context.Background(), request); err != nil || response.Outcome != routeapi.MutationPending {
		t.Fatalf("same-epoch Reserve = %+v, %v", response, err)
	}
	after, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || after == nil || after.Starting == nil || after.Starting.SandboxID != before.Starting.SandboxID {
		t.Fatalf("same epoch replaced execution: before=%+v after=%+v err=%v", before, after, err)
	}
	if consensus.fenceCount() != 0 {
		t.Fatalf("same NodeEpoch created %d fences", consensus.fenceCount())
	}
}

func TestOperatorExternalFenceIsExactCASAndIdempotent(t *testing.T) {
	registry, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
	request := sandboxMutationRequest(t, store)
	if _, err := registry.ReserveSandbox(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || record == nil || record.Starting == nil || record.Starting.Binding == nil {
		t.Fatalf("selected STARTING = %+v, %v", record, err)
	}
	binding := *record.Starting.Binding
	generation, err := store.RegistryServeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	service := &OperatorService{store: store}
	fence := FenceRouteRequest{
		RegistryServeIdentity: generation, Group: record.Group, RouteKey: record.RouteKey,
		SandboxID: record.Starting.SandboxID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
		BindingDigest: binding.BindingDigest, ExpectedRouteRevision: record.Revision.LogIndex,
		ProofDigest: strings.Repeat("a", 64), Reason: "authenticated infrastructure fence",
	}
	wrong := fence
	wrong.BindingDigest = strings.Repeat("b", 64)
	if err := service.FenceRoute(context.Background(), wrong); err == nil {
		t.Fatal("external fence accepted a mismatched Binding digest")
	}
	consensus := store.runtime.(*serviceConsensus)
	if consensus.fenceCount() != 0 {
		t.Fatal("rejected external fence mutated consensus state")
	}
	if err := service.FenceRoute(context.Background(), fence); err != nil {
		t.Fatal(err)
	}
	terminal, err := store.ReadRouteWorkflow(context.Background(), request.Group, request.RouteKey)
	if err != nil || terminal == nil || terminal.State != clusterstate.WorkflowRouteTombstone ||
		terminal.Tombstone == nil || terminal.Tombstone.Proof.Kind != clusterstate.ProofExternalFence ||
		terminal.Tombstone.Proof.ProofDigest != fence.ProofDigest {
		t.Fatalf("external-fenced Route = %+v, %v", terminal, err)
	}
	if consensus.fenceCount() != 1 {
		t.Fatalf("external fence count = %d, want 1", consensus.fenceCount())
	}
	if err := service.FenceRoute(context.Background(), fence); err != nil {
		t.Fatalf("identical external-fence retry failed: %v", err)
	}
}

func findExecutionFence(t *testing.T, consensus *serviceConsensus, sandboxID string) clusterstate.ExecutionFence {
	t.Helper()
	consensus.mu.Lock()
	defer consensus.mu.Unlock()
	for _, state := range consensus.states {
		for _, fence := range state.Fences {
			if fence.SandboxID == sandboxID {
				return fence
			}
		}
	}
	t.Fatalf("execution fence for %q not found", sandboxID)
	return clusterstate.ExecutionFence{}
}
