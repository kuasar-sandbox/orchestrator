package controlplane

import (
	"context"
	"strings"
	"sync"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type acceptingCommandSender struct {
	mu    sync.Mutex
	calls []routesync.Command
}

func TestRegistryServiceProvesExactFinalOutboxAck(t *testing.T) {
	service, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchUnknown)
	consensus := store.runtime.(*serviceConsensus)
	consensus.system.NodeEnrollments["node-1"] = raftstore.NodeEnrollmentRecord{
		NodeID: "node-1", EnrollmentID: "enrollment-1", MaxNodeEpoch: 7,
		DataEndpoint: "node-1:8443", EnrollmentIndex: 1, LastAppliedIndex: 1,
	}
	sender := &acceptingCommandSender{}
	service.commands = sender
	request := raftstore.FenceOutboxAckRequest{
		Group: "/group", RouteKey: "route-1", SandboxID: "sandbox-1",
		NodeID: "node-1", NodeEpoch: 7,
		RegistryGeneration: consensus.system.RegistryGeneration,
		BindingDigest:      strings.Repeat("a", 64), FinalOutboxWatermark: 9,
	}
	evidence, err := service.VerifyFenceOutboxAck(context.Background(), request)
	if err != nil || evidence.AckedWatermark != request.FinalOutboxWatermark ||
		evidence.ProofDigest != finalOutboxAckProof(request) {
		t.Fatalf("final outbox evidence = %+v, %v", evidence, err)
	}
	calls := sender.snapshot()
	if len(calls) != 1 || calls[0].Kind != routesync.CmdFinalizeWorkflow ||
		calls[0].SID != request.SandboxID || calls[0].RegistryGeneration != request.RegistryGeneration ||
		calls[0].BindingDigest != request.BindingDigest {
		t.Fatalf("final outbox command = %+v", calls)
	}

	wrong := request
	wrong.RegistryGeneration = "another-generation"
	if _, err := service.VerifyFenceOutboxAck(context.Background(), wrong); err == nil {
		t.Fatal("another Registry History Generation received final outbox evidence")
	}
	if len(sender.snapshot()) != 1 {
		t.Fatal("invalid final outbox request reached the node")
	}
}

func (s *acceptingCommandSender) SendNodeCommand(
	_ context.Context,
	_ session.ServeIdentity,
	_ string,
	_ uint64,
	_ string,
	command *routesync.Command,
) (routesync.CmdAck, bool, error) {
	s.mu.Lock()
	s.calls = append(s.calls, *command)
	s.mu.Unlock()
	return routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckAccepted}, true, nil
}

func (s *acceptingCommandSender) InstallKeyLease(
	_ context.Context,
	_ session.ServeIdentity,
	_ string,
	_ uint64,
	_ string,
	lease routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, bool, error) {
	ref, err := lease.Ref()
	return ref, err == nil, err
}

func (s *acceptingCommandSender) snapshot() []routesync.Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]routesync.Command(nil), s.calls...)
}

func TestBuildPlacementRejectionsFinalizeBeforePlacementTombstone(t *testing.T) {
	service, store, _, _ := newRegistryServiceFixture(t, clusterstate.DispatchDefinitiveReject)
	sender := &acceptingCommandSender{}
	service.commands = sender
	request := buildMutationRequest(t, store)
	response, err := service.RegisterBuild(context.Background(), request)
	if err != nil || response.Outcome != routeapi.MutationTerminal {
		t.Fatalf("rejected Build = %+v, %v", response, err)
	}
	record, err := store.ReadBuildWorkflow(context.Background(), request.Group, request.BuildID)
	if err != nil || record == nil || record.State != clusterstate.BuildTombstone || record.Tombstone == nil ||
		len(record.Finalizations) != len(record.Tombstone.PlacementFailure.CandidatePool) {
		t.Fatalf("Build placement failure = %+v, %v", record, err)
	}
	for len(record.Finalizations) != 0 {
		committed, changed, err := service.completeBuildFinalization(context.Background(), *record)
		if err != nil || !changed {
			t.Fatalf("complete rejected candidate = %+v, %v, changed=%v", committed, err, changed)
		}
		record = &committed
	}
	if record.State != clusterstate.BuildTombstone || record.Tombstone == nil || len(record.Finalizations) != 0 {
		t.Fatalf("finalized placement tombstone = %+v", record)
	}
	calls := sender.snapshot()
	if len(calls) != len(record.Tombstone.PlacementFailure.CandidatePool) {
		t.Fatalf("FinalizeWorkflow commands = %d", len(calls))
	}
	seen := make(map[string]struct{}, len(calls))
	for _, command := range calls {
		if command.Kind != routesync.CmdFinalizeWorkflow || command.BuildID != request.BuildID ||
			command.SID != "" || command.BindingDigest == "" {
			t.Fatalf("invalid rejected-candidate finalization command: %+v", command)
		}
		seen[command.BindingDigest] = struct{}{}
	}
	if len(seen) != len(calls) {
		t.Fatal("candidate finalizations did not retain distinct Bindings")
	}
}
