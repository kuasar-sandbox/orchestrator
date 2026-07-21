package controlplane

import (
	"context"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

type boundRouteExecution struct {
	SandboxID          string
	NodeID             string
	NodeEpoch          uint64
	RegistryGeneration string
	BindingDigest      string
	LastEventSeq       uint64
}

func routeBoundExecution(record clusterstate.RouteWorkflowRecord) (boundRouteExecution, bool) {
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		if record.Starting == nil || record.Starting.Binding == nil {
			return boundRouteExecution{}, false
		}
		return boundRouteExecution{
			SandboxID: record.Starting.SandboxID, NodeID: record.Starting.Binding.NodeID,
			NodeEpoch:          record.Starting.Binding.NodeEpoch,
			RegistryGeneration: record.Starting.Binding.RegistryGeneration,
			BindingDigest:      record.Starting.Binding.BindingDigest,
			LastEventSeq:       record.Starting.LastEventSeq,
		}, true
	case clusterstate.WorkflowRouteReady:
		return boundRouteFromProjection(*record.Ready), true
	case clusterstate.WorkflowRoutePaused:
		return boundRouteFromProjection(record.Paused.Execution), true
	case clusterstate.WorkflowRouteResuming:
		return boundRouteFromProjection(record.Resuming.Execution), true
	case clusterstate.WorkflowRouteDeleting:
		execution := boundRouteFromProjection(record.Deleting.Execution)
		execution.LastEventSeq = max(execution.LastEventSeq, record.Deleting.LastEventSeq)
		return execution, true
	case clusterstate.WorkflowRouteTombstone:
		if record.Tombstone == nil || record.Tombstone.PlacementFailure != nil {
			return boundRouteExecution{}, false
		}
		return boundRouteExecution{
			SandboxID: record.Tombstone.SandboxID, NodeID: record.Tombstone.NodeID,
			NodeEpoch: record.Tombstone.NodeEpoch, RegistryGeneration: record.Tombstone.RegistryGeneration,
			BindingDigest: record.Tombstone.BindingDigest, LastEventSeq: record.Tombstone.LastEventSeq,
		}, true
	default:
		return boundRouteExecution{}, false
	}
}

func boundRouteFromProjection(execution clusterstate.ReadyRoute) boundRouteExecution {
	return boundRouteExecution{
		SandboxID: execution.SandboxID, NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
		RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
		LastEventSeq: execution.LastEventSeq,
	}
}

func (s *RaftStore) FenceRouteExecution(
	ctx context.Context,
	record clusterstate.RouteWorkflowRecord,
	proof clusterstate.TerminalProof,
	reason string,
) (clusterstate.RouteWorkflowRecord, error) {
	execution, found := routeBoundExecution(record)
	if !found || reason == "" {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: Route has no exact bound execution to fence")
	}
	if err := proof.Validate(); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if proof.Kind != clusterstate.ProofNewerNodeEpoch && proof.Kind != clusterstate.ProofExternalFence {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: Route fencing requires a permanent execution proof")
	}
	if proof.FencedNodeID != execution.NodeID || proof.FencedNodeEpoch != execution.NodeEpoch {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: Route fencing proof identifies another execution")
	}
	tombstone := clusterstate.RouteTombstoneState{
		SandboxID: execution.SandboxID, NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
		RegistryGeneration: execution.RegistryGeneration,
		BindingDigest:      execution.BindingDigest, LastEventSeq: execution.LastEventSeq,
		Proof: proof, TerminalReason: reason,
	}
	fence := clusterstate.ExecutionFence{
		Group: record.Group, RouteKey: record.RouteKey, SandboxID: execution.SandboxID,
		NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
		RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
		LastEventSeq: execution.LastEventSeq, FinalOutboxWatermark: execution.LastEventSeq, Proof: proof,
	}
	if record.State == clusterstate.WorkflowRouteTombstone {
		current := record.Tombstone
		if current == nil || current.PlacementFailure != nil || current.SandboxID != tombstone.SandboxID ||
			current.NodeID != tombstone.NodeID || current.NodeEpoch != tombstone.NodeEpoch ||
			current.RegistryGeneration != tombstone.RegistryGeneration ||
			current.BindingDigest != tombstone.BindingDigest || current.LastEventSeq != tombstone.LastEventSeq ||
			current.Proof != tombstone.Proof || current.TerminalReason != tombstone.TerminalReason {
			return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: Route is already fenced by another proof")
		}
		if err := s.EnsureExecutionFence(ctx, fence); err != nil {
			return clusterstate.RouteWorkflowRecord{}, err
		}
		return record, nil
	}
	next := clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &tombstone,
	}
	if proof.Kind == clusterstate.ProofExternalFence {
		ctx = withTerminalProofAuthorization(ctx, record.Revision, next)
	}
	committed, err := s.CommitRouteWorkflow(ctx, record.Revision, next)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	if err := s.EnsureExecutionFence(ctx, fence); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	return committed, nil
}

func newerNodeEpochRouteProof(
	state raftstore.SystemState,
	record clusterstate.RouteWorkflowRecord,
) (clusterstate.TerminalProof, bool, error) {
	execution, found := routeBoundExecution(record)
	if !found || record.State == clusterstate.WorkflowRouteTombstone {
		return clusterstate.TerminalProof{}, false, nil
	}
	if state.RegistryGeneration != execution.RegistryGeneration {
		return clusterstate.TerminalProof{}, false, errors.New("controlplane: System state belongs to another Route Registry History Generation")
	}
	enrollment, found := state.NodeEnrollments[execution.NodeID]
	if !found || enrollment.MaxNodeEpoch <= execution.NodeEpoch {
		return clusterstate.TerminalProof{}, false, nil
	}
	if state.LastApplied == 0 || enrollment.EnrollmentID == "" || enrollment.LastAppliedIndex == 0 {
		return clusterstate.TerminalProof{}, false, errors.New("controlplane: newer NodeEpoch lacks committed System evidence")
	}
	proof := clusterstate.TerminalProof{
		Kind:         clusterstate.ProofNewerNodeEpoch,
		FencedNodeID: execution.NodeID, FencedNodeEpoch: execution.NodeEpoch,
		ObservedNodeEpoch: enrollment.MaxNodeEpoch,
		SystemEpoch:       state.SystemEpoch, SystemCommitIndex: state.LastApplied,
		EnrollmentID: enrollment.EnrollmentID, EnrollmentCommitIndex: enrollment.LastAppliedIndex,
	}
	digest, err := clusterstate.NewerNodeEpochProofDigest(
		proof, execution.RegistryGeneration, execution.SandboxID, execution.BindingDigest, execution.LastEventSeq,
	)
	if err != nil {
		return clusterstate.TerminalProof{}, false, err
	}
	proof.ProofDigest = digest
	return proof, true, nil
}

func (s *RegistryService) fenceRouteAfterNodeEpochAdvance(
	ctx context.Context,
	record clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, bool, error) {
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, false, err
	}
	return s.fenceRouteWithSystemState(ctx, state, record)
}

func (s *RegistryService) fenceRouteWithSystemState(
	ctx context.Context,
	state raftstore.SystemState,
	record clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, bool, error) {
	proof, found, err := newerNodeEpochRouteProof(state, record)
	if err != nil || !found {
		return record, false, err
	}
	committed, err := s.store.FenceRouteExecution(
		ctx, record, proof, fmt.Sprintf("node epoch advanced from %d to %d", proof.FencedNodeEpoch, proof.ObservedNodeEpoch),
	)
	return committed, err == nil, err
}
