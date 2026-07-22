package controlplane

import (
	"context"
	"errors"
	"reflect"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type terminalProofContextKey struct{}

type terminalProofAuthorization struct {
	Revision  clusterstate.Revision
	Group     string
	RouteKey  string
	Tombstone clusterstate.RouteTombstoneState
}

func withTerminalProofAuthorization(
	ctx context.Context,
	revision clusterstate.Revision,
	next clusterstate.RouteWorkflowRecord,
) context.Context {
	authorization := terminalProofAuthorization{
		Revision: revision, Group: next.Group, RouteKey: next.RouteKey, Tombstone: *next.Tombstone,
	}
	return context.WithValue(ctx, terminalProofContextKey{}, authorization)
}

func (s *RaftStore) CommitNodeTerminalRoute(
	ctx context.Context,
	expected clusterstate.Revision,
	next clusterstate.RouteWorkflowRecord,
	event routesync.ExecutionEvent,
) (clusterstate.RouteWorkflowRecord, error) {
	if err := event.Validate(); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	binding, err := clusterstate.DecodeExecutionBinding(event.Binding)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	proofDigest, err := terminalEventDigest(next.Group, next.RouteKey, event)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	tombstone := next.Tombstone
	if next.State != clusterstate.WorkflowRouteTombstone || tombstone == nil || tombstone.PlacementFailure != nil ||
		binding.Kind != clusterstate.ExecutionKindSandbox || binding.Group != next.Group ||
		binding.RouteKey != next.RouteKey || binding.ObjectID != event.ObjectID ||
		tombstone.SandboxID != event.ObjectID || tombstone.NodeID != event.NodeID ||
		tombstone.NodeEpoch != event.NodeEpoch || tombstone.RegistryGeneration != event.RegistryGeneration ||
		tombstone.BindingDigest != event.BindingDigest || tombstone.LastEventSeq != event.EventSeq ||
		tombstone.Proof.Kind != clusterstate.ProofNodeTerminal ||
		tombstone.Proof.FencedNodeID != event.NodeID || tombstone.Proof.FencedNodeEpoch != event.NodeEpoch ||
		tombstone.Proof.ProofDigest != proofDigest {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: node terminal proof does not match the authenticated event")
	}
	ctx = withTerminalProofAuthorization(ctx, expected, next)
	return s.CommitRouteWorkflow(ctx, expected, next)
}

func (s *RaftStore) applyDataCommand(
	ctx context.Context,
	command raftstore.DataCommand,
) (raftstore.DataApplyResult, error) {
	requiresProof := command.Type == raftstore.DataPutRoute && command.Route != nil &&
		command.Route.State == clusterstate.WorkflowRouteTombstone && command.Route.Tombstone != nil &&
		command.Route.Tombstone.PlacementFailure == nil ||
		command.Type == raftstore.DataPutFence && command.Fence != nil && command.Fence.PlacementFailure == nil
	if !requiresProof {
		return s.runtime.ApplyData(ctx, command)
	}
	proven, ok := s.runtime.(interface {
		ApplyProvenExecutionMutation(context.Context, raftstore.DataCommand) (raftstore.DataApplyResult, error)
	})
	if !ok {
		return raftstore.DataApplyResult{}, errors.New("controlplane: consensus runtime has no trusted terminal-proof workflow")
	}
	return proven.ApplyProvenExecutionMutation(ctx, command)
}

// VerifyTerminalProof is registered directly with the Raft runtime. It accepts
// only evidence introduced at an authenticated node/operator boundary or a
// newer NodeEpoch proven by committed System Group state.
func (s *RaftStore) VerifyTerminalProof(ctx context.Context, request raftstore.TerminalProofRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Fence != nil {
		// The exact proof already passed this verifier before the tombstone was
		// committed. Generic Raft writes cannot create that tombstone or fence.
		return nil
	}
	switch request.Tombstone.Proof.Kind {
	case clusterstate.ProofNewerNodeEpoch:
		return s.verifyNewerNodeEpochProof(ctx, request)
	case clusterstate.ProofNodeTerminal, clusterstate.ProofExternalFence:
		authorization, ok := ctx.Value(terminalProofContextKey{}).(terminalProofAuthorization)
		if !ok || authorization.Revision != request.CurrentRoute.Revision ||
			authorization.Group != request.CurrentRoute.Group || authorization.RouteKey != request.CurrentRoute.RouteKey ||
			!reflect.DeepEqual(authorization.Tombstone, request.Tombstone) {
			return errors.New("controlplane: terminal proof did not originate at its authenticated boundary")
		}
		return nil
	default:
		return errors.New("controlplane: unsupported terminal proof kind")
	}
}

func (s *RaftStore) verifyNewerNodeEpochProof(
	ctx context.Context,
	request raftstore.TerminalProofRequest,
) error {
	state, err := s.runtime.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	proof := request.Tombstone.Proof
	if state.Identity() != request.Identity.PermitIdentity || state.SystemEpoch != proof.SystemEpoch ||
		state.LastApplied < proof.SystemCommitIndex {
		return errors.New("controlplane: newer NodeEpoch proof is not covered by current committed System state")
	}
	enrollment, found := state.NodeEnrollments[proof.FencedNodeID]
	if !found || enrollment.EnrollmentID != proof.EnrollmentID ||
		enrollment.MaxNodeEpoch < proof.ObservedNodeEpoch || enrollment.LastAppliedIndex < proof.EnrollmentCommitIndex {
		return errors.New("controlplane: newer NodeEpoch proof does not match committed node enrollment")
	}
	return nil
}
