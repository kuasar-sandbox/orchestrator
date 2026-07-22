package raftstore

import (
	"context"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

// TerminalProofRequest binds a claimed proof to the exact committed Route and
// optional fence. The verifier is implemented by the trusted node-event,
// committed NodeEpoch, or authenticated operator-proof boundary.
type TerminalProofRequest struct {
	Identity     ShardRequestIdentity             `json:"identity"`
	CurrentRoute clusterstate.RouteWorkflowRecord `json:"current_route"`
	Tombstone    clusterstate.RouteTombstoneState `json:"tombstone"`
	Fence        *clusterstate.ExecutionFence     `json:"fence,omitempty"`
}

type TerminalProofVerifier interface {
	VerifyTerminalProof(context.Context, TerminalProofRequest) error
}

type TerminalProofVerifierFunc func(context.Context, TerminalProofRequest) error

func (f TerminalProofVerifierFunc) VerifyTerminalProof(ctx context.Context, request TerminalProofRequest) error {
	return f(ctx, request)
}

func (r *Runtime) SetTerminalProofVerifier(verifier TerminalProofVerifier) error {
	if r == nil || verifier == nil {
		return errors.New("raftstore: trusted terminal proof verifier is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminalVerifier != nil {
		return errors.New("raftstore: trusted terminal proof verifier is already configured")
	}
	r.terminalVerifier = verifier
	return nil
}

func (r *Runtime) trustedTerminalProofVerifier() TerminalProofVerifier {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminalVerifier
}

// ApplyProvenExecutionMutation is the only public runtime path for an
// execution tombstone or execution fence. Generic row mutation cannot assert
// terminal, newer-epoch, or external-fence evidence.
func (r *Runtime) ApplyProvenExecutionMutation(
	ctx context.Context,
	command DataCommand,
) (DataApplyResult, error) {
	request, err := r.terminalProofRequest(ctx, command)
	if err != nil {
		return DataApplyResult{}, err
	}
	verifier := r.trustedTerminalProofVerifier()
	if verifier == nil {
		return DataApplyResult{}, errors.New("raftstore: trusted terminal proof verifier is unavailable")
	}
	if err := verifier.VerifyTerminalProof(ctx, request); err != nil {
		return DataApplyResult{}, fmt.Errorf("raftstore: terminal proof was not verified: %w", err)
	}
	if err := r.authorizeLocalDataReplica(command.Identity); err != nil {
		return DataApplyResult{}, err
	}
	if err := r.permitCache.Authorize(command.Identity.PermitIdentity, PermitRegistryWrite); err != nil {
		return DataApplyResult{}, err
	}
	return r.applyDataMutation(ctx, command)
}

func (r *Runtime) terminalProofRequest(
	ctx context.Context,
	command DataCommand,
) (TerminalProofRequest, error) {
	if command.Type != DataPutRoute && command.Type != DataPutFence {
		return TerminalProofRequest{}, errors.New("raftstore: proven execution workflow supports only tombstones and fences")
	}
	var (
		group     string
		routeKey  string
		tombstone clusterstate.RouteTombstoneState
		fence     *clusterstate.ExecutionFence
	)
	if command.Type == DataPutRoute {
		if command.Route == nil || command.Route.State != clusterstate.WorkflowRouteTombstone ||
			command.Route.Tombstone == nil || command.Route.Tombstone.PlacementFailure != nil ||
			command.Expect.Absent || command.Expect.LogIndex == 0 {
			return TerminalProofRequest{}, errors.New("raftstore: proven Route mutation requires an existing execution tombstone transition")
		}
		group, routeKey = command.Route.Group, command.Route.RouteKey
		tombstone = *command.Route.Tombstone
	} else {
		if command.Fence == nil {
			return TerminalProofRequest{}, errors.New("raftstore: proven fence mutation requires an execution fence")
		}
		group, routeKey = command.Fence.Group, command.Fence.RouteKey
		fenceCopy := *command.Fence
		fence = &fenceCopy
	}
	lookup, err := r.ReadData(ctx, DataLookup{Workflow: &WorkflowLookup{
		Identity: command.Identity, Group: group, RouteKey: routeKey,
	}})
	if err != nil {
		return TerminalProofRequest{}, err
	}
	if lookup.Workflow == nil || !lookup.Workflow.Available || lookup.Workflow.Route == nil {
		return TerminalProofRequest{}, errors.New("raftstore: terminal proof target Route is unavailable")
	}
	current := cloneRouteRecord(*lookup.Workflow.Route)
	if command.Type == DataPutRoute {
		if current.Revision.LogIndex != command.Expect.LogIndex {
			return TerminalProofRequest{}, errors.New("raftstore: terminal proof targets a stale Route revision")
		}
	} else {
		if current.State != clusterstate.WorkflowRouteTombstone || current.Tombstone == nil ||
			current.Tombstone.PlacementFailure != nil {
			return TerminalProofRequest{}, errors.New("raftstore: execution fence requires a proven Route tombstone")
		}
		tombstone = *current.Tombstone
	}
	request := TerminalProofRequest{
		Identity: command.Identity, CurrentRoute: current, Tombstone: tombstone, Fence: fence,
	}
	if err := request.Validate(); err != nil {
		return TerminalProofRequest{}, err
	}
	return request, nil
}

func (r TerminalProofRequest) Validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	if err := r.CurrentRoute.Validate(); err != nil {
		return err
	}
	tombstoneForValidation := r.Tombstone
	if tombstoneForValidation.FailureRevision == (clusterstate.Revision{}) {
		tombstoneForValidation.FailureRevision = r.CurrentRoute.Revision
	}
	if err := tombstoneForValidation.Validate(r.CurrentRoute.Group, r.CurrentRoute.RouteKey); err != nil {
		return err
	}
	if r.Tombstone.PlacementFailure != nil || r.Tombstone.FenceCompacted ||
		r.CurrentRoute.Group == "" || r.CurrentRoute.RouteKey == "" {
		return errors.New("raftstore: terminal proof request is not an uncompacted execution tombstone")
	}
	execution, found := boundExecutionForProof(r.CurrentRoute)
	if !found || execution.SandboxID != r.Tombstone.SandboxID || execution.NodeID != r.Tombstone.NodeID ||
		execution.NodeEpoch != r.Tombstone.NodeEpoch || execution.RegistryGeneration != r.Tombstone.RegistryGeneration ||
		execution.BindingDigest != r.Tombstone.BindingDigest || r.Tombstone.LastEventSeq < execution.LastEventSeq ||
		r.Tombstone.Proof.FencedNodeID != execution.NodeID ||
		r.Tombstone.Proof.FencedNodeEpoch != execution.NodeEpoch {
		return errors.New("raftstore: terminal proof does not identify the committed execution")
	}
	if r.Fence != nil {
		fenceForValidation := *r.Fence
		if fenceForValidation.Revision == (clusterstate.Revision{}) {
			fenceForValidation.Revision = r.CurrentRoute.Revision
		}
		if err := fenceForValidation.Validate(); err != nil {
			return err
		}
		route := clusterstate.RouteWorkflowRecord{
			Group: r.CurrentRoute.Group, RouteKey: r.CurrentRoute.RouteKey,
			State: clusterstate.WorkflowRouteTombstone, Tombstone: &r.Tombstone,
		}
		if !fenceMatchesRouteTombstone(*r.Fence, route) || r.Fence.FinalOutboxWatermark < r.Fence.LastEventSeq {
			return errors.New("raftstore: execution fence does not match the proven Route tombstone")
		}
	}
	return nil
}

type proofBoundExecution struct {
	SandboxID          string
	NodeID             string
	NodeEpoch          uint64
	RegistryGeneration string
	BindingDigest      string
	LastEventSeq       uint64
}

func boundExecutionForProof(record clusterstate.RouteWorkflowRecord) (proofBoundExecution, bool) {
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		if record.Starting == nil || record.Starting.Binding == nil {
			return proofBoundExecution{}, false
		}
		return proofBoundExecution{
			SandboxID: record.Starting.SandboxID, NodeID: record.Starting.Binding.NodeID,
			NodeEpoch: record.Starting.Binding.NodeEpoch, RegistryGeneration: record.Starting.Binding.RegistryGeneration,
			BindingDigest: record.Starting.Binding.BindingDigest, LastEventSeq: record.Starting.LastEventSeq,
		}, true
	case clusterstate.WorkflowRouteReady:
		return proofBoundFromReady(*record.Ready), true
	case clusterstate.WorkflowRoutePaused:
		return proofBoundFromReady(record.Paused.Execution), true
	case clusterstate.WorkflowRouteResuming:
		return proofBoundFromReady(record.Resuming.Execution), true
	case clusterstate.WorkflowRouteDeleting:
		execution := proofBoundFromReady(record.Deleting.Execution)
		if record.Deleting.LastEventSeq > execution.LastEventSeq {
			execution.LastEventSeq = record.Deleting.LastEventSeq
		}
		return execution, true
	case clusterstate.WorkflowRouteTombstone:
		if record.Tombstone == nil || record.Tombstone.PlacementFailure != nil {
			return proofBoundExecution{}, false
		}
		return proofBoundExecution{
			SandboxID: record.Tombstone.SandboxID, NodeID: record.Tombstone.NodeID,
			NodeEpoch: record.Tombstone.NodeEpoch, RegistryGeneration: record.Tombstone.RegistryGeneration,
			BindingDigest: record.Tombstone.BindingDigest, LastEventSeq: record.Tombstone.LastEventSeq,
		}, true
	default:
		return proofBoundExecution{}, false
	}
}

func proofBoundFromReady(execution clusterstate.ReadyRoute) proofBoundExecution {
	return proofBoundExecution{
		SandboxID: execution.SandboxID, NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
		RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
		LastEventSeq: execution.LastEventSeq,
	}
}
