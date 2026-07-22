package raftstore

import (
	"context"
	"errors"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func (r *Runtime) ApplySystem(ctx context.Context, command SystemCommand) (SystemApplyResult, error) {
	if command.Type == SystemBootstrap || command.Type == SystemRefreshPermit || command.Type == SystemConfirmDrain ||
		command.Type == SystemAdvanceTransition || command.Type == SystemActivateTransition ||
		command.Type == SystemConfirmTransitionDrain || command.Type == SystemFinalizeTransition ||
		command.Type == SystemCloseRegistryGeneration || command.Type == SystemSetGates ||
		command.Type == SystemBeginRecovery || command.Type == SystemConfirmRecoveryDrain ||
		command.Type == SystemAdvanceRecovery {
		return SystemApplyResult{}, errors.New("raftstore: System lifecycle command requires its dedicated workflow")
	}
	if command.Type == SystemBeginTransition {
		if command.Transition == nil || command.Transition.Version != r.registryLayout.RegistryLayoutVersion ||
			command.Transition.Digest != r.registryLayoutDigest {
			return SystemApplyResult{}, errors.New("raftstore: transition does not name the verified next registryLayout")
		}
		result, proposeErr := r.proposeSystem(ctx, command)
		if proposeErr == nil && result.Applied && !result.Conflict {
			return result, nil
		}
		resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
		defer cancelResolve()
		current, readErr := r.ReadSystemStrong(resolveContext)
		if readErr == nil && current.Transition != nil &&
			sameTransitionIdentity(current.Transition, command.Transition) {
			return SystemApplyResult{Applied: true}, nil
		}
		if proposeErr != nil {
			return SystemApplyResult{}, proposeErr
		}
		if result.Conflict || !result.Applied {
			return SystemApplyResult{}, errors.New(result.Reason)
		}
		if readErr != nil {
			return SystemApplyResult{}, readErr
		}
		return SystemApplyResult{}, errors.New("raftstore: committed registryLayout transition was not visible")
	}
	return r.proposeSystem(ctx, command)
}

func sameTransitionIdentity(current, wanted *RegistryLayoutTransition) bool {
	return current != nil && wanted != nil && current.Version == wanted.Version &&
		current.Digest == wanted.Digest && current.PreviousDigest == wanted.PreviousDigest &&
		current.NextSystemEpoch == wanted.NextSystemEpoch
}

// ConfigureServiceGates is the only public workflow for changing normal
// serving gates. It resolves ambiguous proposals by a quorum read and never
// exposes the raw SystemSetGates command through ApplySystem.
func (r *Runtime) ConfigureServiceGates(ctx context.Context, gates GateUpdate) (SystemState, error) {
	return r.SetServingGates(ctx, gates)
}

// CloseRegistryGeneration permanently retires this Registry History Generation and commits the
// exact successor registryLayout intent. The successor's consensus proof outputs are
// deliberately excluded from the intent because they are produced by this
// commit; ConsensusPredecessorProof fills them afterwards.
func (r *Runtime) CloseRegistryGeneration(ctx context.Context, successor RegistryLayout) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.CloseRegistryGeneration(operation, successor)
		if err == nil {
			err = r.acceptRemoteSystemState(state)
		}
		return state, err
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	predecessor := successor.Predecessor
	if successor.ClusterID != state.ClusterID || successor.RegistryGeneration == state.RegistryGeneration ||
		successor.RegistryLayoutVersion != 1 || predecessor == nil ||
		predecessor.Kind != RolloverConsensusClosure ||
		predecessor.RegistryGeneration != state.RegistryGeneration ||
		predecessor.RegistryLayoutDigest != state.ActiveRegistryLayoutDigest ||
		predecessor.ServePermitMaxMillis != state.ServePermitMaxMillis {
		return SystemState{}, errors.New("raftstore: successor Registry Layout is not linked to the active Registry History Generation")
	}
	intentDigest, err := successor.RolloverIntentDigest()
	if err != nil {
		return SystemState{}, err
	}
	if state.Retired {
		if state.Closure != nil && state.Closure.TargetRegistryGeneration == successor.RegistryGeneration &&
			state.Closure.TargetRegistryLayoutIntentDigest == intentDigest {
			return state, nil
		}
		return SystemState{}, errors.New("raftstore: Registry History Generation is retired for another successor Registry Layout")
	}

	result, proposeErr := r.proposeSystem(ctx, SystemCommand{
		Type: SystemCloseRegistryGeneration,
		Closure: &RegistryGenerationClosure{
			TargetRegistryGeneration:         successor.RegistryGeneration,
			TargetRegistryLayoutIntentDigest: intentDigest,
			Kind:                             RolloverConsensusClosure,
		},
	})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && current.Retired && current.Closure != nil &&
		current.Closure.TargetRegistryGeneration == successor.RegistryGeneration &&
		current.Closure.TargetRegistryLayoutIntentDigest == intentDigest {
		return current, nil
	}
	if proposeErr != nil {
		return SystemState{}, proposeErr
	}
	if result.Conflict || !result.Applied {
		return SystemState{}, errors.New(result.Reason)
	}
	if readErr != nil {
		return SystemState{}, readErr
	}
	return SystemState{}, errors.New("raftstore: committed Registry History Generation closure was not visible")
}

// ConfirmPredecessorPermitDrain waits a full predecessor permit lifetime from
// a quorum-confirmed successor-state read before recording drain completion.
// Restarting the caller restarts the full monotonic wait, which is conservative.
func (r *Runtime) ConfirmPredecessorPermitDrain(ctx context.Context, evidenceDigest string) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		timeout := time.Duration(MaximumServePermitMillis+r.config.Tuning.OperationTimeoutMillis) * time.Millisecond
		operation, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		state, err := r.systemClient.ConfirmPredecessorPermitDrain(operation, evidenceDigest)
		if err == nil {
			err = r.acceptRemoteSystemState(state)
		}
		return state, err
	}
	if !isSHA256(evidenceDigest) {
		return SystemState{}, errors.New("raftstore: predecessor drain evidence digest is required")
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	if !state.HasPredecessor {
		return SystemState{}, errors.New("raftstore: first Registry History Generation has no predecessor Permit to drain")
	}
	if state.PredecessorDrainComplete {
		return state, nil
	}
	if state.PredecessorProofKind != RolloverConsensusClosure {
		return SystemState{}, errors.New("raftstore: incomplete external fencing cannot be replaced by a timed drain")
	}
	wait := time.Duration(state.PredecessorPermitMaxMillis) * time.Millisecond
	started := time.Now()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return SystemState{}, ctx.Err()
	case <-timer.C:
	}
	for time.Since(started) < wait {
		remaining := wait - time.Since(started)
		timer.Reset(remaining)
		select {
		case <-ctx.Done():
			return SystemState{}, ctx.Err()
		case <-timer.C:
		}
	}

	current, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if current.RegistryGeneration != state.RegistryGeneration || current.SystemEpoch != state.SystemEpoch ||
		current.PredecessorRegistryGeneration != state.PredecessorRegistryGeneration ||
		current.PredecessorProofDigest != state.PredecessorProofDigest ||
		current.PredecessorPermitMaxMillis != state.PredecessorPermitMaxMillis {
		return SystemState{}, errors.New("raftstore: predecessor drain identity changed during the wait")
	}
	if current.PredecessorDrainComplete {
		return current, nil
	}
	elapsedMillis := uint64(time.Since(started) / time.Millisecond)
	result, err := r.proposeSystem(ctx, SystemCommand{Type: SystemConfirmDrain, Drain: &DrainConfirmation{
		PredecessorProofDigest: state.PredecessorProofDigest,
		WaitedMillis:           elapsedMillis, EvidenceDigest: evidenceDigest,
	}})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	confirmed, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && confirmed.PredecessorDrainComplete &&
		confirmed.PredecessorProofDigest == state.PredecessorProofDigest {
		return confirmed, nil
	}
	if err != nil {
		return SystemState{}, err
	}
	if result.Conflict || !result.Applied {
		return SystemState{}, errors.New(result.Reason)
	}
	if readErr != nil {
		return SystemState{}, readErr
	}
	return SystemState{}, errors.New("raftstore: predecessor drain was not committed")
}

func (r *Runtime) ReadData(ctx context.Context, query DataLookup) (DataLookupResult, error) {
	if err := r.removalFenceError(); err != nil {
		return DataLookupResult{}, err
	}
	if err := query.Validate(); err != nil {
		return DataLookupResult{}, err
	}
	identity, logicalShardID, strong, err := lookupIdentity(query)
	if err != nil {
		return DataLookupResult{}, err
	}
	if err := r.authorizeLocalDataReplica(identity); err != nil {
		return DataLookupResult{}, err
	}
	if err := r.permitCache.Authorize(identity.PermitIdentity, PermitRegistryRead); err != nil {
		return DataLookupResult{}, err
	}
	var value any
	if strong {
		value, err = r.syncRead(ctx, DataRaftShardID(logicalShardID), query)
	} else {
		value, err = r.nodeHost.StaleRead(DataRaftShardID(logicalShardID), query)
	}
	if err != nil {
		return DataLookupResult{}, err
	}
	result, ok := value.(DataLookupResult)
	if !ok {
		return DataLookupResult{}, errors.New("raftstore: Dragonboat returned an invalid data lookup result")
	}
	if !strong {
		r.attachLeaderHint(logicalShardID, &result)
	}
	return result, nil
}

func (r *Runtime) authorizeLocalDataReplica(identity ShardRequestIdentity) error {
	r.mu.Lock()
	position := r.replicaPosition(DataRaftShardID(identity.ShardID))
	if position < 0 {
		r.mu.Unlock()
		return ErrNoLocalReplica
	}
	local := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if identity.RegistryLayoutDigest != r.registryLayoutDigest {
		// A removed replica may serve only the preceding epoch while its
		// already-issued Permit drains. DataState.Accepts performs the exact
		// old-epoch check and the new registryLayout never routes new-epoch reads here.
		if (local.LocalState == ReplicaActive || local.LocalState == ReplicaRemoving) && !local.NonVoting {
			return nil
		}
		return ErrNoLocalReplica
	}
	if local.LocalState != ReplicaActive || local.NonVoting {
		return ErrNoLocalReplica
	}
	if int(identity.ShardID) >= len(r.registryLayout.DataShards) {
		return ErrNoLocalReplica
	}
	placement, found := replicaPlacementForMember(
		r.registryLayout.DataShards[identity.ShardID].Replicas, r.member.MemberID,
	)
	if !found {
		return ErrNoLocalReplica
	}
	if local.ReplicaID != placement.ReplicaID {
		return ErrNoLocalReplica
	}
	return nil
}

func lookupIdentity(query DataLookup) (ShardRequestIdentity, uint32, bool, error) {
	switch {
	case query.Route != nil:
		return shardIdentityFromRoute(query.Route.RequestIdentity), query.Route.ShardID, query.Route.Strong, nil
	case query.Build != nil:
		return shardIdentityFromRoute(query.Build.RequestIdentity), query.Build.ShardID, query.Build.Strong, nil
	case query.Workflow != nil:
		return query.Workflow.Identity, query.Workflow.Identity.ShardID, true, nil
	case query.RouteBucket != nil:
		return query.RouteBucket.Identity, query.RouteBucket.Identity.ShardID, query.RouteBucket.Strong, nil
	case query.Changefeed != nil:
		return query.Changefeed.Identity, query.Changefeed.Identity.ShardID, query.Changefeed.Strong, nil
	case query.Pending != nil:
		return query.Pending.Identity, query.Pending.Identity.ShardID, true, nil
	case query.LeaseBindings != nil:
		return query.LeaseBindings.Identity, query.LeaseBindings.Identity.ShardID, true, nil
	case query.Fence != nil:
		return query.Fence.Identity, query.Fence.Identity.ShardID, true, nil
	case query.Recovery != nil:
		return query.Recovery.Identity, query.Recovery.Identity.ShardID, true, nil
	default:
		return ShardRequestIdentity{}, 0, false, errors.New("raftstore: data lookup identity is unavailable")
	}
}

func (r *Runtime) attachLeaderHint(shardID uint32, result *DataLookupResult) {
	if result == nil {
		return
	}
	needsHint := result.Route != nil &&
		(result.Route.Outcome == routeapi.ReadNeedLeader || result.Route.Outcome == routeapi.ReadReplicaBehind) ||
		result.Build != nil &&
			(result.Build.Outcome == routeapi.ReadNeedLeader || result.Build.Outcome == routeapi.ReadReplicaBehind)
	if !needsHint {
		return
	}
	leaderID, term, valid, err := r.nodeHost.GetLeaderID(DataRaftShardID(shardID))
	if err != nil || !valid || term == 0 {
		return
	}
	placement := r.registryLayout.DataShards[shardID]
	for _, replica := range placement.Replicas {
		if replica.ReplicaID != leaderID {
			continue
		}
		member, found := registryLayoutMember(r.registryLayout, replica.MemberID)
		if !found {
			return
		}
		hint := &routeapi.LeaderHint{MemberID: member.MemberID, Endpoint: member.InternalEndpoint, Term: term}
		if result.Route != nil {
			result.Route.LeaderHint = hint
		} else {
			result.Build.LeaderHint = hint
		}
		return
	}
}
