package raftstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"
)

// ReconcileManifestTransitionShard advances one durable transition edge after
// proving the corresponding data or Raft membership state.
func (r *Runtime) ReconcileManifestTransitionShard(
	ctx context.Context,
	raftShardID uint64,
) (SystemState, error) {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()

	system, transition, progress, err := r.readManifestTransition(ctx, raftShardID)
	if err != nil {
		return SystemState{}, err
	}
	switch progress.Stage {
	case TransitionPending:
		if transition.Activated {
			return SystemState{}, errors.New("raftstore: pending shard remains after manifest activation")
		}
		if err := r.prepareDataEpoch(ctx, system, raftShardID); err != nil {
			return SystemState{}, err
		}
		if err := r.addDesiredNonVoters(ctx, raftShardID); err != nil {
			return SystemState{}, err
		}
		return r.commitTransitionAdvance(ctx, progress.ShardID, TransitionPending, TransitionCatchingUp)
	case TransitionCatchingUp:
		if transition.Activated {
			return SystemState{}, errors.New("raftstore: incomplete catch-up remains after manifest activation")
		}
		if err := r.promoteDesiredReplicas(ctx, raftShardID); err != nil {
			return SystemState{}, err
		}
		return r.commitTransitionAdvance(ctx, progress.ShardID, TransitionCatchingUp, TransitionPromoted)
	case TransitionPromoted:
		if transition.Activated {
			return SystemState{}, errors.New("raftstore: old membership remains after manifest activation")
		}
		if err := r.removeUndesiredReplicas(ctx, raftShardID); err != nil {
			return SystemState{}, err
		}
		return r.commitTransitionAdvance(ctx, progress.ShardID, TransitionPromoted, TransitionOldRemoved)
	case TransitionOldRemoved:
		if transition.Activated {
			return SystemState{}, errors.New("raftstore: unverified membership remains after manifest activation")
		}
		if err := r.verifyDesiredMembership(ctx, raftShardID); err != nil {
			return SystemState{}, err
		}
		if err := r.verifyPreparedDataEpoch(ctx, system, raftShardID); err != nil {
			return SystemState{}, err
		}
		return r.commitTransitionAdvance(ctx, progress.ShardID, TransitionOldRemoved, TransitionComplete)
	case TransitionComplete:
		if !transition.Activated {
			return system, nil
		}
		if !transition.PreviousPermitDrainComplete {
			return SystemState{}, errors.New("raftstore: previous manifest Serve Permits have not drained")
		}
		if err := r.retireDataEpoch(ctx, system, raftShardID); err != nil {
			return SystemState{}, err
		}
		if err := r.verifyDesiredMembership(ctx, raftShardID); err != nil {
			return SystemState{}, err
		}
		return r.commitTransitionAdvance(ctx, progress.ShardID, TransitionComplete, TransitionEpochRetired)
	case TransitionEpochRetired:
		return system, nil
	default:
		return SystemState{}, fmt.Errorf("raftstore: unknown manifest transition stage %q", progress.Stage)
	}
}

func (r *Runtime) ActivateManifestTransition(ctx context.Context) (SystemState, error) {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()

	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	transition, err := r.requireManifestTransition(state)
	if err != nil {
		return SystemState{}, err
	}
	if transition.Activated {
		return state, nil
	}
	if !allTransitionsComplete(transition.Shards) {
		return SystemState{}, errors.New("raftstore: manifest transition has incomplete shards")
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{Type: SystemActivateTransition})
	current, readErr := r.ReadSystemStrong(ctx)
	if readErr == nil {
		if current.Transition != nil && current.Transition.Activated &&
			current.ActiveManifestDigest == r.manifestDigest {
			if err := r.SyncLocalManifest(current); err != nil {
				return SystemState{}, err
			}
			return current, nil
		}
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
	return SystemState{}, errors.New("raftstore: manifest activation was not visible after commit")
}

// ConfirmManifestTransitionPermitDrain starts a fresh monotonic wait from a
// quorum-confirmed activation read. A restart conservatively restarts the wait.
func (r *Runtime) ConfirmManifestTransitionPermitDrain(ctx context.Context) (SystemState, error) {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()

	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	transition, err := r.requireManifestTransition(state)
	if err != nil {
		return SystemState{}, err
	}
	if !transition.Activated {
		return SystemState{}, errors.New("raftstore: manifest transition is not activated")
	}
	if transition.PreviousPermitDrainComplete {
		return state, nil
	}
	wait := time.Duration(state.ServePermitMaxMillis) * time.Millisecond
	started := time.Now()
	if err := waitMonotonic(ctx, started, wait); err != nil {
		return SystemState{}, err
	}

	current, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	currentTransition, err := r.requireManifestTransition(current)
	if err != nil {
		return SystemState{}, err
	}
	if currentTransition.PreviousPermitDrainComplete {
		return current, nil
	}
	if !currentTransition.Activated || currentTransition.ActivationIndex != transition.ActivationIndex ||
		currentTransition.PreviousDigest != transition.PreviousDigest ||
		currentTransition.NextSystemEpoch != transition.NextSystemEpoch {
		return SystemState{}, errors.New("raftstore: manifest activation identity changed during permit drain")
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{
		Type: SystemConfirmTransitionDrain,
		TransitionDrain: &TransitionDrainConfirmation{
			PreviousManifestDigest: transition.PreviousDigest,
			PreviousSystemEpoch:    transition.NextSystemEpoch - 1,
			ActivationIndex:        transition.ActivationIndex,
			WaitedMillis:           uint64(time.Since(started) / time.Millisecond),
		},
	})
	confirmed, readErr := r.ReadSystemStrong(ctx)
	if readErr == nil && confirmed.Transition != nil && confirmed.Transition.PreviousPermitDrainComplete {
		return confirmed, nil
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
	return SystemState{}, errors.New("raftstore: manifest permit drain was not visible after commit")
}

func (r *Runtime) FinalizeManifestTransition(ctx context.Context) (SystemState, error) {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()

	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	transition, err := r.requireManifestTransition(state)
	if err != nil {
		return SystemState{}, err
	}
	if !transition.Activated || !transition.PreviousPermitDrainComplete ||
		!allTransitionsAtStage(transition.Shards, TransitionEpochRetired) {
		return SystemState{}, errors.New("raftstore: manifest transition is not ready to finalize")
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{Type: SystemFinalizeTransition})
	current, readErr := r.ReadSystemStrong(ctx)
	if readErr == nil && current.Transition == nil && current.ActiveManifestDigest == r.manifestDigest {
		if err := r.SyncLocalManifest(current); err != nil {
			return SystemState{}, err
		}
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
	return SystemState{}, errors.New("raftstore: finalized manifest transition remains active")
}

func (r *Runtime) readManifestTransition(
	ctx context.Context,
	raftShardID uint64,
) (SystemState, *ManifestTransition, ShardTransition, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, nil, ShardTransition{}, err
	}
	transition, err := r.requireManifestTransition(state)
	if err != nil {
		return SystemState{}, nil, ShardTransition{}, err
	}
	logicalID, err := transitionProgressID(raftShardID)
	if err != nil {
		return SystemState{}, nil, ShardTransition{}, err
	}
	position := transitionPosition(transition.Shards, logicalID)
	if position < 0 {
		return SystemState{}, nil, ShardTransition{}, errors.New("raftstore: transition does not track the requested shard")
	}
	return state, transition, transition.Shards[position], nil
}

func (r *Runtime) requireManifestTransition(state SystemState) (*ManifestTransition, error) {
	if err := r.authorizeManifestState(state); err != nil {
		return nil, err
	}
	if state.Transition == nil || state.Transition.Digest != r.manifestDigest ||
		state.Transition.Version != r.manifest.ManifestVersion ||
		state.Transition.PreviousDigest != r.manifest.PreviousManifestDigest {
		return nil, errors.New("raftstore: no committed transition for the verified next manifest")
	}
	return state.Transition, nil
}

func (r *Runtime) commitTransitionAdvance(
	ctx context.Context,
	shardID uint32,
	from TransitionStage,
	to TransitionStage,
) (SystemState, error) {
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{
		Type:    SystemAdvanceTransition,
		Advance: &TransitionAdvance{ShardID: shardID, From: from, To: to},
	})
	current, readErr := r.ReadSystemStrong(ctx)
	if readErr == nil && current.Transition != nil {
		position := transitionPosition(current.Transition.Shards, shardID)
		if position >= 0 && current.Transition.Shards[position].Stage == to {
			return current, nil
		}
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
	return SystemState{}, errors.New("raftstore: transition progress was not visible after commit")
}

func (r *Runtime) prepareDataEpoch(ctx context.Context, system SystemState, raftShardID uint64) error {
	logicalID, isData := LogicalShardID(raftShardID)
	if !isData {
		if raftShardID == SystemRaftShardID {
			return nil
		}
		return errors.New("raftstore: unknown Raft shard")
	}
	state, err := r.readDataStateStrong(ctx, raftShardID)
	if err != nil {
		return err
	}
	previous, next := transitionEpochs(system)
	desired := replicaIDsForPlacement(r.manifest.DataShards[logicalID])
	if len(state.ServingEpochs) == 1 {
		if state.ServingEpochs[0] != previous {
			return errors.New("raftstore: data shard does not serve the active pre-transition epoch")
		}
		if err := r.permitCache.Authorize(previous, PermitRegistryWrite); err != nil {
			return err
		}
		result, proposeErr := r.proposeDataRaw(ctx, DataCommand{
			Type:     DataPrepareEpoch,
			Identity: ShardRequestIdentity{PermitIdentity: previous, ShardID: logicalID},
			Epoch:    &next, ReplicaIDs: desired,
		})
		if proposeErr == nil && result.Conflict {
			proposeErr = errors.New(result.Reason)
		}
		state, err = r.readDataStateStrong(ctx, raftShardID)
		if err != nil {
			if proposeErr != nil {
				return proposeErr
			}
			return err
		}
		if validatePreparedEpoch(state, previous, next, desired) == nil {
			return nil
		}
		if proposeErr != nil {
			return proposeErr
		}
		return errors.New("raftstore: prepared data epoch was not visible after commit")
	}
	return validatePreparedEpoch(state, previous, next, desired)
}

func (r *Runtime) verifyPreparedDataEpoch(ctx context.Context, system SystemState, raftShardID uint64) error {
	logicalID, isData := LogicalShardID(raftShardID)
	if !isData {
		if raftShardID == SystemRaftShardID {
			return nil
		}
		return errors.New("raftstore: unknown Raft shard")
	}
	state, err := r.readDataStateStrong(ctx, raftShardID)
	if err != nil {
		return err
	}
	previous, next := transitionEpochs(system)
	return validatePreparedEpoch(state, previous, next, replicaIDsForPlacement(r.manifest.DataShards[logicalID]))
}

func (r *Runtime) retireDataEpoch(ctx context.Context, system SystemState, raftShardID uint64) error {
	logicalID, isData := LogicalShardID(raftShardID)
	if !isData {
		if raftShardID == SystemRaftShardID {
			return nil
		}
		return errors.New("raftstore: unknown Raft shard")
	}
	state, err := r.readDataStateStrong(ctx, raftShardID)
	if err != nil {
		return err
	}
	previous, next := transitionEpochs(system)
	desired := replicaIDsForPlacement(r.manifest.DataShards[logicalID])
	if len(state.ServingEpochs) == 2 {
		if err := validatePreparedEpoch(state, previous, next, desired); err != nil {
			return err
		}
		if err := r.permitCache.Authorize(next, PermitRegistryWrite); err != nil {
			return err
		}
		result, proposeErr := r.proposeDataRaw(ctx, DataCommand{
			Type:     DataRetireEpoch,
			Identity: ShardRequestIdentity{PermitIdentity: next, ShardID: logicalID},
			Epoch:    &previous,
		})
		if proposeErr == nil && result.Conflict {
			proposeErr = errors.New(result.Reason)
		}
		state, err = r.readDataStateStrong(ctx, raftShardID)
		if err != nil {
			if proposeErr != nil {
				return proposeErr
			}
			return err
		}
		if validateRetiredEpoch(state, next, desired) == nil {
			return nil
		}
		if proposeErr != nil {
			return proposeErr
		}
		return errors.New("raftstore: retired data epoch was not visible after commit")
	}
	return validateRetiredEpoch(state, next, desired)
}

func (r *Runtime) readDataStateStrong(ctx context.Context, raftShardID uint64) (DataState, error) {
	value, err := r.nodeHost.SyncRead(ctx, raftShardID, DataStateLookup{})
	if err != nil {
		return DataState{}, err
	}
	state, ok := value.(DataState)
	if !ok {
		return DataState{}, errors.New("raftstore: Dragonboat returned an invalid data state type")
	}
	if err := state.Validate(); err != nil {
		return DataState{}, err
	}
	return state, nil
}

func (r *Runtime) addDesiredNonVoters(ctx context.Context, raftShardID uint64) error {
	placements, err := r.placementForRaftShard(raftShardID)
	if err != nil {
		return err
	}
	for _, replica := range placements {
		membership, err := r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
		if err != nil {
			return err
		}
		target, err := r.desiredReplicaTarget(raftShardID, replica.ReplicaID, replica.MemberID)
		if err != nil {
			return err
		}
		if current, found := membership.Nodes[replica.ReplicaID]; found {
			if current != target {
				return errors.New("raftstore: desired voting replica has the wrong target")
			}
			continue
		}
		if current, found := membership.NonVotings[replica.ReplicaID]; found {
			if current != target {
				return errors.New("raftstore: desired non-voting replica has the wrong target")
			}
			continue
		}
		if _, found := membership.Witnesses[replica.ReplicaID]; found {
			return errors.New("raftstore: desired full replica is currently a witness")
		}
		if err := r.AddNonVoting(ctx, raftShardID, replica.ReplicaID, replica.MemberID); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) promoteDesiredReplicas(
	ctx context.Context,
	raftShardID uint64,
) error {
	minimumApplied, err := r.readShardAppliedBarrier(ctx, raftShardID)
	if err != nil {
		return err
	}
	placements, err := r.placementForRaftShard(raftShardID)
	if err != nil {
		return err
	}
	for _, replica := range placements {
		membership, err := r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
		if err != nil {
			return err
		}
		promotion := ReplicaPromotionRequest{
			ShardID: raftShardID, ReplicaID: replica.ReplicaID, MemberID: replica.MemberID,
			ManifestDigest: r.manifestDigest,
		}
		if _, voting := membership.Nodes[replica.ReplicaID]; voting {
			if err := r.confirmReplicaPromoted(ctx, promotion); err != nil {
				return err
			}
			continue
		}
		if _, joining := membership.NonVotings[replica.ReplicaID]; !joining {
			return errors.New("raftstore: desired replica is neither voting nor catching up")
		}
		request := ReplicaCatchUpRequest{
			ShardID: raftShardID, ReplicaID: replica.ReplicaID, MemberID: replica.MemberID,
			ManifestDigest: r.manifestDigest, MinimumAppliedIndex: minimumApplied,
		}
		proof, err := r.probeReplicaCatchUp(ctx, request)
		if err != nil {
			return err
		}
		if err := r.promoteNonVoting(ctx, request, proof); err != nil {
			return err
		}
		if err := r.confirmReplicaPromoted(ctx, promotion); err != nil {
			return err
		}
	}
	return r.verifyDesiredVoters(ctx, raftShardID)
}

func (r *Runtime) readShardAppliedBarrier(ctx context.Context, raftShardID uint64) (uint64, error) {
	if raftShardID == SystemRaftShardID {
		state, err := r.ReadSystemStrong(ctx)
		if err != nil {
			return 0, err
		}
		if _, err := r.requireManifestTransition(state); err != nil {
			return 0, err
		}
		return state.LastApplied, nil
	}
	logicalID, ok := LogicalShardID(raftShardID)
	if !ok {
		return 0, errors.New("raftstore: unknown Raft shard")
	}
	state, err := r.readDataStateStrong(ctx, raftShardID)
	if err != nil {
		return 0, err
	}
	if err := r.validateDataState(state, logicalID); err != nil {
		return 0, err
	}
	for _, epoch := range state.ServingEpochs {
		if epoch.ManifestDigest == r.manifestDigest {
			return state.LastApplied, nil
		}
	}
	return 0, errors.New("raftstore: data shard has not committed the target manifest epoch")
}

func (r *Runtime) removeUndesiredReplicas(ctx context.Context, raftShardID uint64) error {
	placements, err := r.placementForRaftShard(raftShardID)
	if err != nil {
		return err
	}
	desired := make(map[uint64]struct{}, len(placements))
	for _, replica := range placements {
		desired[replica.ReplicaID] = struct{}{}
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
	if err != nil {
		return err
	}
	joining := make([]uint64, 0)
	for replicaID := range membership.NonVotings {
		if _, keep := desired[replicaID]; !keep {
			joining = append(joining, replicaID)
		}
	}
	sort.Slice(joining, func(i, j int) bool { return joining[i] < joining[j] })
	for _, replicaID := range joining {
		if err := r.RemoveJoiningReplica(ctx, raftShardID, replicaID); err != nil {
			return err
		}
	}
	membership, err = r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
	if err != nil {
		return err
	}
	old := make([]uint64, 0)
	for replicaID := range membership.Nodes {
		if _, keep := desired[replicaID]; !keep {
			old = append(old, replicaID)
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i] < old[j] })
	for _, replicaID := range old {
		if err := r.RemoveOldReplica(ctx, raftShardID, replicaID); err != nil {
			return err
		}
	}
	return r.verifyDesiredMembership(ctx, raftShardID)
}

func (r *Runtime) verifyDesiredVoters(ctx context.Context, raftShardID uint64) error {
	placements, err := r.placementForRaftShard(raftShardID)
	if err != nil {
		return err
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
	if err != nil {
		return err
	}
	for _, replica := range placements {
		target, err := r.desiredReplicaTarget(raftShardID, replica.ReplicaID, replica.MemberID)
		if err != nil {
			return err
		}
		if membership.Nodes[replica.ReplicaID] != target {
			return errors.New("raftstore: desired voting replica is not committed at its signed target")
		}
	}
	return nil
}

func (r *Runtime) verifyDesiredMembership(ctx context.Context, raftShardID uint64) error {
	placements, err := r.placementForRaftShard(raftShardID)
	if err != nil {
		return err
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, raftShardID)
	if err != nil {
		return err
	}
	if len(membership.Nodes) != len(placements) || len(membership.NonVotings) != 0 || len(membership.Witnesses) != 0 {
		return errors.New("raftstore: committed membership is not the exact signed replica set")
	}
	return r.verifyDesiredVoters(ctx, raftShardID)
}

func transitionProgressID(raftShardID uint64) (uint32, error) {
	if raftShardID == SystemRaftShardID {
		return ^uint32(0), nil
	}
	logicalID, ok := LogicalShardID(raftShardID)
	if !ok {
		return 0, errors.New("raftstore: unknown Raft shard")
	}
	return logicalID, nil
}

func transitionEpochs(system SystemState) (PermitIdentity, PermitIdentity) {
	transition := system.Transition
	previous := PermitIdentity{
		ClusterID: system.ClusterID, StorageGeneration: system.StorageGeneration,
		SystemEpoch: transition.NextSystemEpoch - 1, ManifestDigest: transition.PreviousDigest,
	}
	next := PermitIdentity{
		ClusterID: system.ClusterID, StorageGeneration: system.StorageGeneration,
		SystemEpoch: transition.NextSystemEpoch, ManifestDigest: transition.Digest,
	}
	return previous, next
}

func validatePreparedEpoch(state DataState, previous, next PermitIdentity, desired []uint64) error {
	if len(state.ServingEpochs) != 2 || state.ServingEpochs[0] != previous || state.ServingEpochs[1] != next ||
		!slices.Equal(state.PreparedReplicaIDs, desired) {
		return errors.New("raftstore: data shard does not contain the exact prepared epoch and replica set")
	}
	return nil
}

func validateRetiredEpoch(state DataState, next PermitIdentity, desired []uint64) error {
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0] != next ||
		len(state.PreparedReplicaIDs) != 0 || !slices.Equal(state.ReplicaIDs, desired) {
		return errors.New("raftstore: data shard did not retire the previous serving epoch")
	}
	return nil
}

func waitMonotonic(ctx context.Context, started time.Time, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		remaining := wait - time.Since(started)
		if remaining <= 0 {
			return nil
		}
		timer.Reset(remaining)
	}
}
