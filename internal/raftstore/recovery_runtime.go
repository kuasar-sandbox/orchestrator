package raftstore

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"
)

func (r *Runtime) BeginRecovery(ctx context.Context, recovery RecoveryEpoch) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if state.Recovery != nil {
		if *state.Recovery == recovery {
			return state, nil
		}
		return SystemState{}, errors.New("raftstore: another recovery epoch is active")
	}
	return r.commitRecoveryCommand(ctx, SystemCommand{Type: SystemBeginRecovery, Recovery: &recovery})
}

// PrepareRecoveryDataShards commits the recovery SystemEpoch to every data
// shard before recovery writes are allowed to proceed.
func (r *Runtime) PrepareRecoveryDataShards(ctx context.Context, workers int) error {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	if state.Recovery == nil || state.Recovery.Phase != RecoveryPreparing {
		return errors.New("raftstore: recovery data epochs prepare only in PREPARING")
	}
	return r.forEachDataShard(ctx, workers, func(ctx context.Context, shardID uint32) error {
		return r.prepareRecoveryDataShard(ctx, state, shardID)
	})
}

// FinalizeRecoveryDataShards permanently retires the pre-recovery SystemEpoch.
// The System Group cannot close recovery until every shard proves this state.
func (r *Runtime) FinalizeRecoveryDataShards(ctx context.Context, workers int) error {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	if state.Recovery == nil || state.Recovery.Phase != RecoveryFinalizing {
		return errors.New("raftstore: recovery data epochs retire only in FINALIZING")
	}
	return r.forEachDataShard(ctx, workers, func(ctx context.Context, shardID uint32) error {
		return r.retireRecoveryDataShard(ctx, state, shardID)
	})
}

func (r *Runtime) AdvanceRecovery(ctx context.Context, to RecoveryPhase) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if state.Recovery == nil {
		return SystemState{}, errors.New("raftstore: no recovery epoch is active")
	}
	if to == RecoveryClosed {
		if state.Recovery.Phase != RecoveryFinalizing {
			return SystemState{}, errors.New("raftstore: recovery closes only from FINALIZING")
		}
		if err := r.verifyRecoveryDataShardsClosed(ctx, state); err != nil {
			return SystemState{}, err
		}
	}
	return r.commitRecoveryCommand(ctx, SystemCommand{
		Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{From: state.Recovery.Phase, To: to},
	})
}

func (r *Runtime) commitRecoveryCommand(ctx context.Context, command SystemCommand) (SystemState, error) {
	result, proposeErr := r.proposeSystem(ctx, command)
	current, readErr := r.ReadSystemStrong(ctx)
	if readErr == nil {
		if command.Type == SystemBeginRecovery && current.Recovery != nil && command.Recovery != nil &&
			*current.Recovery == *command.Recovery {
			return current, nil
		}
		if command.Type == SystemAdvanceRecovery && command.RecoveryAdvance != nil &&
			(command.RecoveryAdvance.To == RecoveryClosed && current.Recovery == nil ||
				current.Recovery != nil && current.Recovery.Phase == command.RecoveryAdvance.To) {
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
	return SystemState{}, errors.New("raftstore: committed recovery transition was not visible")
}

func (r *Runtime) prepareRecoveryDataShard(ctx context.Context, system SystemState, shardID uint32) error {
	state, err := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if err != nil {
		return err
	}
	next := system.Identity()
	desired := replicaIDsForPlacement(r.registryLayout.DataShards[shardID])
	if len(state.ServingEpochs) == 2 {
		return validateRecoveryPrepared(state, next, desired)
	}
	if len(state.ServingEpochs) != 1 {
		return errors.New("raftstore: recovery shard has an invalid serving-epoch set")
	}
	previous := state.ServingEpochs[0]
	if previous.ClusterID != next.ClusterID || previous.RegistryGeneration != next.RegistryGeneration ||
		previous.RegistryLayoutDigest != next.RegistryLayoutDigest || previous.SystemEpoch+1 != next.SystemEpoch {
		return errors.New("raftstore: recovery shard does not serve the immediately preceding SystemEpoch")
	}
	result, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataPrepareEpoch, Identity: ShardRequestIdentity{PermitIdentity: previous, ShardID: shardID},
		Epoch: &next, ReplicaIDs: desired,
	})
	if proposeErr == nil && result.Conflict {
		proposeErr = errors.New(result.Reason)
	}
	current, readErr := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if readErr == nil && validateRecoveryPrepared(current, next, desired) == nil {
		return nil
	}
	if proposeErr != nil {
		return proposeErr
	}
	if readErr != nil {
		return readErr
	}
	return errors.New("raftstore: prepared recovery data epoch was not visible")
}

func (r *Runtime) retireRecoveryDataShard(ctx context.Context, system SystemState, shardID uint32) error {
	state, err := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if err != nil {
		return err
	}
	next := system.Identity()
	desired := replicaIDsForPlacement(r.registryLayout.DataShards[shardID])
	if recoveryDataShardClosed(state, next, desired) {
		return nil
	}
	if err := validateRecoveryPrepared(state, next, desired); err != nil {
		return err
	}
	previous := state.ServingEpochs[0]
	result, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataRetireEpoch, Identity: ShardRequestIdentity{PermitIdentity: next, ShardID: shardID}, Epoch: &previous,
	})
	if proposeErr == nil && result.Conflict {
		proposeErr = errors.New(result.Reason)
	}
	current, readErr := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if readErr == nil && recoveryDataShardClosed(current, next, desired) {
		return nil
	}
	if proposeErr != nil {
		return proposeErr
	}
	if readErr != nil {
		return readErr
	}
	return errors.New("raftstore: retired recovery data epoch was not visible")
}

func (r *Runtime) verifyRecoveryDataShardsClosed(ctx context.Context, system SystemState) error {
	for shardID := uint32(0); shardID < r.registryLayout.VirtualShardCount; shardID++ {
		state, err := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
		if err != nil {
			return err
		}
		if !recoveryDataShardClosed(state, system.Identity(), replicaIDsForPlacement(r.registryLayout.DataShards[shardID])) {
			return fmt.Errorf("raftstore: data shard %d has not retired its pre-recovery epoch", shardID)
		}
	}
	return nil
}

func validateRecoveryPrepared(state DataState, next PermitIdentity, replicas []uint64) error {
	if len(state.ServingEpochs) != 2 || state.ServingEpochs[1] != next ||
		!slices.Equal(state.PreparedReplicaIDs, replicas) {
		return errors.New("raftstore: recovery data epoch is not prepared")
	}
	return nil
}

func recoveryDataShardClosed(state DataState, next PermitIdentity, replicas []uint64) bool {
	return len(state.ServingEpochs) == 1 && state.ServingEpochs[0] == next &&
		len(state.PreparedReplicaIDs) == 0 && slices.Equal(state.ReplicaIDs, replicas)
}

func (r *Runtime) forEachDataShard(
	ctx context.Context,
	workers int,
	work func(context.Context, uint32) error,
) error {
	if workers <= 0 || workers > 256 {
		return errors.New("raftstore: recovery shard concurrency must be between 1 and 256")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for shardID := uint32(0); shardID < r.registryLayout.VirtualShardCount; shardID++ {
		shardID := shardID
		group.Go(func() error { return work(groupCtx, shardID) })
	}
	return group.Wait()
}
