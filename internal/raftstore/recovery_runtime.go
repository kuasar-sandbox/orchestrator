package raftstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

const recoveryVerificationWorkers = 32

type RecoveryShardAction string

const (
	RecoveryShardPrepare         RecoveryShardAction = "PREPARE"
	RecoveryShardVerifyPrepared  RecoveryShardAction = "VERIFY_PREPARED"
	RecoveryShardFinalize        RecoveryShardAction = "FINALIZE"
	RecoveryShardVerifyFinalized RecoveryShardAction = "VERIFY_FINALIZED"
)

// RecoveryShardAuthorization is committed System state narrowed to the fields
// needed by an authenticated Registry-to-Registry recovery request.
type RecoveryShardAuthorization struct {
	Recovery                    RecoveryEpoch `json:"recovery"`
	ActiveRegistryLayoutVersion uint64        `json:"active_registry_layout_version"`
	SystemCommitIndex           uint64        `json:"system_commit_index"`
}

func (a RecoveryShardAuthorization) Validate(registryLayout RegistryLayout, digest string) error {
	if err := a.Recovery.Validate(); err != nil {
		return err
	}
	if !a.Recovery.PermitDrainComplete || a.Recovery.PermitDrainIndex == 0 ||
		a.SystemCommitIndex < a.Recovery.PermitDrainIndex ||
		a.ActiveRegistryLayoutVersion != registryLayout.RegistryLayoutVersion ||
		a.Recovery.SourceClusterID != registryLayout.ClusterID ||
		a.Recovery.TargetRegistryGeneration != registryLayout.RegistryGeneration ||
		a.Recovery.TargetRegistryLayoutDigest != digest {
		return errors.New("raftstore: recovery shard authorization does not match the active Registry Layout")
	}
	return nil
}

type RecoveryShardRequest struct {
	MemberID      string                     `json:"member_id"`
	ShardID       uint32                     `json:"shard_id"`
	Action        RecoveryShardAction        `json:"action"`
	Authorization RecoveryShardAuthorization `json:"authorization"`
}

func (r RecoveryShardRequest) Validate(registryLayout RegistryLayout, digest string) error {
	if r.MemberID == "" || r.ShardID >= registryLayout.VirtualShardCount ||
		int(r.ShardID) >= len(registryLayout.DataShards) {
		return errors.New("raftstore: incomplete recovery shard request")
	}
	if err := r.Authorization.Validate(registryLayout, digest); err != nil {
		return err
	}
	if _, found := replicaPlacementForMember(registryLayout.DataShards[r.ShardID].Replicas, r.MemberID); !found {
		return errors.New("raftstore: recovery shard request targets a member outside the committed placement")
	}
	switch r.Action {
	case RecoveryShardPrepare, RecoveryShardVerifyPrepared:
		if r.Authorization.Recovery.Phase != RecoveryPreparing {
			return errors.New("raftstore: recovery shard preparation requires PREPARING")
		}
	case RecoveryShardFinalize, RecoveryShardVerifyFinalized:
		if r.Authorization.Recovery.Phase != RecoveryFinalizing {
			return errors.New("raftstore: recovery shard finalization requires FINALIZING")
		}
	default:
		return errors.New("raftstore: unknown recovery shard action")
	}
	return nil
}

type RecoveryShardProof struct {
	MemberID             string              `json:"member_id"`
	ShardID              uint32              `json:"shard_id"`
	Action               RecoveryShardAction `json:"action"`
	RegistryLayoutDigest string              `json:"registry_layout_digest"`
	RecoveryEpoch        uint64              `json:"recovery_epoch"`
	AppliedIndex         uint64              `json:"applied_index"`
}

func (p RecoveryShardProof) Validate(request RecoveryShardRequest, digest string) error {
	if p.MemberID != request.MemberID || p.ShardID != request.ShardID || p.Action != request.Action ||
		p.RegistryLayoutDigest != digest || p.RecoveryEpoch != request.Authorization.Recovery.Epoch ||
		p.AppliedIndex == 0 {
		return errors.New("raftstore: recovery shard proof does not match its request")
	}
	return nil
}

// RecoveryShardClient routes to the exact MemberID over the authenticated
// internal Registry transport. The target invokes ReconcileLocalRecoveryShard.
type RecoveryShardClient interface {
	ReconcileRecoveryShard(context.Context, RecoveryShardRequest) (RecoveryShardProof, error)
}

type RecoveryShardClientFunc func(context.Context, RecoveryShardRequest) (RecoveryShardProof, error)

func (f RecoveryShardClientFunc) ReconcileRecoveryShard(
	ctx context.Context,
	request RecoveryShardRequest,
) (RecoveryShardProof, error) {
	return f(ctx, request)
}

func (r *Runtime) BeginRecovery(ctx context.Context, recovery RecoveryEpoch) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.requireExactActiveRegistryLayout(state); err != nil {
		return SystemState{}, err
	}
	if state.Recovery != nil {
		if sameRecoveryIdentity(*state.Recovery, recovery) {
			return state, nil
		}
		return SystemState{}, errors.New("raftstore: another recovery epoch is active")
	}
	if recovery.Phase != RecoveryPreparing || recovery.PermitDrainComplete || recovery.PermitDrainIndex != 0 {
		return SystemState{}, errors.New("raftstore: recovery must begin in undrained PREPARING")
	}
	return r.commitRecoveryCommand(ctx, SystemCommand{Type: SystemBeginRecovery, Recovery: &recovery})
}

// ConfirmRecoveryPermitDrain waits a full permit lifetime after observing the
// closed recovery gates. Restarting the caller restarts the monotonic wait.
func (r *Runtime) ConfirmRecoveryPermitDrain(ctx context.Context) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.requireExactActiveRegistryLayout(state); err != nil {
		return SystemState{}, err
	}
	if state.Recovery == nil || state.Recovery.Phase != RecoveryPreparing {
		return SystemState{}, errors.New("raftstore: recovery permit drain requires PREPARING")
	}
	if state.Recovery.PermitDrainComplete {
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
	if err := r.requireExactActiveRegistryLayout(current); err != nil {
		return SystemState{}, err
	}
	if current.Recovery == nil || !sameRecoveryIdentity(*current.Recovery, *state.Recovery) ||
		current.Recovery.Phase != RecoveryPreparing {
		return SystemState{}, errors.New("raftstore: recovery identity changed during permit drain")
	}
	if current.Recovery.PermitDrainComplete {
		return current, nil
	}
	return r.commitRecoveryCommand(ctx, SystemCommand{
		Type: SystemConfirmRecoveryDrain,
		RecoveryDrain: &RecoveryDrainConfirmation{
			RecoveryEpoch:              current.Recovery.Epoch,
			TargetRegistryGeneration:   current.Recovery.TargetRegistryGeneration,
			TargetRegistryLayoutDigest: current.Recovery.TargetRegistryLayoutDigest,
			WaitedMillis:               uint64(time.Since(started) / time.Millisecond),
		},
	})
}

// PrepareRecoveryDataShards commits the recovery SystemEpoch to every data
// shard through one of that shard's exact Registry Layout replicas.
func (r *Runtime) PrepareRecoveryDataShards(ctx context.Context, workers int) error {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	authorization, err := r.recoveryShardAuthorization(state, RecoveryPreparing)
	if err != nil {
		return err
	}
	return r.forEachDataShard(ctx, workers, func(ctx context.Context, shardID uint32) error {
		_, err := r.routeRecoveryShard(ctx, authorization, shardID, RecoveryShardPrepare)
		return err
	})
}

// FinalizeRecoveryDataShards permanently retires the pre-recovery SystemEpoch.
func (r *Runtime) FinalizeRecoveryDataShards(ctx context.Context, workers int) error {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return err
	}
	authorization, err := r.recoveryShardAuthorization(state, RecoveryFinalizing)
	if err != nil {
		return err
	}
	return r.forEachDataShard(ctx, workers, func(ctx context.Context, shardID uint32) error {
		_, err := r.routeRecoveryShard(ctx, authorization, shardID, RecoveryShardFinalize)
		return err
	})
}

func (r *Runtime) AdvanceRecovery(ctx context.Context, to RecoveryPhase) (SystemState, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if _, err := r.recoveryShardAuthorization(state, stateRecoveryPhase(state)); err != nil {
		return SystemState{}, err
	}
	if to == RecoveryReconciling || to == RecoveryFinalizing {
		return SystemState{}, errors.New("raftstore: recovery cannot advance without durable node reconstruction proof")
	}
	if to == RecoveryCollecting {
		if state.Recovery.Phase != RecoveryPreparing {
			return SystemState{}, errors.New("raftstore: recovery enters COLLECTING only from PREPARING")
		}
		if err := r.verifyRecoveryDataShards(ctx, state, RecoveryShardVerifyPrepared); err != nil {
			return SystemState{}, err
		}
	}
	if to == RecoveryClosed {
		if state.Recovery.Phase != RecoveryFinalizing {
			return SystemState{}, errors.New("raftstore: recovery closes only from FINALIZING")
		}
		if err := r.verifyRecoveryDataShards(ctx, state, RecoveryShardVerifyFinalized); err != nil {
			return SystemState{}, err
		}
	}
	return r.commitRecoveryCommand(ctx, SystemCommand{
		Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{From: state.Recovery.Phase, To: to},
	})
}

func (r *Runtime) commitRecoveryCommand(ctx context.Context, command SystemCommand) (SystemState, error) {
	result, proposeErr := r.proposeSystem(ctx, command)
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil {
		switch command.Type {
		case SystemBeginRecovery:
			if current.Recovery != nil && command.Recovery != nil &&
				sameRecoveryIdentity(*current.Recovery, *command.Recovery) {
				return current, nil
			}
		case SystemConfirmRecoveryDrain:
			if current.Recovery != nil && command.RecoveryDrain != nil &&
				current.Recovery.Epoch == command.RecoveryDrain.RecoveryEpoch &&
				current.Recovery.PermitDrainComplete {
				return current, nil
			}
		case SystemAdvanceRecovery:
			if command.RecoveryAdvance != nil &&
				(command.RecoveryAdvance.To == RecoveryClosed && current.Recovery == nil ||
					current.Recovery != nil && current.Recovery.Phase == command.RecoveryAdvance.To) {
				return current, nil
			}
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

func (r *Runtime) requireExactActiveRegistryLayout(state SystemState) error {
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return err
	}
	if state.Retired || state.Transition != nil ||
		state.ActiveRegistryLayoutVersion != r.registryLayout.RegistryLayoutVersion ||
		state.ActiveRegistryLayoutDigest != r.registryLayoutDigest {
		return errors.New("raftstore: recovery requires the exact stable active Registry Layout")
	}
	return nil
}

func (r *Runtime) recoveryShardAuthorization(
	state SystemState,
	phase RecoveryPhase,
) (RecoveryShardAuthorization, error) {
	if err := r.requireExactActiveRegistryLayout(state); err != nil {
		return RecoveryShardAuthorization{}, err
	}
	if state.Recovery == nil || state.Recovery.Phase != phase || !state.Recovery.PermitDrainComplete {
		return RecoveryShardAuthorization{}, errors.New("raftstore: recovery phase or permit drain is not ready")
	}
	authorization := RecoveryShardAuthorization{
		Recovery: *state.Recovery, ActiveRegistryLayoutVersion: state.ActiveRegistryLayoutVersion,
		SystemCommitIndex: state.LastApplied,
	}
	if err := authorization.Validate(r.registryLayout, r.registryLayoutDigest); err != nil {
		return RecoveryShardAuthorization{}, err
	}
	return authorization, nil
}

func stateRecoveryPhase(state SystemState) RecoveryPhase {
	if state.Recovery == nil {
		return RecoveryClosed
	}
	return state.Recovery.Phase
}

func (r *Runtime) verifyRecoveryDataShards(
	ctx context.Context,
	state SystemState,
	action RecoveryShardAction,
) error {
	authorization, err := r.recoveryShardAuthorization(state, state.Recovery.Phase)
	if err != nil {
		return err
	}
	return r.forEachDataShard(ctx, recoveryVerificationWorkers, func(ctx context.Context, shardID uint32) error {
		_, err := r.routeRecoveryShard(ctx, authorization, shardID, action)
		return err
	})
}

func (r *Runtime) routeRecoveryShard(
	ctx context.Context,
	authorization RecoveryShardAuthorization,
	shardID uint32,
	action RecoveryShardAction,
) (RecoveryShardProof, error) {
	if shardID >= uint32(len(r.registryLayout.DataShards)) {
		return RecoveryShardProof{}, errors.New("raftstore: recovery targets an unknown data shard")
	}
	placements := r.registryLayout.DataShards[shardID].Replicas
	errorsByMember := make([]error, 0, len(placements))
	for _, placement := range placements {
		request := RecoveryShardRequest{
			MemberID: placement.MemberID, ShardID: shardID, Action: action, Authorization: authorization,
		}
		var (
			proof RecoveryShardProof
			err   error
		)
		if placement.MemberID == r.member.MemberID {
			proof, err = r.ReconcileLocalRecoveryShard(ctx, request)
		} else if r.recoveryClient == nil {
			err = errors.New("authenticated remote recovery shard client is unavailable")
		} else {
			proof, err = r.recoveryClient.ReconcileRecoveryShard(ctx, request)
		}
		if err == nil {
			err = proof.Validate(request, r.registryLayoutDigest)
		}
		if err == nil {
			return proof, nil
		}
		errorsByMember = append(errorsByMember, fmt.Errorf("member %s: %w", placement.MemberID, err))
	}
	return RecoveryShardProof{}, fmt.Errorf("raftstore: no placed replica completed recovery shard %d: %w", shardID, errors.Join(errorsByMember...))
}

// ReconcileLocalRecoveryShard is the target-side handler for authenticated
// Registry-to-Registry recovery routing.
func (r *Runtime) ReconcileLocalRecoveryShard(
	ctx context.Context,
	request RecoveryShardRequest,
) (RecoveryShardProof, error) {
	if err := request.Validate(r.registryLayout, r.registryLayoutDigest); err != nil {
		return RecoveryShardProof{}, err
	}
	if request.MemberID != r.member.MemberID {
		return RecoveryShardProof{}, errors.New("raftstore: recovery request targets another Registry member")
	}
	if err := r.authorizeLiveRecoveryShardRequest(ctx, request.Authorization); err != nil {
		return RecoveryShardProof{}, err
	}
	target := recoveryTargetIdentity(request.Authorization.Recovery)
	if err := r.authorizeLocalDataReplica(ShardRequestIdentity{PermitIdentity: target, ShardID: request.ShardID}); err != nil {
		return RecoveryShardProof{}, err
	}
	var (
		state DataState
		err   error
	)
	switch request.Action {
	case RecoveryShardPrepare:
		state, err = r.prepareRecoveryDataShardLocal(ctx, request.Authorization, request.ShardID)
	case RecoveryShardVerifyPrepared:
		state, err = r.readDataStateStrong(ctx, DataRaftShardID(request.ShardID))
		if err == nil {
			err = validateRecoveryPrepared(state, target, r.recoveryReplicaIDs(request.ShardID))
		}
	case RecoveryShardFinalize:
		state, err = r.finalizeRecoveryDataShardLocal(ctx, request.Authorization, request.ShardID)
	case RecoveryShardVerifyFinalized:
		state, err = r.readDataStateStrong(ctx, DataRaftShardID(request.ShardID))
		if err == nil && !recoveryDataShardClosed(state, target, r.recoveryReplicaIDs(request.ShardID)) {
			err = errors.New("raftstore: recovery data shard has not retired its old epoch")
		}
	}
	if err != nil {
		return RecoveryShardProof{}, err
	}
	return RecoveryShardProof{
		MemberID: request.MemberID, ShardID: request.ShardID, Action: request.Action,
		RegistryLayoutDigest: r.registryLayoutDigest, RecoveryEpoch: request.Authorization.Recovery.Epoch,
		AppliedIndex: state.LastApplied,
	}, nil
}

func (r *Runtime) authorizeLiveRecoveryShardRequest(
	ctx context.Context,
	authorization RecoveryShardAuthorization,
) error {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return fmt.Errorf("raftstore: read live System recovery authorization: %w", err)
	}
	if err := r.requireExactActiveRegistryLayout(state); err != nil {
		return err
	}
	if state.LastApplied < authorization.SystemCommitIndex ||
		state.ActiveRegistryLayoutVersion != authorization.ActiveRegistryLayoutVersion ||
		state.Recovery == nil || *state.Recovery != authorization.Recovery {
		return errors.New("raftstore: recovery shard authorization is stale or no longer committed")
	}
	return nil
}

func (r *Runtime) prepareRecoveryDataShardLocal(
	ctx context.Context,
	authorization RecoveryShardAuthorization,
	shardID uint32,
) (DataState, error) {
	state, err := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if err != nil {
		return DataState{}, err
	}
	next := recoveryTargetIdentity(authorization.Recovery)
	desired := r.recoveryReplicaIDs(shardID)
	if len(state.ServingEpochs) == 2 {
		return state, validateRecoveryPrepared(state, next, desired)
	}
	if len(state.ServingEpochs) != 1 {
		return DataState{}, errors.New("raftstore: recovery shard has an invalid serving-epoch set")
	}
	previous := state.ServingEpochs[0]
	if previous.ClusterID != next.ClusterID || previous.RegistryGeneration != next.RegistryGeneration ||
		previous.RegistryLayoutDigest != next.RegistryLayoutDigest || previous.SystemEpoch+1 != next.SystemEpoch {
		return DataState{}, errors.New("raftstore: recovery shard does not serve the immediately preceding SystemEpoch")
	}
	result, submitted, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataPrepareEpoch, Identity: ShardRequestIdentity{PermitIdentity: previous, ShardID: shardID},
		Epoch: &next, ReplicaIDs: desired,
	})
	if proposeErr != nil && !submitted {
		return DataState{}, proposeErr
	}
	if proposeErr == nil && result.Conflict {
		proposeErr = errors.New(result.Reason)
	}
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.readDataStateStrong(resolveContext, DataRaftShardID(shardID))
	if readErr == nil && validateRecoveryPrepared(current, next, desired) == nil {
		return current, nil
	}
	if proposeErr != nil {
		return DataState{}, proposeErr
	}
	if readErr != nil {
		return DataState{}, readErr
	}
	return DataState{}, errors.New("raftstore: prepared recovery data epoch was not visible")
}

func (r *Runtime) finalizeRecoveryDataShardLocal(
	ctx context.Context,
	authorization RecoveryShardAuthorization,
	shardID uint32,
) (DataState, error) {
	state, err := r.readDataStateStrong(ctx, DataRaftShardID(shardID))
	if err != nil {
		return DataState{}, err
	}
	next := recoveryTargetIdentity(authorization.Recovery)
	desired := r.recoveryReplicaIDs(shardID)
	if recoveryDataShardClosed(state, next, desired) {
		return state, nil
	}
	if err := validateRecoveryPrepared(state, next, desired); err != nil {
		return DataState{}, err
	}
	previous := state.ServingEpochs[0]
	result, submitted, proposeErr := r.proposeDataRaw(ctx, DataCommand{
		Type: DataRetireEpoch, Identity: ShardRequestIdentity{PermitIdentity: next, ShardID: shardID}, Epoch: &previous,
	})
	if proposeErr != nil && !submitted {
		return DataState{}, proposeErr
	}
	if proposeErr == nil && result.Conflict {
		proposeErr = errors.New(result.Reason)
	}
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.readDataStateStrong(resolveContext, DataRaftShardID(shardID))
	if readErr == nil && recoveryDataShardClosed(current, next, desired) {
		return current, nil
	}
	if proposeErr != nil {
		return DataState{}, proposeErr
	}
	if readErr != nil {
		return DataState{}, readErr
	}
	return DataState{}, errors.New("raftstore: retired recovery data epoch was not visible")
}

func recoveryTargetIdentity(recovery RecoveryEpoch) PermitIdentity {
	return PermitIdentity{
		ClusterID: recovery.SourceClusterID, RegistryGeneration: recovery.TargetRegistryGeneration,
		SystemEpoch: recovery.Epoch, RegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
	}
}

func sameRecoveryIdentity(left, right RecoveryEpoch) bool {
	return left.Epoch == right.Epoch && left.SourceClusterID == right.SourceClusterID &&
		left.SourceRegistryGeneration == right.SourceRegistryGeneration &&
		left.SourceRegistryLayoutDigest == right.SourceRegistryLayoutDigest &&
		left.TargetRegistryGeneration == right.TargetRegistryGeneration &&
		left.TargetRegistryLayoutDigest == right.TargetRegistryLayoutDigest
}

func (r *Runtime) recoveryReplicaIDs(shardID uint32) []uint64 {
	return replicaIDsForPlacement(r.registryLayout.DataShards[shardID])
}

func validateRecoveryPrepared(state DataState, next PermitIdentity, replicas []uint64) error {
	previous := next
	if previous.SystemEpoch == 0 {
		return errors.New("raftstore: recovery data epoch is invalid")
	}
	previous.SystemEpoch--
	if len(state.ServingEpochs) != 2 || state.ServingEpochs[0] != previous || state.ServingEpochs[1] != next ||
		!slices.Equal(state.ReplicaIDs, replicas) || !slices.Equal(state.PreparedReplicaIDs, replicas) {
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
