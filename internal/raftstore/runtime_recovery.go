package raftstore

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// BeginRecovery opens the sole recovery epoch for a successor Registry History Generation and
// snapshots every active enrollment into the System Group. Normal serving
// gates remain closed throughout the workflow.
func (r *Runtime) BeginRecovery(ctx context.Context) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.BeginRecovery(operation)
		if err == nil {
			err = r.acceptRemoteSystemState(state)
		}
		return state, err
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if state.Recovery != nil {
		return state, nil
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	nodes := make(map[string]RecoveryNodeProgress)
	for nodeID, enrollment := range state.NodeEnrollments {
		if enrollment.Retired {
			continue
		}
		nodes[nodeID] = RecoveryNodeProgress{
			NodeID: nodeID, EnrollmentID: enrollment.EnrollmentID,
			NodeEpoch: enrollment.MaxNodeEpoch, State: RecoveryNodeExpected,
		}
	}
	recovery := &RecoveryEpoch{
		Epoch: state.SystemEpoch + 1, SourceClusterID: state.ClusterID,
		SourceRegistryGeneration:   state.PredecessorRegistryGeneration,
		SourceRegistryLayoutDigest: state.PredecessorRegistryLayoutDigest,
		TargetRegistryGeneration:   state.RegistryGeneration,
		TargetRegistryLayoutDigest: state.ActiveRegistryLayoutDigest,
		Phase:                      RecoveryPreparing, Nodes: nodes,
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{Type: SystemBeginRecovery, Recovery: recovery})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && current.Recovery != nil && sameRecoveryIdentity(*current.Recovery, *recovery) {
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
	return SystemState{}, errors.New("raftstore: opened recovery epoch was not visible")
}

// ConfirmRecoveryPermitDrain waits a full Serve Permit lifetime after a
// quorum read of the closed RecoveryEpoch. A restart conservatively restarts
// the monotonic wait.
func (r *Runtime) ConfirmRecoveryPermitDrain(ctx context.Context) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		timeout := time.Duration(MaximumServePermitMillis+r.config.Tuning.OperationTimeoutMillis) * time.Millisecond
		operation, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		state, err := r.systemClient.ConfirmRecoveryPermitDrain(operation)
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
	if err := r.authorizeRegistryLayoutState(current); err != nil {
		return SystemState{}, err
	}
	if current.Recovery == nil || current.Recovery.Phase != RecoveryPreparing ||
		!sameRecoveryIdentity(*current.Recovery, *state.Recovery) {
		return SystemState{}, errors.New("raftstore: recovery identity changed during permit drain")
	}
	if current.Recovery.PermitDrainComplete {
		return current, nil
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{
		Type: SystemConfirmRecoveryDrain,
		RecoveryDrain: &RecoveryDrainConfirmation{
			RecoveryEpoch: current.Recovery.Epoch, TargetRegistryGeneration: current.Recovery.TargetRegistryGeneration,
			TargetRegistryLayoutDigest: current.Recovery.TargetRegistryLayoutDigest,
			WaitedMillis:               uint64(time.Since(started) / time.Millisecond),
		},
	})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	confirmed, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && confirmed.Recovery != nil &&
		sameRecoveryIdentity(*confirmed.Recovery, *current.Recovery) && confirmed.Recovery.PermitDrainComplete {
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
	return SystemState{}, errors.New("raftstore: recovery permit drain was not committed")
}

func (r *Runtime) AdvanceRecovery(ctx context.Context, from, to RecoveryPhase) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.AdvanceRecovery(operation, from, to)
		if err == nil {
			err = r.acceptRemoteSystemState(state)
		}
		return state, err
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if state.Recovery == nil {
		if to == RecoveryClosed && state.SystemEpoch > 1 {
			return state, nil
		}
		return SystemState{}, errors.New("raftstore: no open recovery epoch")
	}
	if state.Recovery.Phase == to {
		return state, nil
	}
	if state.Recovery.Phase != from {
		return SystemState{}, errors.New("raftstore: recovery phase changed")
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{
		Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{From: from, To: to},
	})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && (to == RecoveryClosed && current.Recovery == nil ||
		current.Recovery != nil && current.Recovery.Phase == to) {
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
	return SystemState{}, errors.New("raftstore: recovery phase transition was not visible")
}

func (r *Runtime) SetServingGates(ctx context.Context, gates GateUpdate) (SystemState, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.SetServingGates(operation, gates)
		if err == nil {
			err = r.acceptRemoteSystemState(state)
		}
		return state, err
	}
	if gates.Write && !gates.Serve || gates.Cutover && !gates.Serve {
		return SystemState{}, errors.New("raftstore: invalid serving gate combination")
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	if state.ServeGate == gates.Serve && state.WriteGate == gates.Write && state.CutoverGate == gates.Cutover {
		return state, nil
	}
	result, proposeErr := r.proposeSystem(ctx, SystemCommand{Type: SystemSetGates, Gates: &gates})
	resolveContext, cancelResolve := ambiguityResolutionContext(ctx)
	defer cancelResolve()
	current, readErr := r.ReadSystemStrong(resolveContext)
	if readErr == nil && current.ServeGate == gates.Serve && current.WriteGate == gates.Write &&
		current.CutoverGate == gates.Cutover {
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
	return SystemState{}, errors.New("raftstore: serving gate update was not visible")
}

// ApplyRecoveryData is intentionally separate from ApplyData. It authorizes an
// exact open RecoveryEpoch with a strong System Group read and cannot be used
// to bypass closed normal-operation gates.
func (r *Runtime) ApplyRecoveryData(ctx context.Context, command DataCommand) (DataApplyResult, error) {
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return DataApplyResult{}, err
	}
	if err := validateRecoveryDataCommand(state, command); err != nil {
		return DataApplyResult{}, err
	}
	if err := r.authorizeLocalDataReplica(command.Identity); err != nil {
		return DataApplyResult{}, err
	}
	return r.proposeDataRaw(ctx, command)
}

func (r *Runtime) ReadRecoveryData(ctx context.Context, query RecoveryLookup) (RecoveryLookupResult, error) {
	if err := query.Validate(); err != nil {
		return RecoveryLookupResult{}, err
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return RecoveryLookupResult{}, err
	}
	if state.Recovery == nil || !state.Recovery.PermitDrainComplete ||
		query.Identity.PermitIdentity != recoveryTarget(*state.Recovery) {
		return RecoveryLookupResult{}, errors.New("raftstore: recovery read does not match the open System epoch")
	}
	if err := r.authorizeLocalDataReplica(query.Identity); err != nil {
		return RecoveryLookupResult{}, err
	}
	value, err := r.syncRead(ctx, DataRaftShardID(query.Identity.ShardID), DataLookup{Recovery: &query})
	if err != nil {
		return RecoveryLookupResult{}, err
	}
	result, ok := value.(DataLookupResult)
	if !ok || result.Recovery == nil {
		return RecoveryLookupResult{}, errors.New("raftstore: invalid recovery lookup result")
	}
	return *result.Recovery, nil
}

func validateRecoveryDataCommand(state SystemState, command DataCommand) error {
	if state.Recovery == nil {
		return errors.New("raftstore: data recovery requires an open System recovery epoch")
	}
	recovery := *state.Recovery
	if !recovery.PermitDrainComplete || recovery.PermitDrainIndex == 0 ||
		recovery.PermitDrainIndex > state.LastApplied {
		return errors.New("raftstore: recovery data operation requires committed Permit drain")
	}
	target := recoveryTarget(recovery)
	want := ShardRequestIdentity{PermitIdentity: target, ShardID: command.Identity.ShardID}
	final := want
	final.SystemEpoch++
	if command.Identity != want && (command.Type != DataFinalizeRecovery || command.Identity != final) {
		return errors.New("raftstore: recovery data command identity mismatch")
	}
	switch command.Type {
	case DataBeginRecovery:
		start := command.RecoveryStart
		if recovery.Phase != RecoveryPreparing || start == nil || start.RecoveryEpoch != recovery.Epoch ||
			start.SourceClusterID != recovery.SourceClusterID ||
			start.SourceRegistryGeneration != recovery.SourceRegistryGeneration ||
			start.SourceRegistryLayoutDigest != recovery.SourceRegistryLayoutDigest || start.Target != target {
			return errors.New("raftstore: invalid data recovery initialization")
		}
	case DataStageRecovery:
		if recovery.Phase != RecoveryCollecting || command.RecoveryRecord == nil {
			return errors.New("raftstore: recovery report staging is outside COLLECTING")
		}
	case DataAckRecovery, DataActivateRecovery, DataQuarantineRecovery:
		if recovery.Phase != RecoveryReconciling || command.RecoveryUpdate == nil {
			return errors.New("raftstore: recovery object reconciliation is outside RECONCILING")
		}
	case DataFinalizeRecovery:
		if recovery.Phase != RecoveryFinalizing || command.RecoveryFinal == nil {
			return errors.New("raftstore: data recovery finalization is outside FINALIZING")
		}
	default:
		return fmt.Errorf("raftstore: command %q is not a recovery data mutation", command.Type)
	}
	return nil
}

func recoveryTarget(recovery RecoveryEpoch) PermitIdentity {
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
