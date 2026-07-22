package raftstore

import (
	"errors"
	"reflect"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

func validateRouteTransition(
	current clusterstate.RouteWorkflowRecord,
	next clusterstate.RouteWorkflowRecord,
	fences map[string]clusterstate.ExecutionFence,
	usedSandboxIDs map[string]struct{},
) error {
	if current.Group != next.Group || current.RouteKey != next.RouteKey {
		return errors.New("raftstore: Route identity changed")
	}
	finalizations, err := classifyFinalizationChange(current.Finalizations, next.Finalizations)
	if err != nil {
		return err
	}
	if current.State == next.State {
		return validateSameRouteState(current, next, finalizations)
	}
	if finalizations.kind != finalizationsUnchanged {
		return errors.New("raftstore: Route state transition changed pending finalizations")
	}
	switch current.State {
	case clusterstate.WorkflowRouteStarting:
		if next.State == clusterstate.WorkflowRouteReady {
			if !readyMatchesStarting(*current.Starting, *next.Ready) {
				return errors.New("raftstore: READY does not match committed STARTING execution")
			}
			return nil
		}
		if next.State == clusterstate.WorkflowRouteTombstone {
			return validateStartingTombstone(*current.Starting, *next.Tombstone)
		}
	case clusterstate.WorkflowRouteReady:
		if next.State == clusterstate.WorkflowRouteTombstone {
			return validateProvenRouteTombstone(*current.Ready, *next.Tombstone)
		}
		if next.State == clusterstate.WorkflowRoutePaused &&
			sameExecutionEnteringPaused(*current.Ready, next.Paused.Execution) &&
			next.Paused.Execution.LastEventSeq > current.Ready.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRouteDeleting &&
			sameReadyExecution(*current.Ready, next.Deleting.Execution) &&
			next.Deleting.Execution.LastEventSeq >= current.Ready.LastEventSeq {
			return nil
		}
	case clusterstate.WorkflowRoutePaused:
		if next.State == clusterstate.WorkflowRouteTombstone {
			return validateProvenRouteTombstone(current.Paused.Execution, *next.Tombstone)
		}
		if next.State == clusterstate.WorkflowRouteReady {
			return validateAutoResume(current.Paused.Execution, *next.Ready)
		}
		if next.State == clusterstate.WorkflowRouteResuming &&
			sameReadyExecution(current.Paused.Execution, next.Resuming.Execution) &&
			reflect.DeepEqual(current.Paused.ResumeIntent, next.Resuming.Intent) &&
			next.Resuming.Execution.LastEventSeq >= current.Paused.Execution.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRouteDeleting &&
			sameReadyExecution(current.Paused.Execution, next.Deleting.Execution) &&
			next.Deleting.Execution.LastEventSeq >= current.Paused.Execution.LastEventSeq {
			return nil
		}
	case clusterstate.WorkflowRouteResuming:
		if next.State == clusterstate.WorkflowRouteTombstone {
			return validateProvenRouteTombstone(current.Resuming.Execution, *next.Tombstone)
		}
		if next.State == clusterstate.WorkflowRouteReady &&
			sameReadyExecution(current.Resuming.Execution, *next.Ready) &&
			next.Ready.LastEventSeq > current.Resuming.Execution.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRoutePaused &&
			sameExecutionEnteringPaused(current.Resuming.Execution, next.Paused.Execution) &&
			next.Paused.Execution.LastEventSeq > current.Resuming.Execution.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRouteDeleting &&
			sameReadyExecution(current.Resuming.Execution, next.Deleting.Execution) &&
			next.Deleting.Execution.LastEventSeq >= current.Resuming.Execution.LastEventSeq {
			return nil
		}
	case clusterstate.WorkflowRouteDeleting:
		if next.State == clusterstate.WorkflowRouteTombstone && next.Tombstone != nil &&
			!next.Tombstone.FenceCompacted &&
			next.Tombstone.SandboxID == current.Deleting.Execution.SandboxID &&
			next.Tombstone.NodeID == current.Deleting.Execution.NodeID &&
			next.Tombstone.NodeEpoch == current.Deleting.Execution.NodeEpoch &&
			next.Tombstone.BindingDigest == current.Deleting.Execution.BindingDigest &&
			next.Tombstone.LastEventSeq >= current.Deleting.LastEventSeq {
			if isPermanentExecutionProof(next.Tombstone.Proof) {
				if next.Tombstone.LastEventSeq != max(current.Deleting.Execution.LastEventSeq, current.Deleting.LastEventSeq) {
					return errors.New("raftstore: fencing proof changed the Route event watermark")
				}
			} else if next.Tombstone.Proof.Kind != clusterstate.ProofNodeTerminal {
				return errors.New("raftstore: Route tombstone lacks a recognized execution proof")
			}
			return nil
		}
	case clusterstate.WorkflowRouteTombstone:
		if next.State == clusterstate.WorkflowRouteStarting {
			return validateRouteReplacement(current, next, fences, usedSandboxIDs)
		}
	}
	return errors.New("raftstore: illegal Route state transition")
}

func validateSameRouteState(
	current, next clusterstate.RouteWorkflowRecord,
	finalizations finalizationChange,
) error {
	if finalizations.kind == finalizationsRemoved {
		if sameRouteBusinessState(current, next) {
			return nil
		}
		return errors.New("raftstore: Route finalization completion changed workflow state")
	}
	switch current.State {
	case clusterstate.WorkflowRouteStarting:
		if err := validateRouteStartingUpdate(*current.Starting, *next.Starting); err != nil {
			return err
		}
		if current.Starting.SelectedCandidate != nil && next.Starting.SelectedCandidate == nil {
			if finalizations.kind != finalizationsAppended ||
				!finalizationMatchesBinding(finalizations.intent, current.Starting.SandboxID, *current.Starting.Binding, false) {
				return errors.New("raftstore: definitive Route rejection lacks exact finalization intent")
			}
			return nil
		}
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: Route STARTING changed finalizations without rejection")
		}
		return nil
	case clusterstate.WorkflowRouteReady:
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: READY appended a workflow finalization")
		}
		if !sameReadyExecution(*current.Ready, *next.Ready) ||
			current.Ready.LastEventSeq > next.Ready.LastEventSeq {
			return errors.New("raftstore: READY execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRoutePaused:
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: PAUSED appended a workflow finalization")
		}
		currentCopy, nextCopy := *current.Paused, *next.Paused
		currentCopy.Execution.LastEventSeq, nextCopy.Execution.LastEventSeq = 0, 0
		if !reflect.DeepEqual(currentCopy, nextCopy) ||
			current.Paused.Execution.LastEventSeq > next.Paused.Execution.LastEventSeq {
			return errors.New("raftstore: PAUSED execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRouteResuming:
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: RESUMING appended a workflow finalization")
		}
		currentCopy, nextCopy := *current.Resuming, *next.Resuming
		currentCopy.Execution.LastEventSeq, nextCopy.Execution.LastEventSeq = 0, 0
		if !reflect.DeepEqual(currentCopy, nextCopy) ||
			current.Resuming.Execution.LastEventSeq > next.Resuming.Execution.LastEventSeq {
			return errors.New("raftstore: RESUMING execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRouteDeleting:
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: DELETING appended a workflow finalization")
		}
		currentCopy, nextCopy := *current.Deleting, *next.Deleting
		currentCopy.Execution.LastEventSeq, nextCopy.Execution.LastEventSeq = 0, 0
		currentCopy.LastEventSeq, nextCopy.LastEventSeq = 0, 0
		if !reflect.DeepEqual(currentCopy, nextCopy) ||
			current.Deleting.Execution.LastEventSeq > next.Deleting.Execution.LastEventSeq ||
			current.Deleting.LastEventSeq > next.Deleting.LastEventSeq {
			return errors.New("raftstore: DELETING execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRouteTombstone:
		if finalizations.kind != finalizationsUnchanged {
			return errors.New("raftstore: TOMBSTONE appended a workflow finalization")
		}
		if !reflect.DeepEqual(current.Tombstone, next.Tombstone) {
			return errors.New("raftstore: TOMBSTONE is immutable")
		}
		return nil
	default:
		return errors.New("raftstore: invalid Route state")
	}
}

func validateAutoResume(paused, ready clusterstate.ReadyRoute) error {
	if !sameReadyExecution(paused, ready) ||
		ready.LastEventSeq <= paused.LastEventSeq {
		return errors.New("raftstore: node-local auto-resume changed execution identity or lacked a newer event")
	}
	return nil
}

func validateRouteReplacement(
	current clusterstate.RouteWorkflowRecord,
	next clusterstate.RouteWorkflowRecord,
	fences map[string]clusterstate.ExecutionFence,
	usedSandboxIDs map[string]struct{},
) error {
	if _, reused := usedSandboxIDs[fenceMapKey(current.Group, current.RouteKey, next.Starting.SandboxID)]; reused {
		return errors.New("raftstore: replacement reused a historically fenced SID")
	}
	if current.Tombstone.PlacementFailure != nil {
		failure := current.Tombstone.PlacementFailure
		if next.Starting.SandboxID == failure.SandboxID ||
			next.Starting.PlacementRound != failure.PlacementRound+1 ||
			next.Starting.SelectedCandidate != nil || next.Starting.Binding != nil ||
			len(next.Starting.DefinitivelyRejected) != 0 || next.Starting.LastEventSeq != 0 ||
			!reflect.DeepEqual(next.Starting.Intent, failure.Intent) {
			return errors.New("raftstore: placement retry requires a clean next round and a new SID")
		}
		if current.Tombstone.FenceCompacted {
			return nil
		}
		key := fenceMapKey(current.Group, current.RouteKey, failure.SandboxID)
		fence, found := fences[key]
		if !found || !placementFenceMatchesFailure(
			fence, current.Group, current.RouteKey, current.Revision.RegistryGeneration, *failure,
		) {
			return errors.New("raftstore: placement retry has no committed abandoned-SID fence")
		}
		return nil
	}
	if next.Starting.SandboxID == current.Tombstone.SandboxID {
		return errors.New("raftstore: replacement reused the fenced SID")
	}
	if current.Tombstone.FenceCompacted {
		return nil
	}
	key := fenceMapKey(current.Group, current.RouteKey, current.Tombstone.SandboxID)
	fence, found := fences[key]
	if !found || fence.Proof.ProofDigest != current.Tombstone.Proof.ProofDigest ||
		fence.NodeID != current.Tombstone.NodeID || fence.NodeEpoch != current.Tombstone.NodeEpoch ||
		fence.BindingDigest != current.Tombstone.BindingDigest {
		return errors.New("raftstore: replacement has no committed old-execution fence")
	}
	return nil
}

func validateBuildTransition(current, next clusterstate.BuildRecord) error {
	if current.Group != next.Group || current.BuildID != next.BuildID {
		return errors.New("raftstore: Build identity changed")
	}
	finalizations, err := classifyFinalizationChange(current.Finalizations, next.Finalizations)
	if err != nil {
		return err
	}
	if current.State == next.State {
		if finalizations.kind == finalizationsRemoved {
			if sameBuildBusinessState(current, next) {
				return nil
			}
			return errors.New("raftstore: Build finalization completion changed workflow state")
		}
		if current.State == clusterstate.BuildStarting {
			if !reflect.DeepEqual(current.Starting.CandidatePool, next.Starting.CandidatePool) ||
				!reflect.DeepEqual(current.Starting.Intent, next.Starting.Intent) {
				return errors.New("raftstore: BUILD_STARTING intent changed or proof regressed")
			}
			if err := validateStartingCandidateProgress(
				current.Starting.SelectedCandidate, next.Starting.SelectedCandidate,
				current.Starting.Binding, next.Starting.Binding,
				current.Starting.DefinitivelyRejected, next.Starting.DefinitivelyRejected,
				0, 0,
			); err != nil {
				return err
			}
			if current.Starting.SelectedCandidate != nil && next.Starting.SelectedCandidate == nil {
				if finalizations.kind != finalizationsAppended ||
					!finalizationMatchesBinding(finalizations.intent, current.BuildID, *current.Starting.Binding, false) {
					return errors.New("raftstore: definitive Build rejection lacks exact finalization intent")
				}
				return nil
			}
			if finalizations.kind != finalizationsUnchanged {
				return errors.New("raftstore: BUILD_STARTING changed finalizations without rejection")
			}
			return nil
		}
		if finalizations.kind == finalizationsUnchanged && sameBuildBusinessState(current, next) {
			return nil
		}
		return errors.New("raftstore: committed Build registration state is immutable")
	}
	if current.State != clusterstate.BuildStarting ||
		(next.State != clusterstate.BuildRegistered && next.State != clusterstate.BuildTombstone) {
		return errors.New("raftstore: illegal Build state transition")
	}
	if finalizations.kind != finalizationsUnchanged {
		return errors.New("raftstore: Build registration transition changed pending finalizations")
	}
	if next.State == clusterstate.BuildTombstone {
		if next.Tombstone == nil ||
			!buildFailureMatchesStarting(*current.Starting, next.Tombstone.PlacementFailure) {
			return errors.New("raftstore: Build placement tombstone differs from committed STARTING intent")
		}
		return nil
	}
	if next.Projection == nil || current.Starting.Binding == nil ||
		!buildProjectionMatchesBinding(*next.Projection, *current.Starting.Binding, current.Starting.Intent) {
		return errors.New("raftstore: Build registration does not match committed Binding")
	}
	return nil
}

func validateRouteStartingUpdate(current, next clusterstate.RouteStartingState) error {
	if current.SandboxID == next.SandboxID {
		if current.PlacementRound != next.PlacementRound ||
			!reflect.DeepEqual(current.CandidatePool, next.CandidatePool) ||
			!reflect.DeepEqual(current.Intent, next.Intent) {
			return errors.New("raftstore: STARTING immutable intent changed")
		}
		return validateStartingCandidateProgress(
			current.SelectedCandidate, next.SelectedCandidate,
			current.Binding, next.Binding,
			current.DefinitivelyRejected, next.DefinitivelyRejected,
			current.LastEventSeq, next.LastEventSeq,
		)
	}
	return errors.New("raftstore: a new SID must pass through a placement-failure tombstone and fence")
}

func validateStartingCandidateProgress(
	currentSelected, nextSelected *uint32,
	currentBinding, nextBinding *clusterstate.ExecutionBindingIntent,
	currentRejected, nextRejected []uint32,
	currentEventSeq, nextEventSeq uint64,
) error {
	switch {
	case currentSelected == nil && nextSelected == nil:
		if currentBinding != nil || nextBinding != nil ||
			!extendsRejectedCandidates(currentRejected, nextRejected) || currentEventSeq != nextEventSeq {
			return errors.New("raftstore: unselected workflow changed outside monotonic candidate rejection")
		}
	case currentSelected == nil && nextSelected != nil:
		if !sameRejectedCandidates(currentRejected, nextRejected) || currentEventSeq != nextEventSeq {
			return errors.New("raftstore: candidate selection changed rejection or event progress")
		}
	case currentSelected != nil && nextSelected != nil:
		if *currentSelected != *nextSelected || !reflect.DeepEqual(currentBinding, nextBinding) ||
			!sameRejectedCandidates(currentRejected, nextRejected) || currentEventSeq > nextEventSeq {
			return errors.New("raftstore: selected candidate identity changed or event progress regressed")
		}
	case currentSelected != nil && nextSelected == nil:
		if !appendsRejectedCandidate(currentRejected, nextRejected, *currentSelected) ||
			nextBinding != nil || currentEventSeq != nextEventSeq {
			return errors.New("raftstore: selected candidate lacks an exact definitive no-side-effect rejection")
		}
	}
	return nil
}

func sameRejectedCandidates(current, next []uint32) bool {
	if len(current) != len(next) {
		return false
	}
	for index := range current {
		if current[index] != next[index] {
			return false
		}
	}
	return true
}

func extendsRejectedCandidates(current, next []uint32) bool {
	if len(next) < len(current) {
		return false
	}
	for index := range current {
		if current[index] != next[index] {
			return false
		}
	}
	return true
}

func appendsRejectedCandidate(current, next []uint32, candidate uint32) bool {
	if len(next) != len(current)+1 || next[len(current)] != candidate {
		return false
	}
	for index := range current {
		if current[index] != next[index] {
			return false
		}
	}
	return true
}

func readyMatchesStarting(starting clusterstate.RouteStartingState, ready clusterstate.ReadyRoute) bool {
	if starting.Binding == nil {
		return false
	}
	return ready.SandboxID == starting.SandboxID && ready.NodeID == starting.Binding.NodeID &&
		ready.NodeEpoch == starting.Binding.NodeEpoch && ready.DataEndpoint == starting.Binding.DataEndpoint &&
		ready.RegistryGeneration == starting.Binding.RegistryGeneration &&
		ready.BindingDigest == starting.Binding.BindingDigest && reflect.DeepEqual(ready.Intent, starting.Intent) &&
		ready.LastEventSeq > starting.LastEventSeq
}

func validateStartingTombstone(starting clusterstate.RouteStartingState, tombstone clusterstate.RouteTombstoneState) error {
	if tombstone.PlacementFailure != nil {
		failure := tombstone.PlacementFailure
		if tombstone.FenceCompacted || starting.SelectedCandidate != nil || starting.Binding != nil ||
			len(starting.DefinitivelyRejected) != len(starting.CandidatePool) ||
			failure.SandboxID != starting.SandboxID || failure.PlacementRound != starting.PlacementRound ||
			!reflect.DeepEqual(failure.CandidatePool, starting.CandidatePool) ||
			!reflect.DeepEqual(failure.Intent, starting.Intent) ||
			!sameRejectedCandidates(failure.DefinitivelyRejected, starting.DefinitivelyRejected) {
			return errors.New("raftstore: placement failure differs from committed STARTING intent")
		}
		return nil
	}
	if tombstone.FenceCompacted || starting.Binding == nil || tombstone.SandboxID != starting.SandboxID ||
		tombstone.NodeID != starting.Binding.NodeID || tombstone.NodeEpoch != starting.Binding.NodeEpoch ||
		tombstone.BindingDigest != starting.Binding.BindingDigest {
		return errors.New("raftstore: execution tombstone differs from committed STARTING Binding")
	}
	if isPermanentExecutionProof(tombstone.Proof) {
		if tombstone.LastEventSeq != starting.LastEventSeq {
			return errors.New("raftstore: fencing proof changed the STARTING event watermark")
		}
	} else if tombstone.Proof.Kind != clusterstate.ProofNodeTerminal || tombstone.LastEventSeq <= starting.LastEventSeq {
		return errors.New("raftstore: STARTING tombstone lacks a newer terminal event or permanent fence")
	}
	return nil
}

func validateProvenRouteTombstone(execution clusterstate.ReadyRoute, tombstone clusterstate.RouteTombstoneState) error {
	if !isPermanentExecutionProof(tombstone.Proof) || tombstone.SandboxID != execution.SandboxID ||
		tombstone.NodeID != execution.NodeID || tombstone.NodeEpoch != execution.NodeEpoch ||
		tombstone.BindingDigest != execution.BindingDigest || tombstone.LastEventSeq != execution.LastEventSeq {
		return errors.New("raftstore: Route fencing proof differs from the committed execution")
	}
	return nil
}

func isPermanentExecutionProof(proof clusterstate.TerminalProof) bool {
	return proof.Kind == clusterstate.ProofNewerNodeEpoch || proof.Kind == clusterstate.ProofExternalFence
}

func sameReadyExecution(left, right clusterstate.ReadyRoute) bool {
	left.LastEventSeq, right.LastEventSeq = 0, 0
	return reflect.DeepEqual(left, right)
}

func sameExecutionEnteringPaused(left, right clusterstate.ReadyRoute) bool {
	left.LastEventSeq, right.LastEventSeq = 0, 0
	left.SnapshotRef = right.SnapshotRef
	return reflect.DeepEqual(left, right)
}

func buildFailureMatchesStarting(starting clusterstate.BuildStartingState, failure clusterstate.BuildPlacementFailureState) bool {
	return starting.SelectedCandidate == nil && starting.Binding == nil &&
		len(starting.DefinitivelyRejected) == len(starting.CandidatePool) &&
		failure.BuildID == starting.BuildID && reflect.DeepEqual(failure.CandidatePool, starting.CandidatePool) &&
		reflect.DeepEqual(failure.Intent, starting.Intent) &&
		sameRejectedCandidates(failure.DefinitivelyRejected, starting.DefinitivelyRejected)
}

func buildProjectionMatchesBinding(
	projection clusterstate.BuildProjection,
	binding clusterstate.ExecutionBindingIntent,
	intent clusterstate.DispatchIntent,
) bool {
	return projection.BuildID != "" && projection.NodeID == binding.NodeID && projection.NodeEpoch == binding.NodeEpoch &&
		projection.DataEndpoint == binding.DataEndpoint &&
		projection.RegistryGeneration == binding.RegistryGeneration && projection.BindingDigest == binding.BindingDigest &&
		reflect.DeepEqual(projection.Intent, intent)
}

type finalizationChangeKind uint8

const (
	finalizationsUnchanged finalizationChangeKind = iota
	finalizationsAppended
	finalizationsRemoved
)

type finalizationChange struct {
	kind   finalizationChangeKind
	intent clusterstate.WorkflowFinalizationIntent
}

func classifyFinalizationChange(
	current, next []clusterstate.WorkflowFinalizationIntent,
) (finalizationChange, error) {
	if sameFinalizations(current, next) {
		return finalizationChange{kind: finalizationsUnchanged}, nil
	}
	if len(next) == len(current)+1 && sameFinalizations(current, next[:len(current)]) {
		return finalizationChange{kind: finalizationsAppended, intent: next[len(next)-1]}, nil
	}
	if len(current) == len(next)+1 {
		for removed := range current {
			candidate := append([]clusterstate.WorkflowFinalizationIntent(nil), current[:removed]...)
			candidate = append(candidate, current[removed+1:]...)
			if sameFinalizations(candidate, next) {
				return finalizationChange{kind: finalizationsRemoved, intent: current[removed]}, nil
			}
		}
	}
	return finalizationChange{}, errors.New("raftstore: pending workflow finalizations changed non-monotonically")
}

func sameFinalizations(left, right []clusterstate.WorkflowFinalizationIntent) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !reflect.DeepEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func finalizationMatchesBinding(
	intent clusterstate.WorkflowFinalizationIntent,
	objectID string,
	binding clusterstate.ExecutionBindingIntent,
	terminal bool,
) bool {
	return intent.ObjectID == objectID && intent.NodeID == binding.NodeID && intent.NodeEpoch == binding.NodeEpoch &&
		intent.DataEndpoint == binding.DataEndpoint && intent.RegistryGeneration == binding.RegistryGeneration &&
		intent.BindingDigest == binding.BindingDigest && (intent.TerminalProof != nil) == terminal
}

func sameRouteBusinessState(left, right clusterstate.RouteWorkflowRecord) bool {
	left.Revision, right.Revision = clusterstate.Revision{}, clusterstate.Revision{}
	left.Finalizations, right.Finalizations = nil, nil
	return reflect.DeepEqual(left, right)
}

func sameBuildBusinessState(left, right clusterstate.BuildRecord) bool {
	left.Revision, right.Revision = clusterstate.Revision{}, clusterstate.Revision{}
	left.Finalizations, right.Finalizations = nil, nil
	return reflect.DeepEqual(left, right)
}

func validateFenceCompaction(
	state DataState,
	fence clusterstate.ExecutionFence,
	authorization FenceCompactionAuthorization,
	identity ShardRequestIdentity,
) error {
	if authorization.FenceRevision != fence.Revision.LogIndex ||
		authorization.TerminalProofDigest != fence.ProofDigest() ||
		!isSHA256(authorization.RetentionProofDigest) {
		return errors.New("raftstore: fence compaction proof does not match the committed fence")
	}
	outboxCovered := fence.PlacementFailure != nil || authorization.FinalOutboxWatermarkAcked &&
		fence.FinalOutboxWatermark >= fence.LastEventSeq
	proofPermanentlyFenced := fence.PlacementFailure == nil &&
		(fence.Proof.Kind == clusterstate.ProofNewerNodeEpoch || fence.Proof.Kind == clusterstate.ProofExternalFence)
	nodeEpochPermanentlyFenced := authorization.NodeEpochFence != nil &&
		authorization.NodeEpochFence.validates(identity.PermitIdentity, fence)
	if authorization.NodeEpochFence != nil && !nodeEpochPermanentlyFenced {
		return errors.New("raftstore: fence compaction carries an invalid NodeEpoch proof")
	}
	if !outboxCovered && !proofPermanentlyFenced && !nodeEpochPermanentlyFenced {
		return errors.New("raftstore: fence compaction lacks a final outbox or NodeEpoch proof")
	}
	replicaIDs := compactionReplicaIDs(state)
	if len(authorization.ReplicaApplied) != len(replicaIDs) {
		return errors.New("raftstore: fence compaction lacks every replica watermark")
	}
	for i, applied := range authorization.ReplicaApplied {
		if applied.ReplicaID != replicaIDs[i] || applied.AppliedIndex < fence.Revision.LogIndex {
			return errors.New("raftstore: a replica has not applied the fence revision")
		}
	}
	return nil
}

func compactionReplicaIDs(state DataState) []uint64 {
	set := make(map[uint64]struct{}, len(state.ReplicaIDs)+len(state.PreparedReplicaIDs))
	for _, replicaID := range state.ReplicaIDs {
		set[replicaID] = struct{}{}
	}
	for _, replicaID := range state.PreparedReplicaIDs {
		set[replicaID] = struct{}{}
	}
	replicaIDs := make([]uint64, 0, len(set))
	for replicaID := range set {
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Slice(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] })
	return replicaIDs
}

func fenceMatchesRouteTombstone(fence clusterstate.ExecutionFence, route clusterstate.RouteWorkflowRecord) bool {
	if route.State != clusterstate.WorkflowRouteTombstone || route.Tombstone == nil ||
		route.Tombstone.FenceCompacted || route.Group != fence.Group || route.RouteKey != fence.RouteKey {
		return false
	}
	if route.Tombstone.PlacementFailure != nil {
		return placementFenceMatchesFailure(
			fence, route.Group, route.RouteKey, route.Revision.RegistryGeneration, *route.Tombstone.PlacementFailure,
		)
	}
	return fence.PlacementFailure == nil && route.Tombstone.SandboxID == fence.SandboxID &&
		route.Tombstone.NodeID == fence.NodeID && route.Tombstone.NodeEpoch == fence.NodeEpoch &&
		route.Tombstone.RegistryGeneration == fence.RegistryGeneration &&
		route.Tombstone.LastEventSeq == fence.LastEventSeq &&
		route.Tombstone.BindingDigest == fence.BindingDigest && route.Tombstone.Proof == fence.Proof
}

func placementFenceMatchesFailure(
	fence clusterstate.ExecutionFence,
	group string,
	routeKey string,
	registryGeneration string,
	failure clusterstate.RoutePlacementFailureState,
) bool {
	if fence.Group != group || fence.RouteKey != routeKey || fence.SandboxID != failure.SandboxID ||
		fence.RegistryGeneration != registryGeneration ||
		fence.PlacementFailure == nil || !reflect.DeepEqual(*fence.PlacementFailure, failure) {
		return false
	}
	digest, err := clusterstate.PlacementFailureProofDigest(
		group, routeKey, registryGeneration, failure,
	)
	return err == nil && digest == fence.PlacementFailureDigest
}

func validateFenceTransition(current, next clusterstate.ExecutionFence) error {
	currentRevision, nextRevision := current.Revision, next.Revision
	currentWatermark, nextWatermark := current.FinalOutboxWatermark, next.FinalOutboxWatermark
	current.Revision, next.Revision = clusterstate.Revision{}, clusterstate.Revision{}
	current.FinalOutboxWatermark, next.FinalOutboxWatermark = 0, 0
	if !reflect.DeepEqual(current, next) || nextWatermark < currentWatermark ||
		nextRevision.RegistryGeneration != currentRevision.RegistryGeneration || nextRevision.ShardID != currentRevision.ShardID {
		return errors.New("raftstore: execution fence identity or proof changed")
	}
	return nil
}
