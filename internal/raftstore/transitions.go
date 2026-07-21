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
) error {
	if current.Group != next.Group || current.RouteKey != next.RouteKey {
		return errors.New("raftstore: Route identity changed")
	}
	if current.State == next.State {
		return validateSameRouteState(current, next)
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
		if next.State == clusterstate.WorkflowRoutePaused &&
			sameReadyExecution(*current.Ready, next.Paused.Execution) &&
			next.Paused.Execution.LastEventSeq > current.Ready.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRouteDeleting &&
			sameReadyExecution(*current.Ready, next.Deleting.Execution) &&
			next.Deleting.Execution.LastEventSeq >= current.Ready.LastEventSeq {
			return nil
		}
	case clusterstate.WorkflowRoutePaused:
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
		if next.State == clusterstate.WorkflowRouteReady &&
			sameReadyExecution(current.Resuming.Execution, *next.Ready) &&
			next.Ready.LastEventSeq > current.Resuming.Execution.LastEventSeq {
			return nil
		}
		if next.State == clusterstate.WorkflowRoutePaused &&
			sameReadyExecution(current.Resuming.Execution, next.Paused.Execution) &&
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
			return nil
		}
	case clusterstate.WorkflowRouteTombstone:
		if next.State == clusterstate.WorkflowRouteStarting {
			return validateRouteReplacement(current, next, fences)
		}
	}
	return errors.New("raftstore: illegal Route state transition")
}

func validateSameRouteState(current, next clusterstate.RouteWorkflowRecord) error {
	switch current.State {
	case clusterstate.WorkflowRouteStarting:
		return validateRouteStartingUpdate(*current.Starting, *next.Starting)
	case clusterstate.WorkflowRouteReady:
		if !sameReadyExecution(*current.Ready, *next.Ready) ||
			current.Ready.LastEventSeq > next.Ready.LastEventSeq {
			return errors.New("raftstore: READY execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRoutePaused:
		currentCopy, nextCopy := *current.Paused, *next.Paused
		currentCopy.Execution.LastEventSeq, nextCopy.Execution.LastEventSeq = 0, 0
		if !reflect.DeepEqual(currentCopy, nextCopy) ||
			current.Paused.Execution.LastEventSeq > next.Paused.Execution.LastEventSeq {
			return errors.New("raftstore: PAUSED execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRouteResuming:
		currentCopy, nextCopy := *current.Resuming, *next.Resuming
		currentCopy.Execution.LastEventSeq, nextCopy.Execution.LastEventSeq = 0, 0
		if !reflect.DeepEqual(currentCopy, nextCopy) ||
			current.Resuming.Execution.LastEventSeq > next.Resuming.Execution.LastEventSeq {
			return errors.New("raftstore: RESUMING execution changed or event sequence regressed")
		}
		return nil
	case clusterstate.WorkflowRouteDeleting:
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
) error {
	if current.Tombstone.PlacementFailure != nil {
		if next.Starting.SandboxID == current.Tombstone.PlacementFailure.SandboxID ||
			next.Starting.PlacementRound <= current.Tombstone.PlacementFailure.PlacementRound ||
			!reflect.DeepEqual(next.Starting.Intent, current.Tombstone.PlacementFailure.Intent) {
			return errors.New("raftstore: placement retry requires a new SID and placement round")
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
	if current.State == next.State {
		if current.State == clusterstate.BuildStarting {
			if !reflect.DeepEqual(current.Starting.CandidatePool, next.Starting.CandidatePool) ||
				!reflect.DeepEqual(current.Starting.Intent, next.Starting.Intent) {
				return errors.New("raftstore: BUILD_STARTING intent changed or proof regressed")
			}
			return validateStartingCandidateProgress(
				current.Starting.SelectedCandidate, next.Starting.SelectedCandidate,
				current.Starting.Binding, next.Starting.Binding,
				current.Starting.DefinitivelyRejected, next.Starting.DefinitivelyRejected,
				current.Starting.LastEventSeq, next.Starting.LastEventSeq,
			)
		}
		if current.State == clusterstate.BuildError && current.Failure != nil {
			if reflect.DeepEqual(current.Failure, next.Failure) {
				return nil
			}
			return errors.New("raftstore: Build placement failure is immutable")
		}
		if current.State == clusterstate.BuildTombstone {
			if reflect.DeepEqual(current.Tombstone, next.Tombstone) {
				return nil
			}
			return errors.New("raftstore: BUILD_TOMBSTONE is immutable")
		}
		if current.Projection != nil && next.Projection != nil &&
			sameBuildProjection(*current.Projection, *next.Projection) &&
			current.Projection.LastEventSeq <= next.Projection.LastEventSeq {
			return nil
		}
		return errors.New("raftstore: Build execution changed or event sequence regressed")
	}
	allowed := current.State == clusterstate.BuildStarting &&
		(next.State == clusterstate.BuildQueued || next.State == clusterstate.BuildRegistered || next.State == clusterstate.BuildError) ||
		current.State == clusterstate.BuildQueued && (next.State == clusterstate.BuildRegistered || next.State == clusterstate.BuildError) ||
		current.State == clusterstate.BuildRegistered && (next.State == clusterstate.BuildBuilding || next.State == clusterstate.BuildError) ||
		current.State == clusterstate.BuildBuilding && (next.State == clusterstate.BuildReady || next.State == clusterstate.BuildError) ||
		(current.State == clusterstate.BuildReady || current.State == clusterstate.BuildError && current.Projection != nil) &&
			next.State == clusterstate.BuildTombstone
	if !allowed {
		return errors.New("raftstore: illegal Build state transition")
	}
	if current.Starting != nil {
		if next.Failure != nil {
			if !buildFailureMatchesStarting(*current.Starting, *next.Failure) {
				return errors.New("raftstore: Build placement failure differs from committed STARTING intent")
			}
			return nil
		}
		if next.Projection == nil || current.Starting.Binding == nil ||
			!buildProjectionMatchesBinding(*next.Projection, *current.Starting.Binding, current.Starting.LastEventSeq) {
			return errors.New("raftstore: Build projection does not match committed Binding")
		}
	}
	if current.Projection != nil && next.Projection != nil &&
		(current.Projection.NodeID != next.Projection.NodeID || current.Projection.NodeEpoch != next.Projection.NodeEpoch ||
			current.Projection.BindingDigest != next.Projection.BindingDigest ||
			current.Projection.RegistryGeneration != next.Projection.RegistryGeneration ||
			current.Projection.LastEventSeq >= next.Projection.LastEventSeq) {
		return errors.New("raftstore: Build execution changed or event sequence regressed")
	}
	if next.Tombstone != nil && current.Projection != nil &&
		!reflect.DeepEqual(*current.Projection, next.Tombstone.Projection) {
		return errors.New("raftstore: Build tombstone identifies another execution")
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
	if current.SelectedCandidate != nil || current.Binding != nil ||
		len(current.DefinitivelyRejected) != len(current.CandidatePool) || current.LastEventSeq != 0 ||
		next.PlacementRound <= current.PlacementRound || next.PlacementRound != current.PlacementRound+1 ||
		!reflect.DeepEqual(current.Intent, next.Intent) || next.SelectedCandidate != nil || next.Binding != nil ||
		len(next.DefinitivelyRejected) != 0 || next.LastEventSeq != 0 {
		return errors.New("raftstore: next placement round requires an exhausted no-side-effect pool and a new SID")
	}
	return nil
}

func validateStartingCandidateProgress(
	currentSelected, nextSelected *uint32,
	currentBinding, nextBinding *clusterstate.ExecutionBindingIntent,
	currentRejected, nextRejected []uint32,
	currentEventSeq, nextEventSeq uint64,
) error {
	switch {
	case currentSelected == nil && nextSelected == nil:
		if !sameRejectedCandidates(currentRejected, nextRejected) || currentEventSeq != nextEventSeq {
			return errors.New("raftstore: unselected workflow changed without a candidate result")
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
		ready.BindingDigest == starting.Binding.BindingDigest && ready.LastEventSeq > starting.LastEventSeq
}

func validateStartingTombstone(starting clusterstate.RouteStartingState, tombstone clusterstate.RouteTombstoneState) error {
	if tombstone.PlacementFailure != nil {
		failure := tombstone.PlacementFailure
		if starting.SelectedCandidate != nil || starting.Binding != nil ||
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
		tombstone.BindingDigest != starting.Binding.BindingDigest ||
		tombstone.LastEventSeq <= starting.LastEventSeq {
		return errors.New("raftstore: execution tombstone differs from committed STARTING Binding")
	}
	return nil
}

func sameReadyExecution(left, right clusterstate.ReadyRoute) bool {
	left.LastEventSeq, right.LastEventSeq = 0, 0
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
	lastEventSeq uint64,
) bool {
	return projection.BuildID != "" && projection.NodeID == binding.NodeID && projection.NodeEpoch == binding.NodeEpoch &&
		projection.RegistryGeneration == binding.RegistryGeneration && projection.BindingDigest == binding.BindingDigest &&
		projection.LastEventSeq > lastEventSeq
}

func sameBuildProjection(left, right clusterstate.BuildProjection) bool {
	left.LastEventSeq, right.LastEventSeq = 0, 0
	return reflect.DeepEqual(left, right)
}

func validateFenceCompaction(
	state DataState,
	fence clusterstate.ExecutionFence,
	authorization FenceCompactionAuthorization,
) error {
	if authorization.FenceRevision != fence.Revision.LogIndex ||
		authorization.TerminalProofDigest != fence.Proof.ProofDigest ||
		!isSHA256(authorization.RetentionProofDigest) {
		return errors.New("raftstore: fence compaction proof does not match the committed fence")
	}
	outboxCovered := authorization.FinalOutboxWatermarkAcked &&
		fence.FinalOutboxWatermark >= fence.LastEventSeq
	if !outboxCovered && !authorization.NodeEpochPermanentlyFenced {
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
	return route.State == clusterstate.WorkflowRouteTombstone && route.Tombstone != nil &&
		route.Tombstone.PlacementFailure == nil && route.Group == fence.Group && route.RouteKey == fence.RouteKey &&
		route.Tombstone.SandboxID == fence.SandboxID && route.Tombstone.NodeID == fence.NodeID &&
		!route.Tombstone.FenceCompacted && route.Tombstone.NodeEpoch == fence.NodeEpoch &&
		route.Tombstone.RegistryGeneration == fence.RegistryGeneration &&
		route.Tombstone.LastEventSeq == fence.LastEventSeq &&
		route.Tombstone.BindingDigest == fence.BindingDigest && route.Tombstone.Proof == fence.Proof
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
