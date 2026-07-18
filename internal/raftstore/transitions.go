package raftstore

import (
	"errors"
	"reflect"

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
		if current.Starting.SandboxID != next.Starting.SandboxID ||
			current.Starting.PlacementRound != next.Starting.PlacementRound ||
			!reflect.DeepEqual(current.Starting.CandidatePool, next.Starting.CandidatePool) ||
			!reflect.DeepEqual(current.Starting.Intent, next.Starting.Intent) ||
			!indexSetContains(next.Starting.DefinitivelyRejected, current.Starting.DefinitivelyRejected) {
			return errors.New("raftstore: STARTING immutable intent changed or rejection proof regressed")
		}
		if current.Starting.SelectedCandidate != nil &&
			(next.Starting.SelectedCandidate == nil || *current.Starting.SelectedCandidate != *next.Starting.SelectedCandidate) {
			return errors.New("raftstore: STARTING selected candidate changed")
		}
		if current.Starting.Binding != nil && !reflect.DeepEqual(current.Starting.Binding, next.Starting.Binding) {
			return errors.New("raftstore: STARTING committed Binding changed")
		}
		if current.Starting.LastEventSeq > next.Starting.LastEventSeq {
			return errors.New("raftstore: STARTING event sequence regressed")
		}
		return nil
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
				!reflect.DeepEqual(current.Starting.Intent, next.Starting.Intent) ||
				!indexSetContains(next.Starting.DefinitivelyRejected, current.Starting.DefinitivelyRejected) ||
				current.Starting.LastEventSeq > next.Starting.LastEventSeq {
				return errors.New("raftstore: BUILD_STARTING intent changed or proof regressed")
			}
			if current.Starting.SelectedCandidate != nil &&
				(next.Starting.SelectedCandidate == nil || *current.Starting.SelectedCandidate != *next.Starting.SelectedCandidate) {
				return errors.New("raftstore: BUILD_STARTING selected candidate changed")
			}
			if current.Starting.Binding != nil && !reflect.DeepEqual(current.Starting.Binding, next.Starting.Binding) {
				return errors.New("raftstore: BUILD_STARTING committed Binding changed")
			}
			return nil
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
			current.Projection.StorageGeneration != next.Projection.StorageGeneration ||
			current.Projection.LastEventSeq >= next.Projection.LastEventSeq) {
		return errors.New("raftstore: Build execution changed or event sequence regressed")
	}
	if next.Tombstone != nil && current.Projection != nil &&
		!reflect.DeepEqual(*current.Projection, next.Tombstone.Projection) {
		return errors.New("raftstore: Build tombstone identifies another execution")
	}
	return nil
}

func readyMatchesStarting(starting clusterstate.RouteStartingState, ready clusterstate.ReadyRoute) bool {
	if starting.Binding == nil {
		return false
	}
	return ready.SandboxID == starting.SandboxID && ready.NodeID == starting.Binding.NodeID &&
		ready.NodeEpoch == starting.Binding.NodeEpoch && ready.DataEndpoint == starting.Binding.DataEndpoint &&
		ready.StorageGeneration == starting.Binding.StorageGeneration &&
		ready.BindingDigest == starting.Binding.BindingDigest && ready.LastEventSeq > starting.LastEventSeq
}

func validateStartingTombstone(starting clusterstate.RouteStartingState, tombstone clusterstate.RouteTombstoneState) error {
	if tombstone.PlacementFailure != nil {
		failure := tombstone.PlacementFailure
		if failure.SandboxID != starting.SandboxID || failure.PlacementRound != starting.PlacementRound ||
			!reflect.DeepEqual(failure.CandidatePool, starting.CandidatePool) ||
			!reflect.DeepEqual(failure.Intent, starting.Intent) ||
			!indexSetContains(failure.DefinitivelyRejected, starting.DefinitivelyRejected) {
			return errors.New("raftstore: placement failure differs from committed STARTING intent")
		}
		return nil
	}
	if starting.Binding == nil || tombstone.SandboxID != starting.SandboxID ||
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
	return failure.BuildID == starting.BuildID && reflect.DeepEqual(failure.CandidatePool, starting.CandidatePool) &&
		reflect.DeepEqual(failure.Intent, starting.Intent) &&
		indexSetContains(failure.DefinitivelyRejected, starting.DefinitivelyRejected)
}

func buildProjectionMatchesBinding(
	projection clusterstate.BuildProjection,
	binding clusterstate.ExecutionBindingIntent,
	lastEventSeq uint64,
) bool {
	return projection.BuildID != "" && projection.NodeID == binding.NodeID && projection.NodeEpoch == binding.NodeEpoch &&
		projection.StorageGeneration == binding.StorageGeneration && projection.BindingDigest == binding.BindingDigest &&
		projection.LastEventSeq > lastEventSeq
}

func sameBuildProjection(left, right clusterstate.BuildProjection) bool {
	left.LastEventSeq, right.LastEventSeq = 0, 0
	return reflect.DeepEqual(left, right)
}

func indexSetContains(superset, subset []uint32) bool {
	values := make(map[uint32]struct{}, len(superset))
	for _, value := range superset {
		values[value] = struct{}{}
	}
	for _, value := range subset {
		if _, found := values[value]; !found {
			return false
		}
	}
	return true
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
	if len(authorization.ReplicaApplied) != len(state.ReplicaIDs) {
		return errors.New("raftstore: fence compaction lacks every replica watermark")
	}
	for i, applied := range authorization.ReplicaApplied {
		if applied.ReplicaID != state.ReplicaIDs[i] || applied.AppliedIndex <= fence.Revision.LogIndex {
			return errors.New("raftstore: a replica has not applied beyond the fence revision")
		}
	}
	return nil
}

func fenceMatchesRouteTombstone(fence clusterstate.ExecutionFence, route clusterstate.RouteWorkflowRecord) bool {
	return route.State == clusterstate.WorkflowRouteTombstone && route.Tombstone != nil &&
		route.Tombstone.PlacementFailure == nil && route.Group == fence.Group && route.RouteKey == fence.RouteKey &&
		route.Tombstone.SandboxID == fence.SandboxID && route.Tombstone.NodeID == fence.NodeID &&
		route.Tombstone.NodeEpoch == fence.NodeEpoch && route.Tombstone.LastEventSeq == fence.LastEventSeq &&
		route.Tombstone.BindingDigest == fence.BindingDigest && route.Tombstone.Proof == fence.Proof
}

func validateFenceTransition(current, next clusterstate.ExecutionFence) error {
	currentRevision, nextRevision := current.Revision, next.Revision
	currentWatermark, nextWatermark := current.FinalOutboxWatermark, next.FinalOutboxWatermark
	current.Revision, next.Revision = clusterstate.Revision{}, clusterstate.Revision{}
	current.FinalOutboxWatermark, next.FinalOutboxWatermark = 0, 0
	if !reflect.DeepEqual(current, next) || nextWatermark < currentWatermark ||
		nextRevision.StorageGeneration != currentRevision.StorageGeneration || nextRevision.ShardID != currentRevision.ShardID {
		return errors.New("raftstore: execution fence identity or proof changed")
	}
	return nil
}
