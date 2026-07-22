package raftstore

import clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"

func cloneRouteRecord(source clusterstate.RouteWorkflowRecord) clusterstate.RouteWorkflowRecord {
	clone := source
	clone.Finalizations = cloneWorkflowFinalizations(source.Finalizations)
	if source.Starting != nil {
		starting := *source.Starting
		cloneStartingFields(&starting)
		clone.Starting = &starting
	}
	if source.Ready != nil {
		ready := cloneReadyRoute(*source.Ready)
		clone.Ready = &ready
	}
	if source.Paused != nil {
		paused := *source.Paused
		paused.Execution = cloneReadyRoute(source.Paused.Execution)
		paused.ResumeIntent = cloneDispatchIntent(source.Paused.ResumeIntent)
		clone.Paused = &paused
	}
	if source.Resuming != nil {
		resuming := *source.Resuming
		resuming.Execution = cloneReadyRoute(source.Resuming.Execution)
		resuming.Intent = cloneDispatchIntent(source.Resuming.Intent)
		clone.Resuming = &resuming
	}
	if source.Deleting != nil {
		deleting := *source.Deleting
		deleting.Execution = cloneReadyRoute(source.Deleting.Execution)
		deleting.DeleteSpec = append([]byte(nil), source.Deleting.DeleteSpec...)
		clone.Deleting = &deleting
	}
	if source.Tombstone != nil {
		tombstone := *source.Tombstone
		if source.Tombstone.PlacementFailure != nil {
			failure := *source.Tombstone.PlacementFailure
			failure.CandidatePool = append([]clusterstate.PlacementCandidate(nil), failure.CandidatePool...)
			failure.DefinitivelyRejected = append([]uint32(nil), failure.DefinitivelyRejected...)
			failure.Intent = cloneDispatchIntent(failure.Intent)
			tombstone.PlacementFailure = &failure
		}
		clone.Tombstone = &tombstone
	}
	return clone
}

func cloneReadyRoute(source clusterstate.ReadyRoute) clusterstate.ReadyRoute {
	clone := source
	clone.Intent = cloneDispatchIntent(source.Intent)
	return clone
}

func cloneBuildRecord(source clusterstate.BuildRecord) clusterstate.BuildRecord {
	clone := source
	if source.Starting != nil {
		starting := *source.Starting
		starting.CandidatePool = append([]clusterstate.PlacementCandidate(nil), source.Starting.CandidatePool...)
		starting.DefinitivelyRejected = append([]uint32(nil), source.Starting.DefinitivelyRejected...)
		starting.Intent = cloneDispatchIntent(source.Starting.Intent)
		starting.SelectedCandidate = cloneUint32(source.Starting.SelectedCandidate)
		if source.Starting.Binding != nil {
			binding := *source.Starting.Binding
			starting.Binding = &binding
		}
		clone.Starting = &starting
	}
	if source.Projection != nil {
		projection := *source.Projection
		clone.Projection = &projection
	}
	if source.Tombstone != nil {
		tombstone := *source.Tombstone
		failure := source.Tombstone.PlacementFailure
		failure.CandidatePool = append([]clusterstate.PlacementCandidate(nil), failure.CandidatePool...)
		failure.DefinitivelyRejected = append([]uint32(nil), failure.DefinitivelyRejected...)
		failure.Intent = cloneDispatchIntent(failure.Intent)
		tombstone.PlacementFailure = failure
		clone.Tombstone = &tombstone
	}
	clone.Finalizations = cloneWorkflowFinalizations(source.Finalizations)
	return clone
}

func cloneWorkflowFinalizations(source []clusterstate.WorkflowFinalizationIntent) []clusterstate.WorkflowFinalizationIntent {
	if source == nil {
		return nil
	}
	clone := make([]clusterstate.WorkflowFinalizationIntent, len(source))
	copy(clone, source)
	for index := range source {
		if source[index].TerminalProof != nil {
			proof := *source[index].TerminalProof
			clone[index].TerminalProof = &proof
		}
	}
	return clone
}

func cloneStartingFields(starting *clusterstate.RouteStartingState) {
	starting.CandidatePool = append([]clusterstate.PlacementCandidate(nil), starting.CandidatePool...)
	starting.DefinitivelyRejected = append([]uint32(nil), starting.DefinitivelyRejected...)
	starting.Intent = cloneDispatchIntent(starting.Intent)
	starting.SelectedCandidate = cloneUint32(starting.SelectedCandidate)
	if starting.Binding != nil {
		binding := *starting.Binding
		starting.Binding = &binding
	}
}

func cloneDispatchIntent(source clusterstate.DispatchIntent) clusterstate.DispatchIntent {
	clone := source
	clone.NormalizedDemand = append([]byte(nil), source.NormalizedDemand...)
	clone.DispatchSpec = append([]byte(nil), source.DispatchSpec...)
	return clone
}

func cloneUint32(source *uint32) *uint32 {
	if source == nil {
		return nil
	}
	clone := *source
	return &clone
}

func cloneExecutionFence(source clusterstate.ExecutionFence) clusterstate.ExecutionFence {
	clone := source
	if source.PlacementFailure != nil {
		failure := *source.PlacementFailure
		failure.CandidatePool = append([]clusterstate.PlacementCandidate(nil), source.PlacementFailure.CandidatePool...)
		failure.DefinitivelyRejected = append([]uint32(nil), source.PlacementFailure.DefinitivelyRejected...)
		failure.Intent = cloneDispatchIntent(source.PlacementFailure.Intent)
		clone.PlacementFailure = &failure
	}
	return clone
}
