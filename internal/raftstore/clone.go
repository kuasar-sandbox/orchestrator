package raftstore

import clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"

func cloneRouteRecord(source clusterstate.RouteWorkflowRecord) clusterstate.RouteWorkflowRecord {
	clone := source
	if source.Starting != nil {
		starting := *source.Starting
		cloneStartingFields(&starting)
		clone.Starting = &starting
	}
	if source.Ready != nil {
		ready := *source.Ready
		clone.Ready = &ready
	}
	if source.Paused != nil {
		paused := *source.Paused
		paused.ResumeIntent = cloneDispatchIntent(source.Paused.ResumeIntent)
		clone.Paused = &paused
	}
	if source.Resuming != nil {
		resuming := *source.Resuming
		resuming.Intent = cloneDispatchIntent(source.Resuming.Intent)
		clone.Resuming = &resuming
	}
	if source.Deleting != nil {
		deleting := *source.Deleting
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
		clone.Tombstone = &tombstone
	}
	if source.Failure != nil {
		failure := *source.Failure
		failure.CandidatePool = append([]clusterstate.PlacementCandidate(nil), source.Failure.CandidatePool...)
		failure.DefinitivelyRejected = append([]uint32(nil), source.Failure.DefinitivelyRejected...)
		failure.Intent = cloneDispatchIntent(source.Failure.Intent)
		clone.Failure = &failure
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
