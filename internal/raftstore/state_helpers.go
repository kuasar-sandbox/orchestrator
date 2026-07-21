package raftstore

import (
	"encoding/json"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type SystemStateLookup struct{}
type DataStateLookup struct{}

func encodeApplyResult(applied bool, result any) (sm.Result, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return sm.Result{}, err
	}
	value := uint64(0)
	if applied {
		value = 1
	}
	return sm.Result{Value: value, Data: raw}, nil
}

func cloneSystemState(state SystemState) SystemState {
	clone := state
	if state.Transition != nil {
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		clone.Transition = &transition
	}
	if state.Recovery != nil {
		recovery := *state.Recovery
		clone.Recovery = &recovery
	}
	if state.Closure != nil {
		closure := *state.Closure
		clone.Closure = &closure
	}
	return clone
}

func cloneDataStateForLookup(state DataState) DataState {
	clone := state
	clone.ReplicaIDs = append([]uint64(nil), state.ReplicaIDs...)
	clone.PreparedReplicaIDs = append([]uint64(nil), state.PreparedReplicaIDs...)
	clone.ServingEpochs = append([]PermitIdentity(nil), state.ServingEpochs...)
	clone.RouteChanges = append([]RouteChange(nil), state.RouteChanges...)
	clone.Routes = make(map[string]clusterstate.RouteWorkflowRecord, len(state.Routes))
	for key, record := range state.Routes {
		clone.Routes[key] = cloneRouteRecord(record)
	}
	clone.Builds = make(map[string]clusterstate.BuildRecord, len(state.Builds))
	for key, record := range state.Builds {
		clone.Builds[key] = cloneBuildRecord(record)
	}
	clone.Fences = make(map[string]clusterstate.ExecutionFence, len(state.Fences))
	for key, fence := range state.Fences {
		clone.Fences[key] = fence
	}
	return clone
}
