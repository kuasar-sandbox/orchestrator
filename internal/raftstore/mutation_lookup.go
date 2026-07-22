package raftstore

import (
	"errors"
	"reflect"
)

// DataMutationLookup resolves an ambiguous proposal without introducing a
// second mutation identity. It compares the exact intended row against the
// strongly-read committed state and reports the committed log revision.
type DataMutationLookup struct {
	Command DataCommand
}

type DataMutationStatus struct {
	Committed bool
	Revision  uint64
}

func (q DataMutationLookup) Validate() error {
	if err := q.Command.Identity.Validate(); err != nil {
		return err
	}
	switch q.Command.Type {
	case DataPutRoute:
		if q.Command.Route == nil {
			return errors.New("raftstore: Route mutation lookup requires a Route")
		}
	case DataPutBuild:
		if q.Command.Build == nil {
			return errors.New("raftstore: Build mutation lookup requires a Build")
		}
	case DataPutFence:
		if q.Command.Fence == nil {
			return errors.New("raftstore: fence mutation lookup requires a fence")
		}
	default:
		return errors.New("raftstore: mutation lookup supports only public row mutations")
	}
	return nil
}

func LookupDataMutation(state DataState, query DataMutationLookup) (DataMutationStatus, error) {
	if err := query.Validate(); err != nil {
		return DataMutationStatus{}, err
	}
	if !state.Accepts(query.Command.Identity) {
		return DataMutationStatus{}, nil
	}
	command := query.Command
	switch command.Type {
	case DataPutRoute:
		current, found := state.Routes[routeMapKey(command.Route.Group, command.Route.RouteKey)]
		if !found || !mutationRevisionAdvanced(command.Expect, current.Revision.LogIndex) {
			return DataMutationStatus{}, nil
		}
		wanted := cloneRouteRecord(*command.Route)
		if wanted.Finalizations == nil {
			wanted.Finalizations = cloneWorkflowFinalizations(current.Finalizations)
		}
		wanted.Revision = current.Revision
		normalizeRouteRevision(&wanted)
		if current.State == wanted.State && current.Tombstone != nil && wanted.Tombstone != nil &&
			current.Tombstone.FenceCompacted {
			wanted.Tombstone.FenceCompacted = true
		}
		return DataMutationStatus{Committed: reflect.DeepEqual(current, wanted), Revision: current.Revision.LogIndex}, nil
	case DataPutBuild:
		current, found := state.Builds[buildMapKey(command.Build.Group, command.Build.BuildID)]
		if !found || !mutationRevisionAdvanced(command.Expect, current.Revision.LogIndex) {
			return DataMutationStatus{}, nil
		}
		wanted := cloneBuildRecord(*command.Build)
		if wanted.Finalizations == nil {
			wanted.Finalizations = cloneWorkflowFinalizations(current.Finalizations)
		}
		wanted.Revision = current.Revision
		return DataMutationStatus{Committed: reflect.DeepEqual(current, wanted), Revision: current.Revision.LogIndex}, nil
	default:
		current, found := state.Fences[fenceMapKey(command.Fence.Group, command.Fence.RouteKey, command.Fence.SandboxID)]
		if !found || !mutationRevisionAdvanced(command.Expect, current.Revision.LogIndex) {
			return DataMutationStatus{}, nil
		}
		wanted := *command.Fence
		wanted.Revision = current.Revision
		return DataMutationStatus{Committed: current == wanted, Revision: current.Revision.LogIndex}, nil
	}
}

func mutationRevisionAdvanced(expect RevisionExpectation, revision uint64) bool {
	if expect.Absent {
		return expect.LogIndex == 0 && revision != 0
	}
	return expect.LogIndex != 0 && revision > expect.LogIndex
}
