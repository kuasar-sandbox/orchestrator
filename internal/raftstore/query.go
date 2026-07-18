package raftstore

import (
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

type DataLookup struct {
	Route   *routeapi.ReadRouteRequest `json:"route,omitempty"`
	Build   *routeapi.ReadBuildRequest `json:"build,omitempty"`
	Pending *PendingLookup             `json:"pending,omitempty"`
}

func (q DataLookup) Validate() error {
	present := 0
	if q.Route != nil {
		present++
		if err := q.Route.Validate(); err != nil {
			return err
		}
	}
	if q.Build != nil {
		present++
		if err := q.Build.Validate(); err != nil {
			return err
		}
	}
	if q.Pending != nil {
		present++
		if err := q.Pending.Validate(); err != nil {
			return err
		}
	}
	if present != 1 {
		return errors.New("raftstore: data lookup must contain exactly one query")
	}
	return nil
}

type DataLookupResult struct {
	Route   *routeapi.ReadRouteResponse `json:"route,omitempty"`
	Build   *routeapi.ReadBuildResponse `json:"build,omitempty"`
	Pending *PendingLookupResult        `json:"pending,omitempty"`
}

func LookupData(state DataState, query DataLookup) (DataLookupResult, error) {
	if err := query.Validate(); err != nil {
		return DataLookupResult{}, err
	}
	switch {
	case query.Route != nil:
		response := lookupRoute(state, *query.Route)
		return DataLookupResult{Route: &response}, nil
	case query.Build != nil:
		response := lookupBuild(state, *query.Build)
		return DataLookupResult{Build: &response}, nil
	default:
		response := lookupPending(state, *query.Pending)
		return DataLookupResult{Pending: &response}, nil
	}
}

func lookupRoute(state DataState, request routeapi.ReadRouteRequest) routeapi.ReadRouteResponse {
	if !state.Initialized {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadUnavailable, Reason: "Route shard is not initialized"}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: "Route replica is not initialized"}
	}
	if !state.Accepts(shardIdentityFromRoute(request.RequestIdentity)) {
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadUnavailable, Reason: "Route request generation or epoch is fenced"}
	}
	_, shardID, err := clusterstate.RouteShardFor(request.Group, request.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
	if err != nil || shardID != state.ShardID || request.ShardID != state.ShardID {
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadConflict, Reason: "Route request targets another shard"}
	}
	record, found := state.Routes[routeMapKey(request.Group, request.RouteKey)]
	if !found {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNotFound}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: "Route is absent from local applied state"}
	}
	if record.Revision.LogIndex < request.MinRouteRevision {
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadReplicaBehind, Reason: "Route revision is below the requested minimum"}
	}
	if record.State != clusterstate.WorkflowRouteReady || record.Ready == nil {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadConflict, Reason: string(record.State)}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: string(record.State)}
	}
	if request.SandboxID != "" && request.SandboxID != record.Ready.SandboxID {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadConflict, Reason: "Route is bound to another sandbox ID"}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: "local Route sandbox ID mismatch"}
	}
	ready := *record.Ready
	return routeapi.ReadRouteResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, RouteKey: record.RouteKey,
		Route: &ready, RouteRevision: record.Revision.LogIndex,
	}
}

func lookupBuild(state DataState, request routeapi.ReadBuildRequest) routeapi.ReadBuildResponse {
	if !state.Initialized {
		if request.Strong {
			return routeapi.ReadBuildResponse{Outcome: routeapi.ReadUnavailable, Reason: "Build shard is not initialized"}
		}
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNeedLeader, Reason: "Build replica is not initialized"}
	}
	if !state.Accepts(shardIdentityFromRoute(request.RequestIdentity)) {
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadUnavailable, Reason: "Build request generation or epoch is fenced"}
	}
	_, shardID, err := clusterstate.BuildShardFor(request.Group, request.BuildID, state.BuildBucketCount, state.VirtualShardCount)
	if err != nil || shardID != state.ShardID || request.ShardID != state.ShardID {
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadConflict, Reason: "Build request targets another shard"}
	}
	record, found := state.Builds[buildMapKey(request.Group, request.BuildID)]
	if !found {
		if request.Strong {
			return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNotFound}
		}
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNeedLeader, Reason: "Build is absent from local applied state"}
	}
	if record.Revision.LogIndex < request.MinBuildRevision {
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadReplicaBehind, Reason: "Build revision is below the requested minimum"}
	}
	if !positiveBuildState(record.State) || record.Projection == nil {
		if request.Strong {
			return routeapi.ReadBuildResponse{Outcome: routeapi.ReadConflict, Reason: string(record.State)}
		}
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNeedLeader, Reason: string(record.State)}
	}
	projection := *record.Projection
	return routeapi.ReadBuildResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, Build: &projection,
		BuildState: record.State, BuildRevision: record.Revision.LogIndex,
	}
}

func positiveBuildState(state clusterstate.BuildWorkflowState) bool {
	switch state {
	case clusterstate.BuildQueued, clusterstate.BuildRegistered, clusterstate.BuildBuilding,
		clusterstate.BuildReady, clusterstate.BuildError:
		return true
	default:
		return false
	}
}

func shardIdentityFromRoute(identity routeapi.RequestIdentity) ShardRequestIdentity {
	return ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: identity.ClusterID, StorageGeneration: identity.StorageGeneration,
		SystemEpoch: identity.SystemEpoch, ManifestDigest: identity.ManifestDigest,
	}, ShardID: identity.ShardID}
}

type PendingLookup struct {
	Identity ShardRequestIdentity `json:"identity"`
	AfterKey string               `json:"after_key,omitempty"`
	Limit    uint32               `json:"limit"`
}

func (q PendingLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: pending lookup limit must be between 1 and 4096")
	}
	return nil
}

type PendingWorkflow struct {
	Key   string                            `json:"key"`
	Route *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build *clusterstate.BuildRecord         `json:"build,omitempty"`
}

type PendingLookupResult struct {
	Workflows []PendingWorkflow `json:"workflows"`
	NextKey   string            `json:"next_key,omitempty"`
}

func lookupPending(state DataState, query PendingLookup) PendingLookupResult {
	if !state.Accepts(query.Identity) {
		return PendingLookupResult{}
	}
	workflows := make([]PendingWorkflow, 0)
	for key, record := range state.Routes {
		qualified := "r" + key
		if qualified > query.AfterKey && routeNeedsCoordinator(record.State) {
			copy := cloneRouteRecord(record)
			workflows = append(workflows, PendingWorkflow{Key: qualified, Route: &copy})
		}
	}
	for key, record := range state.Builds {
		qualified := "b" + key
		if qualified > query.AfterKey && buildNeedsCoordinator(record.State) {
			copy := cloneBuildRecord(record)
			workflows = append(workflows, PendingWorkflow{Key: qualified, Build: &copy})
		}
	}
	sort.Slice(workflows, func(i, j int) bool { return workflows[i].Key < workflows[j].Key })
	hasMore := len(workflows) > int(query.Limit)
	if hasMore {
		workflows = workflows[:query.Limit]
	}
	result := PendingLookupResult{Workflows: workflows}
	if hasMore {
		result.NextKey = workflows[len(workflows)-1].Key
	}
	return result
}

func routeNeedsCoordinator(state clusterstate.RouteWorkflowState) bool {
	return state == clusterstate.WorkflowRouteStarting || state == clusterstate.WorkflowRouteResuming ||
		state == clusterstate.WorkflowRouteDeleting
}

func buildNeedsCoordinator(state clusterstate.BuildWorkflowState) bool {
	return state == clusterstate.BuildStarting || state == clusterstate.BuildQueued ||
		state == clusterstate.BuildRegistered || state == clusterstate.BuildBuilding
}
