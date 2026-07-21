package raftstore

import (
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

type DataLookup struct {
	Route         *routeapi.ReadRouteRequest `json:"route,omitempty"`
	Build         *routeapi.ReadBuildRequest `json:"build,omitempty"`
	Workflow      *WorkflowLookup            `json:"workflow,omitempty"`
	RouteBucket   *RouteBucketLookup         `json:"route_bucket,omitempty"`
	Changefeed    *RouteChangefeedLookup     `json:"changefeed,omitempty"`
	Pending       *PendingLookup             `json:"pending,omitempty"`
	LeaseBindings *LeaseBindingLookup        `json:"lease_bindings,omitempty"`
	Fence         *FenceLookup               `json:"fence,omitempty"`
	Recovery      *RecoveryLookup            `json:"recovery,omitempty"`
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
	if q.Workflow != nil {
		present++
		if err := q.Workflow.Validate(); err != nil {
			return err
		}
	}
	if q.RouteBucket != nil {
		present++
		if err := q.RouteBucket.Validate(); err != nil {
			return err
		}
	}
	if q.Changefeed != nil {
		present++
		if err := q.Changefeed.Validate(); err != nil {
			return err
		}
	}
	if q.Pending != nil {
		present++
		if err := q.Pending.Validate(); err != nil {
			return err
		}
	}
	if q.LeaseBindings != nil {
		present++
		if err := q.LeaseBindings.Validate(); err != nil {
			return err
		}
	}
	if q.Fence != nil {
		present++
		if err := q.Fence.Validate(); err != nil {
			return err
		}
	}
	if q.Recovery != nil {
		present++
		if err := q.Recovery.Validate(); err != nil {
			return err
		}
	}
	if present != 1 {
		return errors.New("raftstore: data lookup must contain exactly one query")
	}
	return nil
}

type DataLookupResult struct {
	Route         *routeapi.ReadRouteResponse `json:"route,omitempty"`
	Build         *routeapi.ReadBuildResponse `json:"build,omitempty"`
	Workflow      *WorkflowLookupResult       `json:"workflow,omitempty"`
	RouteBucket   *RouteBucketResult          `json:"route_bucket,omitempty"`
	Changefeed    *RouteChangefeedResult      `json:"changefeed,omitempty"`
	Pending       *PendingLookupResult        `json:"pending,omitempty"`
	LeaseBindings *LeaseBindingLookupResult   `json:"lease_bindings,omitempty"`
	Fence         *FenceLookupResult          `json:"fence,omitempty"`
	Recovery      *RecoveryLookupResult       `json:"recovery,omitempty"`
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
	case query.Workflow != nil:
		response := lookupWorkflow(state, *query.Workflow)
		return DataLookupResult{Workflow: &response}, nil
	case query.RouteBucket != nil:
		response := lookupRouteBucket(state, *query.RouteBucket)
		return DataLookupResult{RouteBucket: &response}, nil
	case query.Changefeed != nil:
		response := lookupRouteChangefeed(state, *query.Changefeed)
		return DataLookupResult{Changefeed: &response}, nil
	case query.Fence != nil:
		response := lookupFence(state, *query.Fence)
		return DataLookupResult{Fence: &response}, nil
	case query.Recovery != nil:
		response := lookupRecovery(state, *query.Recovery)
		return DataLookupResult{Recovery: &response}, nil
	case query.Pending != nil:
		response := lookupPending(state, *query.Pending)
		return DataLookupResult{Pending: &response}, nil
	default:
		response, err := lookupLeaseBindings(state, *query.LeaseBindings)
		return DataLookupResult{LeaseBindings: &response}, err
	}
}

type RecoveryLookup struct {
	Identity ShardRequestIdentity `json:"identity"`
	NodeID   string               `json:"node_id,omitempty"`
	AfterKey string               `json:"after_key,omitempty"`
	Limit    uint32               `json:"limit"`
}

func (q RecoveryLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: recovery lookup limit must be between 1 and 4096")
	}
	return nil
}

type RecoveryLookupResult struct {
	Available bool                   `json:"available"`
	Reason    string                 `json:"reason,omitempty"`
	Records   []RecoveryObjectRecord `json:"records"`
	NextKey   string                 `json:"next_key,omitempty"`
}

func lookupRecovery(state DataState, query RecoveryLookup) RecoveryLookupResult {
	result := RecoveryLookupResult{Records: make([]RecoveryObjectRecord, 0)}
	if state.Recovery == nil || query.Identity.ShardID != state.ShardID ||
		query.Identity.PermitIdentity != state.Recovery.Target {
		result.Reason = "data shard has no matching open recovery epoch"
		return result
	}
	keys := make([]string, 0, len(state.RecoveryRecords))
	for key, record := range state.RecoveryRecords {
		if key > query.AfterKey && (query.NodeID == "" || record.NodeID == query.NodeID) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result.Available = true
	if len(keys) > int(query.Limit) {
		keys = keys[:query.Limit]
		result.NextKey = keys[len(keys)-1]
	}
	for _, key := range keys {
		result.Records = append(result.Records, cloneRecoveryRecord(state.RecoveryRecords[key]))
	}
	return result
}

// WorkflowLookup is the leader/coordinator view of one complete workflow.
// Router reads remain a separate API that exposes only safe projections.
type WorkflowLookup struct {
	Identity ShardRequestIdentity `json:"identity"`
	Group    string               `json:"group"`
	RouteKey string               `json:"route_key,omitempty"`
	BuildID  string               `json:"build_id,omitempty"`
}

func (q WorkflowLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Group == "" || (q.RouteKey == "") == (q.BuildID == "") {
		return errors.New("raftstore: workflow lookup requires a group and exactly one Route or Build key")
	}
	return nil
}

type WorkflowLookupResult struct {
	Available bool                              `json:"available"`
	Reason    string                            `json:"reason,omitempty"`
	Route     *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build     *clusterstate.BuildRecord         `json:"build,omitempty"`
}

func lookupWorkflow(state DataState, query WorkflowLookup) WorkflowLookupResult {
	if !state.Initialized || !state.Accepts(query.Identity) {
		return WorkflowLookupResult{Reason: "workflow shard identity is not available"}
	}
	if query.RouteKey != "" {
		_, shardID, err := clusterstate.RouteShardFor(
			query.Group, query.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
		)
		if err != nil || shardID != state.ShardID {
			return WorkflowLookupResult{Reason: "Route workflow targets another shard"}
		}
		record, found := state.Routes[routeMapKey(query.Group, query.RouteKey)]
		if !found {
			return WorkflowLookupResult{Available: true}
		}
		copy := cloneRouteRecord(record)
		return WorkflowLookupResult{Available: true, Route: &copy}
	}
	_, shardID, err := clusterstate.BuildShardFor(
		query.Group, query.BuildID, state.BuildBucketCount, state.VirtualShardCount,
	)
	if err != nil || shardID != state.ShardID {
		return WorkflowLookupResult{Reason: "Build workflow targets another shard"}
	}
	record, found := state.Builds[buildMapKey(query.Group, query.BuildID)]
	if !found {
		return WorkflowLookupResult{Available: true}
	}
	copy := cloneBuildRecord(record)
	return WorkflowLookupResult{Available: true, Build: &copy}
}

type LeaseBindingLookup struct {
	Identity ShardRequestIdentity `json:"identity"`
	AfterKey string               `json:"after_key,omitempty"`
	Limit    uint32               `json:"limit"`
}

func (q LeaseBindingLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: lease Binding lookup limit must be between 1 and 4096")
	}
	return nil
}

// LeaseBinding is the non-secret projection needed to refresh a node-local
// key lease. Provider-owned key material never enters consensus state.
type LeaseBinding struct {
	Key                    string `json:"key"`
	Group                  string `json:"group"`
	NodeID                 string `json:"node_id"`
	NodeEpoch              uint64 `json:"node_epoch"`
	DataEndpoint           string `json:"data_endpoint"`
	RegistryGeneration     string `json:"registry_generation"`
	AuthKeyFingerprint     string `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
}

type LeaseBindingLookupResult struct {
	Bindings []LeaseBinding `json:"bindings"`
	NextKey  string         `json:"next_key,omitempty"`
}

func lookupLeaseBindings(state DataState, query LeaseBindingLookup) (LeaseBindingLookupResult, error) {
	if !state.Accepts(query.Identity) {
		return LeaseBindingLookupResult{}, nil
	}
	bindings := make([]LeaseBinding, 0)
	for key, record := range state.Builds {
		qualified := "b" + key
		if qualified <= query.AfterKey {
			continue
		}
		binding, active, err := buildLeaseBinding(record)
		if err != nil {
			return LeaseBindingLookupResult{}, err
		}
		if active {
			binding.Key = qualified
			bindings = append(bindings, binding)
		}
	}
	for key, record := range state.Routes {
		qualified := "r" + key
		if qualified <= query.AfterKey {
			continue
		}
		binding, active, err := routeLeaseBinding(record)
		if err != nil {
			return LeaseBindingLookupResult{}, err
		}
		if active {
			binding.Key = qualified
			bindings = append(bindings, binding)
		}
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Key < bindings[j].Key })
	hasMore := len(bindings) > int(query.Limit)
	if hasMore {
		bindings = bindings[:query.Limit]
	}
	result := LeaseBindingLookupResult{Bindings: bindings}
	if hasMore {
		result.NextKey = bindings[len(bindings)-1].Key
	}
	return result, nil
}

func routeLeaseBinding(record clusterstate.RouteWorkflowRecord) (LeaseBinding, bool, error) {
	var (
		nodeID, dataEndpoint, registryGeneration string
		nodeEpoch                                uint64
		intent                                   clusterstate.DispatchIntent
	)
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		if record.Starting == nil || record.Starting.Binding == nil {
			return LeaseBinding{}, false, nil
		}
		binding := record.Starting.Binding
		nodeID, nodeEpoch, dataEndpoint = binding.NodeID, binding.NodeEpoch, binding.DataEndpoint
		registryGeneration, intent = binding.RegistryGeneration, record.Starting.Intent
	case clusterstate.WorkflowRouteReady:
		nodeID, nodeEpoch, dataEndpoint = record.Ready.NodeID, record.Ready.NodeEpoch, record.Ready.DataEndpoint
		registryGeneration, intent = record.Ready.RegistryGeneration, record.Ready.Intent
	case clusterstate.WorkflowRoutePaused:
		execution := record.Paused.Execution
		nodeID, nodeEpoch, dataEndpoint = execution.NodeID, execution.NodeEpoch, execution.DataEndpoint
		registryGeneration, intent = execution.RegistryGeneration, execution.Intent
	case clusterstate.WorkflowRouteResuming:
		execution := record.Resuming.Execution
		nodeID, nodeEpoch, dataEndpoint = execution.NodeID, execution.NodeEpoch, execution.DataEndpoint
		registryGeneration, intent = execution.RegistryGeneration, execution.Intent
	case clusterstate.WorkflowRouteDeleting:
		execution := record.Deleting.Execution
		nodeID, nodeEpoch, dataEndpoint = execution.NodeID, execution.NodeEpoch, execution.DataEndpoint
		registryGeneration, intent = execution.RegistryGeneration, execution.Intent
	default:
		return LeaseBinding{}, false, nil
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return LeaseBinding{}, false, err
	}
	return LeaseBinding{
		Group: record.Group, NodeID: nodeID, NodeEpoch: nodeEpoch, DataEndpoint: dataEndpoint,
		RegistryGeneration: registryGeneration, AuthKeyFingerprint: spec.AuthKeyFingerprint,
		ManifestKeyFingerprint: spec.ManifestKeyFingerprint,
	}, true, nil
}

func buildLeaseBinding(record clusterstate.BuildRecord) (LeaseBinding, bool, error) {
	var (
		nodeID, dataEndpoint, registryGeneration string
		nodeEpoch                                uint64
		intent                                   clusterstate.DispatchIntent
	)
	switch record.State {
	case clusterstate.BuildStarting:
		if record.Starting == nil || record.Starting.Binding == nil {
			return LeaseBinding{}, false, nil
		}
		binding := record.Starting.Binding
		nodeID, nodeEpoch, dataEndpoint = binding.NodeID, binding.NodeEpoch, binding.DataEndpoint
		registryGeneration, intent = binding.RegistryGeneration, record.Starting.Intent
	case clusterstate.BuildRegistered:
		projection := record.Projection
		nodeID, nodeEpoch, dataEndpoint = projection.NodeID, projection.NodeEpoch, projection.DataEndpoint
		registryGeneration, intent = projection.RegistryGeneration, projection.Intent
	default:
		return LeaseBinding{}, false, nil
	}
	spec, err := clusterstate.ParseBuildDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return LeaseBinding{}, false, err
	}
	return LeaseBinding{
		Group: record.Group, NodeID: nodeID, NodeEpoch: nodeEpoch, DataEndpoint: dataEndpoint,
		RegistryGeneration: registryGeneration, AuthKeyFingerprint: spec.AuthKeyFingerprint,
		ManifestKeyFingerprint: spec.ManifestKeyFingerprint,
	}, true, nil
}

type RouteBucketLookup struct {
	Identity      ShardRequestIdentity `json:"identity"`
	Group         string               `json:"group"`
	Bucket        uint32               `json:"bucket"`
	AfterRouteKey string               `json:"after_route_key,omitempty"`
	Limit         uint32               `json:"limit"`
	Strong        bool                 `json:"strong,omitempty"`
}

func (q RouteBucketLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Group == "" || q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: Route bucket lookup requires a group and a limit between 1 and 4096")
	}
	return nil
}

type RouteBucketEntry struct {
	RouteKey    string                          `json:"route_key"`
	State       clusterstate.RouteWorkflowState `json:"state"`
	NodeID      string                          `json:"node_id"`
	TemplateRef string                          `json:"template_ref"`
}

type RouteBucketResult struct {
	Available        bool               `json:"available"`
	Reason           string             `json:"reason,omitempty"`
	Group            string             `json:"group"`
	Bucket           uint32             `json:"bucket"`
	SnapshotRevision uint64             `json:"snapshot_revision"`
	Routes           []RouteBucketEntry `json:"routes"`
	NextRouteKey     string             `json:"next_route_key,omitempty"`
}

func lookupRouteBucket(state DataState, query RouteBucketLookup) RouteBucketResult {
	result := RouteBucketResult{Group: query.Group, Bucket: query.Bucket}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result
	}
	result.Available = true
	result.SnapshotRevision = state.LastApplied
	result.Routes = make([]RouteBucketEntry, 0)
	for _, record := range state.Routes {
		if record.Group != query.Group || record.RouteKey <= query.AfterRouteKey {
			continue
		}
		bucket, _, err := clusterstate.RouteShardFor(
			record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
		)
		if err == nil && bucket == query.Bucket {
			if entry, listed := routeBucketEntry(record); listed {
				result.Routes = append(result.Routes, entry)
			}
		}
	}
	sort.Slice(result.Routes, func(left, right int) bool {
		return result.Routes[left].RouteKey < result.Routes[right].RouteKey
	})
	if len(result.Routes) > int(query.Limit) {
		result.Routes = result.Routes[:query.Limit]
		result.NextRouteKey = result.Routes[len(result.Routes)-1].RouteKey
	}
	return result
}

func routeBucketEntry(record clusterstate.RouteWorkflowRecord) (RouteBucketEntry, bool) {
	var execution *clusterstate.ReadyRoute
	switch record.State {
	case clusterstate.WorkflowRouteReady:
		execution = record.Ready
	case clusterstate.WorkflowRoutePaused:
		if record.Paused != nil {
			execution = &record.Paused.Execution
		}
	default:
		return RouteBucketEntry{}, false
	}
	if execution == nil {
		return RouteBucketEntry{}, false
	}
	return RouteBucketEntry{
		RouteKey: record.RouteKey, State: record.State,
		NodeID: execution.NodeID, TemplateRef: execution.TemplateRef,
	}, true
}

type RouteChangefeedLookup struct {
	Identity      ShardRequestIdentity `json:"identity"`
	Group         string               `json:"group"`
	Bucket        uint32               `json:"bucket"`
	AfterRevision uint64               `json:"after_revision"`
	Limit         uint32               `json:"limit"`
	Strong        bool                 `json:"strong,omitempty"`
}

func (q RouteChangefeedLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Group == "" {
		return errors.New("raftstore: Route changefeed requires a group")
	}
	if q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: Route changefeed limit must be between 1 and 4096")
	}
	return nil
}

type RouteChangefeedResult struct {
	Available      bool          `json:"available"`
	Reset          bool          `json:"reset"`
	Reason         string        `json:"reason,omitempty"`
	FloorRevision  uint64        `json:"floor_revision"`
	HeadRevision   uint64        `json:"head_revision"`
	CursorRevision uint64        `json:"cursor_revision"`
	Changes        []RouteChange `json:"changes"`
}

func lookupRouteChangefeed(state DataState, query RouteChangefeedLookup) RouteChangefeedResult {
	result := RouteChangefeedResult{
		FloorRevision:  state.RouteChangefeedFloor,
		HeadRevision:   state.LastApplied,
		CursorRevision: query.AfterRevision,
		Changes:        make([]RouteChange, 0),
	}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result
	}
	if query.AfterRevision > state.LastApplied {
		result.Reason = "Route changefeed position is ahead of local applied state"
		return result
	}
	result.Available = true
	if query.AfterRevision < state.RouteChangefeedFloor {
		result.Reset = true
		result.CursorRevision = state.LastApplied
		return result
	}

	scanLimit := int(query.Limit) * 16
	if scanLimit < 256 {
		scanLimit = 256
	}
	if scanLimit > 16_384 {
		scanLimit = 16_384
	}
	exhausted := true
	scanned := 0
	for _, change := range state.RouteChanges {
		if change.Revision <= query.AfterRevision {
			continue
		}
		if scanned == scanLimit || len(result.Changes) == int(query.Limit) {
			exhausted = false
			break
		}
		scanned++
		result.CursorRevision = change.Revision
		if change.Bucket != query.Bucket || change.Group != query.Group {
			continue
		}
		result.Changes = append(result.Changes, change)
	}
	if exhausted {
		result.CursorRevision = state.LastApplied
	}
	return result
}

func routeBucketTargetsShard(state DataState, group string, bucket, shardID uint32) bool {
	if state.VirtualShardCount == 0 || bucket >= state.RouteBucketCount || shardID != state.ShardID {
		return false
	}
	hash, err := clusterstate.RouteShardHash(group, bucket)
	return err == nil && uint32(hash%uint64(state.VirtualShardCount)) == state.ShardID
}

type FenceLookup struct {
	Identity  ShardRequestIdentity `json:"identity"`
	Group     string               `json:"group"`
	RouteKey  string               `json:"route_key"`
	SandboxID string               `json:"sandbox_id"`
}

func (q FenceLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Group == "" || q.RouteKey == "" || q.SandboxID == "" {
		return errors.New("raftstore: execution-fence lookup identity is incomplete")
	}
	return nil
}

type FenceLookupResult struct {
	Fence *clusterstate.ExecutionFence `json:"fence,omitempty"`
}

func lookupFence(state DataState, query FenceLookup) FenceLookupResult {
	if !state.Accepts(query.Identity) {
		return FenceLookupResult{}
	}
	_, shardID, err := clusterstate.RouteShardFor(
		query.Group, query.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
	)
	if err != nil || shardID != state.ShardID || query.Identity.ShardID != state.ShardID {
		return FenceLookupResult{}
	}
	fence, found := state.Fences[fenceMapKey(query.Group, query.RouteKey, query.SandboxID)]
	if !found {
		return FenceLookupResult{}
	}
	copy := fence
	return FenceLookupResult{Fence: &copy}
}

func lookupRoute(state DataState, request routeapi.ReadRouteRequest) routeapi.ReadRouteResponse {
	if !state.Initialized {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadUnavailable, Reason: "Route shard is not initialized"}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: "Route replica is not initialized"}
	}
	if !state.Accepts(shardIdentityFromRoute(request.RequestIdentity)) {
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadUnavailable, Reason: "Route request Registry History Generation or system epoch is fenced"}
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
	if record.State == clusterstate.WorkflowRoutePaused && record.Paused != nil && request.Strong && request.Addressable {
		paused := record.Paused.Execution
		return routeapi.ReadRouteResponse{
			Outcome: routeapi.ReadReady, Group: record.Group, RouteKey: record.RouteKey,
			State: clusterstate.WorkflowRoutePaused,
			Route: &paused, RouteRevision: record.Revision.LogIndex,
		}
	}
	if record.State != clusterstate.WorkflowRouteReady || record.Ready == nil {
		if request.Strong {
			return routeapi.ReadRouteResponse{Outcome: routeapi.ReadConflict, Reason: string(record.State)}
		}
		return routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, Reason: string(record.State)}
	}
	ready := *record.Ready
	return routeapi.ReadRouteResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, RouteKey: record.RouteKey,
		State: clusterstate.WorkflowRouteReady, Route: &ready, RouteRevision: record.Revision.LogIndex,
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
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadUnavailable, Reason: "Build request Registry History Generation or system epoch is fenced"}
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
	if record.State != clusterstate.BuildRegistered || record.Projection == nil {
		if request.Strong {
			if record.State == clusterstate.BuildStarting && record.Starting != nil {
				spec, err := clusterstate.ParseBuildDispatchSpec(record.Starting.Intent.DispatchSpec)
				if err != nil {
					return routeapi.ReadBuildResponse{Outcome: routeapi.ReadUnavailable, Reason: "BUILD_STARTING dispatch intent is invalid"}
				}
				return routeapi.ReadBuildResponse{
					Outcome: routeapi.ReadConflict, Group: record.Group,
					Pending: &routeapi.PendingBuildProjection{
						BuildID: record.BuildID, TemplateRef: spec.TemplateID, Profile: spec.Profile,
					},
					BuildState: clusterstate.BuildStarting, BuildRevision: record.Revision.LogIndex,
					Reason: string(record.State),
				}
			}
			return routeapi.ReadBuildResponse{Outcome: routeapi.ReadConflict, Reason: string(record.State)}
		}
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNeedLeader, Reason: string(record.State)}
	}
	copy := *record.Projection
	return routeapi.ReadBuildResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, Build: &copy,
		BuildState: clusterstate.BuildRegistered, BuildRevision: record.Revision.LogIndex,
	}
}

func shardIdentityFromRoute(identity routeapi.RequestIdentity) ShardRequestIdentity {
	return ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
		SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
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
	Fence *clusterstate.ExecutionFence      `json:"fence,omitempty"`
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
		if qualified > query.AfterKey && routeNeedsCoordinator(record) {
			copy := cloneRouteRecord(record)
			workflows = append(workflows, PendingWorkflow{Key: qualified, Route: &copy})
		}
	}
	for key, record := range state.Builds {
		qualified := "b" + key
		if qualified > query.AfterKey && buildNeedsCoordinator(record) {
			copy := cloneBuildRecord(record)
			workflows = append(workflows, PendingWorkflow{Key: qualified, Build: &copy})
		}
	}
	for key, fence := range state.Fences {
		qualified := "f" + key
		if qualified > query.AfterKey {
			copy := cloneExecutionFence(fence)
			workflows = append(workflows, PendingWorkflow{Key: qualified, Fence: &copy})
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

func routeNeedsCoordinator(record clusterstate.RouteWorkflowRecord) bool {
	return record.State == clusterstate.WorkflowRouteStarting || record.State == clusterstate.WorkflowRouteResuming ||
		record.State == clusterstate.WorkflowRouteDeleting || record.State == clusterstate.WorkflowRouteTombstone ||
		len(record.Finalizations) != 0
}

func buildNeedsCoordinator(record clusterstate.BuildRecord) bool {
	return record.State == clusterstate.BuildStarting || len(record.Finalizations) != 0
}
