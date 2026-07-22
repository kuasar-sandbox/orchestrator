package raftstore

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

type DataLookup struct {
	Route       *routeapi.ReadRouteRequest `json:"route,omitempty"`
	Build       *routeapi.ReadBuildRequest `json:"build,omitempty"`
	Workflow    *WorkflowLookup            `json:"workflow,omitempty"`
	RouteBucket *RouteBucketLookup         `json:"route_bucket,omitempty"`
	Changefeed  *RouteChangefeedLookup     `json:"changefeed,omitempty"`
	Pending     *PendingLookup             `json:"pending,omitempty"`
	Fence       *FenceLookup               `json:"fence,omitempty"`
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
	if q.Fence != nil {
		present++
		if err := q.Fence.Validate(); err != nil {
			return err
		}
	}
	if present != 1 {
		return errors.New("raftstore: data lookup must contain exactly one query")
	}
	return nil
}

type DataLookupResult struct {
	Route       *routeapi.ReadRouteResponse `json:"route,omitempty"`
	Build       *routeapi.ReadBuildResponse `json:"build,omitempty"`
	Workflow    *WorkflowLookupResult       `json:"workflow,omitempty"`
	RouteBucket *RouteBucketResult          `json:"route_bucket,omitempty"`
	Changefeed  *RouteChangefeedResult      `json:"changefeed,omitempty"`
	Pending     *PendingLookupResult        `json:"pending,omitempty"`
	Fence       *FenceLookupResult          `json:"fence,omitempty"`
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
		response, err := lookupRouteChangefeed(state, *query.Changefeed)
		return DataLookupResult{Changefeed: &response}, err
	case query.Fence != nil:
		response := lookupFence(state, *query.Fence)
		return DataLookupResult{Fence: &response}, nil
	default:
		response, err := lookupPending(state, *query.Pending)
		return DataLookupResult{Pending: &response}, err
	}
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
	result.Routes = make([]RouteBucketEntry, 0, min(int(query.Limit)+1, len(state.Routes)))
	for _, record := range state.Routes {
		if record.Group != query.Group || record.RouteKey <= query.AfterRouteKey {
			continue
		}
		bucket, _, err := clusterstate.RouteShardFor(
			record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
		)
		if err == nil && bucket == query.Bucket {
			if entry, listed := routeBucketEntry(record); listed {
				addBoundedRouteBucketEntry(&result.Routes, entry, int(query.Limit)+1)
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

type routeBucketEntryHeap []RouteBucketEntry

func (h routeBucketEntryHeap) Len() int           { return len(h) }
func (h routeBucketEntryHeap) Less(i, j int) bool { return h[i].RouteKey > h[j].RouteKey }
func (h routeBucketEntryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *routeBucketEntryHeap) Push(value any)    { *h = append(*h, value.(RouteBucketEntry)) }
func (h *routeBucketEntryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func addBoundedRouteBucketEntry(entries *[]RouteBucketEntry, entry RouteBucketEntry, bound int) {
	if len(*entries) < bound {
		heap.Push((*routeBucketEntryHeap)(entries), entry)
		return
	}
	if bound == 0 || entry.RouteKey >= (*entries)[0].RouteKey {
		return
	}
	(*entries)[0] = entry
	heap.Fix((*routeBucketEntryHeap)(entries), 0)
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

const (
	MaxRouteChangefeedResponseBytes = 8 << 20
	maxRouteChangefeedEnvelopeBytes = len(`{"available":true,"reset":false,"floor_revision":18446744073709551615,"head_revision":18446744073709551615,"cursor_revision":18446744073709551615,"changes":[]}`)
)

type routeChangefeedPageBuilder struct {
	limit        int
	payloadBytes int
	changes      []RouteChange
}

func newRouteChangefeedPageBuilder(limit uint32) *routeChangefeedPageBuilder {
	return &routeChangefeedPageBuilder{limit: int(limit), changes: make([]RouteChange, 0, limit)}
}

func (b *routeChangefeedPageBuilder) add(change RouteChange) (bool, error) {
	encoded, err := json.Marshal(change)
	if err != nil {
		return false, fmt.Errorf("raftstore: encode Route changefeed row: %w", err)
	}
	payloadBytes := b.payloadBytes + len(encoded)
	if len(b.changes) != 0 {
		payloadBytes++
	}
	if len(b.changes) >= b.limit || maxRouteChangefeedEnvelopeBytes+payloadBytes > MaxRouteChangefeedResponseBytes {
		if len(b.changes) == 0 {
			return false, errors.New("raftstore: one Route changefeed row exceeds the response byte limit")
		}
		return false, nil
	}
	b.payloadBytes = payloadBytes
	b.changes = append(b.changes, change)
	return true, nil
}

func lookupRouteChangefeed(state DataState, query RouteChangefeedLookup) (RouteChangefeedResult, error) {
	result := RouteChangefeedResult{
		FloorRevision:  state.RouteChangefeedFloor,
		HeadRevision:   state.LastApplied,
		CursorRevision: query.AfterRevision,
		Changes:        make([]RouteChange, 0),
	}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result, nil
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result, nil
	}
	if query.AfterRevision > state.LastApplied {
		result.Reason = "Route changefeed position is ahead of local applied state"
		return result, nil
	}
	result.Available = true
	if query.AfterRevision < state.RouteChangefeedFloor {
		result.Reset = true
		result.CursorRevision = state.LastApplied
		return result, nil
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
	builder := newRouteChangefeedPageBuilder(query.Limit)
	for _, change := range state.RouteChanges {
		if change.Revision <= query.AfterRevision {
			continue
		}
		if scanned == scanLimit {
			exhausted = false
			break
		}
		scanned++
		if change.Bucket != query.Bucket || change.Group != query.Group {
			result.CursorRevision = change.Revision
			continue
		}
		added, err := builder.add(change)
		if err != nil {
			return RouteChangefeedResult{}, err
		}
		if !added {
			exhausted = false
			break
		}
		result.CursorRevision = change.Revision
	}
	result.Changes = builder.changes
	if exhausted {
		result.CursorRevision = state.LastApplied
	}
	return result, nil
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
	projection := *record.Projection
	return routeapi.ReadBuildResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, Build: &projection,
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

const MaxPendingLookupResponseBytes = 8 << 20

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

type pendingWorkflowReference struct {
	key     string
	mapKey  string
	isRoute bool
}

type pendingPageBuilder struct {
	limit        int
	payloadBytes int
	hasMore      bool
	workflows    []PendingWorkflow
}

func newPendingPageBuilder(limit uint32) *pendingPageBuilder {
	return &pendingPageBuilder{limit: int(limit), workflows: make([]PendingWorkflow, 0, limit)}
}

func (b *pendingPageBuilder) add(workflow PendingWorkflow) (bool, error) {
	encoded, err := json.Marshal(workflow)
	if err != nil {
		return false, fmt.Errorf("raftstore: encode pending workflow: %w", err)
	}
	key, err := json.Marshal(workflow.Key)
	if err != nil {
		return false, fmt.Errorf("raftstore: encode pending workflow key: %w", err)
	}
	payloadBytes := b.payloadBytes + len(encoded)
	if len(b.workflows) != 0 {
		payloadBytes++
	}
	// Reserve NextKey even if this turns out to be the final row. A page that
	// stops on either count or bytes therefore always fits the same bound.
	responseBytes := len(`{"workflows":[`) + payloadBytes + len(`],"next_key":`) + len(key) + 1
	if len(b.workflows) >= b.limit || responseBytes > MaxPendingLookupResponseBytes {
		if len(b.workflows) == 0 {
			return false, errors.New("raftstore: one pending workflow exceeds the response byte limit")
		}
		b.hasMore = true
		return false, nil
	}
	b.payloadBytes = payloadBytes
	b.workflows = append(b.workflows, workflow)
	return true, nil
}

func (b *pendingPageBuilder) result() PendingLookupResult {
	result := PendingLookupResult{Workflows: b.workflows}
	if b.hasMore {
		result.NextKey = b.workflows[len(b.workflows)-1].Key
	}
	return result
}

func lookupPending(state DataState, query PendingLookup) (PendingLookupResult, error) {
	if !state.Accepts(query.Identity) {
		return PendingLookupResult{}, nil
	}
	references := make([]pendingWorkflowReference, 0)
	for key, record := range state.Routes {
		qualified := "r" + key
		if qualified > query.AfterKey && routeNeedsCoordinator(record) {
			references = append(references, pendingWorkflowReference{key: qualified, mapKey: key, isRoute: true})
		}
	}
	for key, record := range state.Builds {
		qualified := "b" + key
		if qualified > query.AfterKey && buildNeedsCoordinator(record) {
			references = append(references, pendingWorkflowReference{key: qualified, mapKey: key})
		}
	}
	sort.Slice(references, func(i, j int) bool { return references[i].key < references[j].key })
	builder := newPendingPageBuilder(query.Limit)
	for _, reference := range references {
		workflow := PendingWorkflow{Key: reference.key}
		if reference.isRoute {
			record := cloneRouteRecord(state.Routes[reference.mapKey])
			workflow.Route = &record
		} else {
			record := cloneBuildRecord(state.Builds[reference.mapKey])
			workflow.Build = &record
		}
		accepted, err := builder.add(workflow)
		if err != nil {
			return PendingLookupResult{}, err
		}
		if !accepted {
			break
		}
	}
	return builder.result(), nil
}

func routeNeedsCoordinator(record clusterstate.RouteWorkflowRecord) bool {
	return record.State == clusterstate.WorkflowRouteStarting || record.State == clusterstate.WorkflowRouteResuming ||
		record.State == clusterstate.WorkflowRouteDeleting || record.State == clusterstate.WorkflowRouteTombstone ||
		len(record.Finalizations) != 0
}

func buildNeedsCoordinator(record clusterstate.BuildRecord) bool {
	return record.State == clusterstate.BuildStarting || len(record.Finalizations) != 0
}
