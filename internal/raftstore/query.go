package raftstore

import (
	"container/heap"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
		response, err := lookupRouteBucket(state, *query.RouteBucket)
		return DataLookupResult{RouteBucket: &response}, err
	case query.Changefeed != nil:
		response, err := lookupRouteChangefeed(state, *query.Changefeed)
		return DataLookupResult{Changefeed: &response}, err
	case query.Fence != nil:
		response := lookupFence(state, *query.Fence)
		return DataLookupResult{Fence: &response}, nil
	case query.Recovery != nil:
		response := lookupRecovery(state, *query.Recovery)
		return DataLookupResult{Recovery: &response}, nil
	case query.Pending != nil:
		response, err := lookupPending(state, *query.Pending)
		return DataLookupResult{Pending: &response}, err
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
	Identity      ShardRequestIdentity            `json:"identity"`
	Group         string                          `json:"group"`
	Bucket        uint32                          `json:"bucket"`
	State         clusterstate.RouteWorkflowState `json:"state,omitempty"`
	AfterRouteKey string                          `json:"after_route_key,omitempty"`
	Limit         uint32                          `json:"limit"`
	Strong        bool                            `json:"strong,omitempty"`
}

func (q RouteBucketLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Group == "" || q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: Route bucket lookup requires a group and a limit between 1 and 4096")
	}
	if q.State != "" && q.State != clusterstate.WorkflowRouteReady && q.State != clusterstate.WorkflowRoutePaused {
		return errors.New("raftstore: Route bucket state must be READY or PAUSED")
	}
	return nil
}

type RouteBucketEntry struct {
	RouteKey     string                             `json:"route_key"`
	State        clusterstate.RouteWorkflowState    `json:"state"`
	NodeID       string                             `json:"node_id"`
	TemplateRef  string                             `json:"template_ref"`
	Presentation clusterstate.SandboxPresentationV1 `json:"presentation"`
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

func lookupRouteBucket(state DataState, query RouteBucketLookup) (RouteBucketResult, error) {
	result := RouteBucketResult{Group: query.Group, Bucket: query.Bucket}
	if !state.Initialized || !state.Accepts(query.Identity) {
		result.Reason = "Route shard identity is not available"
		return result, nil
	}
	if !routeBucketTargetsShard(state, query.Group, query.Bucket, query.Identity.ShardID) {
		result.Reason = "Route bucket targets another shard"
		return result, nil
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
				if query.State != "" && entry.State != query.State {
					continue
				}
				addBoundedRouteBucketEntry(&result.Routes, entry, int(query.Limit)+1)
			}
		}
	}
	return finishRouteBucketPage(result, int(query.Limit))
}

const MaxRouteBucketResponseBytes = 8 << 20

func finishRouteBucketPage(result RouteBucketResult, limit int) (RouteBucketResult, error) {
	sort.Slice(result.Routes, func(left, right int) bool {
		return result.Routes[left].RouteKey < result.Routes[right].RouteKey
	})
	candidates := result.Routes
	result.Routes = make([]RouteBucketEntry, 0, min(limit, len(candidates)))
	payloadBytes := 0
	for index, entry := range candidates {
		if len(result.Routes) == limit {
			break
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return RouteBucketResult{}, fmt.Errorf("raftstore: encode Route bucket row: %w", err)
		}
		nextPayloadBytes := payloadBytes + len(encoded)
		if len(result.Routes) != 0 {
			nextPayloadBytes++
		}
		hasMore := index+1 < len(candidates)
		nextRouteKey := ""
		if hasMore {
			nextRouteKey = entry.RouteKey
		}
		envelope := result
		envelope.Routes = make([]RouteBucketEntry, 0)
		envelope.NextRouteKey = nextRouteKey
		envelopeBytes, err := json.Marshal(DataLookupResult{RouteBucket: &envelope})
		if err != nil {
			return RouteBucketResult{}, fmt.Errorf("raftstore: encode Route bucket envelope: %w", err)
		}
		if len(envelopeBytes)+nextPayloadBytes > MaxRouteBucketResponseBytes {
			if len(result.Routes) == 0 {
				return RouteBucketResult{}, errors.New("raftstore: one Route bucket row exceeds the response byte limit")
			}
			return result, nil
		}
		payloadBytes = nextPayloadBytes
		result.Routes = append(result.Routes, entry)
		result.NextRouteKey = nextRouteKey
	}
	return result, nil
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
		Presentation: execution.Presentation.Clone(),
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
	maxRouteChangefeedEnvelopeBytes = len(`{"changefeed":{"available":true,"reset":false,"floor_revision":18446744073709551615,"head_revision":18446744073709551615,"cursor_revision":18446744073709551615,"changes":[]}}`)
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
	Fence              *clusterstate.ExecutionFence `json:"fence,omitempty"`
	HistoricallyFenced bool                         `json:"historically_fenced"`
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
	key := fenceMapKey(query.Group, query.RouteKey, query.SandboxID)
	_, historicallyFenced := state.UsedSandboxIDs[key]
	fence, found := state.Fences[key]
	if !found {
		return FenceLookupResult{HistoricallyFenced: historicallyFenced}
	}
	copy := cloneExecutionFence(fence)
	return FenceLookupResult{Fence: &copy, HistoricallyFenced: historicallyFenced}
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
		paused := cloneReadyRoute(record.Paused.Execution)
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
	ready := cloneReadyRoute(*record.Ready)
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
						BuildID: record.BuildID, RegistryGeneration: record.Revision.RegistryGeneration,
						TemplateRef: spec.TemplateID, Profile: spec.Profile,
					},
					BuildState: clusterstate.BuildStarting, BuildRevision: record.Revision.LogIndex,
					Reason: string(record.State),
				}
			}
			return routeapi.ReadBuildResponse{Outcome: routeapi.ReadConflict, Reason: string(record.State)}
		}
		return routeapi.ReadBuildResponse{Outcome: routeapi.ReadNeedLeader, Reason: string(record.State)}
	}
	registered := cloneBuildRecord(record)
	return routeapi.ReadBuildResponse{
		Outcome: routeapi.ReadReady, Group: record.Group, Build: registered.Projection,
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

const (
	MaxPendingLookupResponseBytes   = 8 << 20
	pendingLookupOuterEnvelopeBytes = len(`{"pending":`) + 1
)

func (q PendingLookup) Validate() error {
	if err := q.Identity.Validate(); err != nil {
		return err
	}
	if q.Limit == 0 || q.Limit > 4096 {
		return errors.New("raftstore: pending lookup limit must be between 1 and 4096")
	}
	if _, err := decodePendingCursor(q.AfterKey); err != nil {
		return err
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

type pendingWorkflowReference struct {
	key    string
	mapKey string
	table  byte
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
	responseBytes := pendingLookupOuterEnvelopeBytes + len(`{"workflows":[`) + payloadBytes +
		len(`],"next_key":`) + len(key) + 1
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
	afterKey, err := decodePendingCursor(query.AfterKey)
	if err != nil {
		return PendingLookupResult{}, err
	}
	references := make([]pendingWorkflowReference, 0)
	for key, record := range state.Routes {
		qualified := "r" + key
		if qualified > afterKey && routeNeedsCoordinator(record) {
			references = append(references, pendingWorkflowReference{key: qualified, mapKey: key, table: 'r'})
		}
	}
	for key, record := range state.Builds {
		qualified := "b" + key
		if qualified > afterKey && buildNeedsCoordinator(record) {
			references = append(references, pendingWorkflowReference{key: qualified, mapKey: key, table: 'b'})
		}
	}
	for key := range state.Fences {
		qualified := "f" + key
		if qualified > afterKey {
			references = append(references, pendingWorkflowReference{key: qualified, mapKey: key, table: 'f'})
		}
	}
	sort.Slice(references, func(i, j int) bool { return references[i].key < references[j].key })
	builder := newPendingPageBuilder(query.Limit)
	for _, reference := range references {
		workflow := PendingWorkflow{Key: encodePendingCursor(reference.key)}
		switch reference.table {
		case 'r':
			record := cloneRouteRecord(state.Routes[reference.mapKey])
			workflow.Route = &record
		case 'b':
			record := cloneBuildRecord(state.Builds[reference.mapKey])
			workflow.Build = &record
		case 'f':
			fence := cloneExecutionFence(state.Fences[reference.mapKey])
			workflow.Fence = &fence
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

func encodePendingCursor(key string) string {
	return hex.EncodeToString([]byte(key))
}

func decodePendingCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	raw, err := hex.DecodeString(cursor)
	if err != nil || len(raw) < 2 || raw[0] != 'b' && raw[0] != 'f' && raw[0] != 'r' ||
		hex.EncodeToString(raw) != cursor {
		return "", errors.New("raftstore: invalid pending workflow cursor")
	}
	return string(raw), nil
}

func routeNeedsCoordinator(record clusterstate.RouteWorkflowRecord) bool {
	return record.State == clusterstate.WorkflowRouteStarting || record.State == clusterstate.WorkflowRouteResuming ||
		record.State == clusterstate.WorkflowRouteDeleting || record.State == clusterstate.WorkflowRouteTombstone ||
		len(record.Finalizations) != 0
}

func buildNeedsCoordinator(record clusterstate.BuildRecord) bool {
	return record.State == clusterstate.BuildStarting || len(record.Finalizations) != 0
}
