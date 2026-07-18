package raftstore

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type ShardRequestIdentity struct {
	PermitIdentity
	ShardID uint32 `json:"shard_id"`
}

const RouteChangefeedRetentionRevisions uint64 = 10_000

type RouteChange struct {
	Revision uint64                          `json:"revision"`
	Bucket   uint32                          `json:"bucket"`
	Group    string                          `json:"group"`
	RouteKey string                          `json:"route_key"`
	State    clusterstate.RouteWorkflowState `json:"state"`
}

func (i ShardRequestIdentity) Validate() error {
	if err := i.PermitIdentity.Validate(); err != nil {
		return err
	}
	return nil
}

type DataState struct {
	Initialized          bool                                        `json:"initialized"`
	ClusterID            string                                      `json:"cluster_id"`
	StorageGeneration    string                                      `json:"storage_generation"`
	ShardID              uint32                                      `json:"shard_id"`
	SchemaVersion        uint32                                      `json:"schema_version"`
	ProtocolVersion      uint32                                      `json:"protocol_version"`
	HashVersion          string                                      `json:"hash_version"`
	RouteBucketCount     uint32                                      `json:"route_bucket_count"`
	BuildBucketCount     uint32                                      `json:"build_bucket_count"`
	VirtualShardCount    uint32                                      `json:"virtual_shard_count"`
	ReplicaIDs           []uint64                                    `json:"replica_ids"`
	PreparedReplicaIDs   []uint64                                    `json:"prepared_replica_ids,omitempty"`
	ServingEpochs        []PermitIdentity                            `json:"serving_epochs"`
	RouteChangefeedFloor uint64                                      `json:"route_changefeed_floor"`
	RouteChanges         []RouteChange                               `json:"route_changes,omitempty"`
	Routes               map[string]clusterstate.RouteWorkflowRecord `json:"routes"`
	Builds               map[string]clusterstate.BuildRecord         `json:"builds"`
	Fences               map[string]clusterstate.ExecutionFence      `json:"fences"`
	LastApplied          uint64                                      `json:"last_applied"`
}

func (s DataState) Validate() error {
	if !s.Initialized {
		if s.ClusterID != "" || s.StorageGeneration != "" || s.ShardID != 0 || s.SchemaVersion != 0 ||
			s.ProtocolVersion != 0 || s.HashVersion != "" || s.RouteBucketCount != 0 ||
			s.BuildBucketCount != 0 || s.VirtualShardCount != 0 || len(s.ReplicaIDs) != 0 ||
			len(s.PreparedReplicaIDs) != 0 || s.RouteChangefeedFloor != 0 || len(s.RouteChanges) != 0 ||
			s.LastApplied != 0 || len(s.Routes) != 0 || len(s.Builds) != 0 || len(s.Fences) != 0 ||
			len(s.ServingEpochs) != 0 {
			return errors.New("raftstore: uninitialized data shard contains state")
		}
		return nil
	}
	if err := validateDataStateIdentity(s); err != nil {
		return err
	}
	for key, record := range s.Routes {
		if err := validateStoredRoute(s, key, record); err != nil {
			return err
		}
	}
	for key, record := range s.Builds {
		if err := validateStoredBuild(s, key, record); err != nil {
			return err
		}
	}
	for key, fence := range s.Fences {
		if err := validateStoredFence(s, key, fence); err != nil {
			return err
		}
	}
	if s.RouteChangefeedFloor > s.LastApplied {
		return errors.New("raftstore: Route changefeed floor exceeds applied state")
	}
	for index, change := range s.RouteChanges {
		if change.Revision <= s.RouteChangefeedFloor || change.Revision > s.LastApplied ||
			index > 0 && change.Revision <= s.RouteChanges[index-1].Revision {
			return errors.New("raftstore: Route changefeed revisions are invalid")
		}
		if err := validateRouteChange(s, change); err != nil {
			return err
		}
	}
	return nil
}

func validateRouteChange(state DataState, change RouteChange) error {
	if change.Revision == 0 || change.Group == "" || change.RouteKey == "" {
		return errors.New("raftstore: Route change identity is incomplete")
	}
	switch change.State {
	case clusterstate.WorkflowRouteStarting, clusterstate.WorkflowRouteReady, clusterstate.WorkflowRoutePaused,
		clusterstate.WorkflowRouteResuming, clusterstate.WorkflowRouteDeleting, clusterstate.WorkflowRouteTombstone:
	default:
		return errors.New("raftstore: Route change state is invalid")
	}
	bucket, shardID, err := clusterstate.RouteShardFor(
		change.Group, change.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
	)
	if err != nil || bucket != change.Bucket || shardID != state.ShardID {
		return errors.New("raftstore: Route change belongs to another bucket or shard")
	}
	return nil
}

func validateDataStateIdentity(s DataState) error {
	if s.ClusterID == "" || s.StorageGeneration == "" || s.SchemaVersion == 0 || s.ProtocolVersion == 0 ||
		s.HashVersion != "ShardHashV1" || s.RouteBucketCount == 0 || s.BuildBucketCount == 0 ||
		!isPowerOfTwo(s.RouteBucketCount) || !isPowerOfTwo(s.BuildBucketCount) ||
		s.VirtualShardCount == 0 || !isPowerOfTwo(s.VirtualShardCount) || s.ShardID >= s.VirtualShardCount ||
		len(s.ReplicaIDs) != int(DefaultReplication) || len(s.ServingEpochs) == 0 || len(s.ServingEpochs) > 2 ||
		s.Routes == nil || s.Builds == nil || s.Fences == nil || s.LastApplied == 0 {
		return errors.New("raftstore: incomplete data shard identity")
	}
	if !sort.SliceIsSorted(s.ReplicaIDs, func(i, j int) bool { return s.ReplicaIDs[i] < s.ReplicaIDs[j] }) {
		return errors.New("raftstore: data shard replica IDs are not sorted")
	}
	for i, replicaID := range s.ReplicaIDs {
		if replicaID == 0 || i > 0 && replicaID == s.ReplicaIDs[i-1] {
			return errors.New("raftstore: invalid data shard replica set")
		}
	}
	if len(s.ServingEpochs) == 1 && len(s.PreparedReplicaIDs) != 0 {
		return errors.New("raftstore: active data shard retains a prepared replica set")
	}
	if len(s.ServingEpochs) == 2 {
		if err := validateReplicaIDs(s.PreparedReplicaIDs); err != nil {
			return err
		}
	}
	for i, epoch := range s.ServingEpochs {
		if err := epoch.Validate(); err != nil || epoch.ClusterID != s.ClusterID || epoch.StorageGeneration != s.StorageGeneration {
			return errors.New("raftstore: serving epoch belongs to another data shard generation")
		}
		if i > 0 && epoch.SystemEpoch != s.ServingEpochs[i-1].SystemEpoch+1 {
			return errors.New("raftstore: serving epochs are not consecutive")
		}
	}
	return nil
}

func validateStoredRoute(state DataState, key string, record clusterstate.RouteWorkflowRecord) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("raftstore: invalid stored Route: %w", err)
	}
	if key != routeMapKey(record.Group, record.RouteKey) || !revisionBelongsTo(state, record.Revision) {
		return errors.New("raftstore: stored Route identity or revision differs from its shard")
	}
	_, shardID, err := clusterstate.RouteShardFor(record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
	if err != nil || shardID != state.ShardID {
		return errors.New("raftstore: stored Route belongs to another shard")
	}
	return nil
}

func validateStoredBuild(state DataState, key string, record clusterstate.BuildRecord) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("raftstore: invalid stored Build: %w", err)
	}
	if key != buildMapKey(record.Group, record.BuildID) || !revisionBelongsTo(state, record.Revision) {
		return errors.New("raftstore: stored Build identity or revision differs from its shard")
	}
	_, shardID, err := clusterstate.BuildShardFor(record.Group, record.BuildID, state.BuildBucketCount, state.VirtualShardCount)
	if err != nil || shardID != state.ShardID {
		return errors.New("raftstore: stored Build belongs to another shard")
	}
	return nil
}

func validateStoredFence(state DataState, key string, fence clusterstate.ExecutionFence) error {
	if err := fence.Validate(); err != nil {
		return fmt.Errorf("raftstore: invalid stored execution fence: %w", err)
	}
	if key != fenceMapKey(fence.Group, fence.RouteKey, fence.SandboxID) || !revisionBelongsTo(state, fence.Revision) {
		return errors.New("raftstore: stored execution fence identity or revision differs from its shard")
	}
	_, shardID, err := clusterstate.RouteShardFor(fence.Group, fence.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
	if err != nil || shardID != state.ShardID {
		return errors.New("raftstore: stored execution fence belongs to another shard")
	}
	return nil
}

func revisionBelongsTo(state DataState, revision clusterstate.Revision) bool {
	return revision.StorageGeneration == state.StorageGeneration && revision.ShardID == state.ShardID &&
		revision.LogIndex > 0 && revision.LogIndex <= state.LastApplied
}

func (s DataState) Accepts(identity ShardRequestIdentity) bool {
	if !s.Initialized || identity.ClusterID != s.ClusterID || identity.StorageGeneration != s.StorageGeneration ||
		identity.ShardID != s.ShardID {
		return false
	}
	for _, epoch := range s.ServingEpochs {
		if epoch == identity.PermitIdentity {
			return true
		}
	}
	return false
}

type DataCommandType string

const (
	DataInitializeShard DataCommandType = "INITIALIZE_SHARD"
	DataPrepareEpoch    DataCommandType = "PREPARE_EPOCH"
	DataRetireEpoch     DataCommandType = "RETIRE_EPOCH"
	DataPutRoute        DataCommandType = "PUT_ROUTE"
	DataPutBuild        DataCommandType = "PUT_BUILD"
	DataPutFence        DataCommandType = "PUT_FENCE"
	DataCompactFence    DataCommandType = "COMPACT_FENCE"
)

type RevisionExpectation struct {
	Absent   bool   `json:"absent,omitempty"`
	LogIndex uint64 `json:"log_index,omitempty"`
}

func (e RevisionExpectation) matches(found bool, revision uint64) bool {
	if e.Absent {
		return !found && e.LogIndex == 0
	}
	return found && e.LogIndex != 0 && revision == e.LogIndex
}

type FenceCompactionAuthorization struct {
	Group                      string                `json:"group"`
	RouteKey                   string                `json:"route_key"`
	SandboxID                  string                `json:"sandbox_id"`
	FenceRevision              uint64                `json:"fence_revision"`
	TerminalProofDigest        string                `json:"terminal_proof_digest"`
	FinalOutboxWatermarkAcked  bool                  `json:"final_outbox_watermark_acked"`
	NodeEpochPermanentlyFenced bool                  `json:"node_epoch_permanently_fenced"`
	ReplicaApplied             []ReplicaAppliedProof `json:"replica_applied"`
	RetentionProofDigest       string                `json:"retention_proof_digest"`
}

type DataShardBootstrap struct {
	ClusterID         string   `json:"cluster_id"`
	StorageGeneration string   `json:"storage_generation"`
	ManifestDigest    string   `json:"manifest_digest"`
	ShardID           uint32   `json:"shard_id"`
	ReplicaIDs        []uint64 `json:"replica_ids"`
	SchemaVersion     uint32   `json:"schema_version"`
	ProtocolVersion   uint32   `json:"protocol_version"`
	HashVersion       string   `json:"hash_version"`
	VirtualShardCount uint32   `json:"virtual_shard_count"`
	RouteBucketCount  uint32   `json:"route_bucket_count"`
	BuildBucketCount  uint32   `json:"build_bucket_count"`
}

func NewDataShardBootstrap(manifest Manifest, shardID uint32) (DataShardBootstrap, error) {
	if err := manifest.Validate(); err != nil {
		return DataShardBootstrap{}, err
	}
	if shardID >= manifest.VirtualShardCount {
		return DataShardBootstrap{}, errors.New("raftstore: data-shard bootstrap targets an unknown shard")
	}
	digest, err := manifest.Digest()
	if err != nil {
		return DataShardBootstrap{}, err
	}
	return dataShardBootstrap(manifest, digest, shardID)
}

func dataShardBootstrap(manifest Manifest, digest string, shardID uint32) (DataShardBootstrap, error) {
	if !isSHA256(digest) {
		return DataShardBootstrap{}, errors.New("raftstore: data-shard bootstrap requires a verified manifest digest")
	}
	if shardID >= manifest.VirtualShardCount || int(shardID) >= len(manifest.DataShards) ||
		manifest.DataShards[shardID].ShardID != shardID {
		return DataShardBootstrap{}, errors.New("raftstore: data-shard bootstrap targets an unknown shard")
	}
	return DataShardBootstrap{
		ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
		ManifestDigest: digest, ShardID: shardID,
		ReplicaIDs:    append([]uint64(nil), replicaIDsForPlacement(manifest.DataShards[shardID])...),
		SchemaVersion: manifest.SchemaVersion, ProtocolVersion: manifest.ProtocolVersion,
		HashVersion: manifest.HashVersion, VirtualShardCount: manifest.VirtualShardCount,
		RouteBucketCount: manifest.RouteBucketCount, BuildBucketCount: manifest.BuildBucketCount,
	}, nil
}

func (b DataShardBootstrap) Validate() error {
	if b.ClusterID == "" || b.StorageGeneration == "" || !isSHA256(b.ManifestDigest) ||
		validateReplicaIDs(b.ReplicaIDs) != nil || b.SchemaVersion == 0 ||
		b.ProtocolVersion == 0 || b.HashVersion != "ShardHashV1" ||
		!isPowerOfTwo(b.VirtualShardCount) || !isPowerOfTwo(b.RouteBucketCount) ||
		!isPowerOfTwo(b.BuildBucketCount) {
		return errors.New("raftstore: invalid data-shard bootstrap parameters")
	}
	return nil
}

type DataCommand struct {
	Type       DataCommandType                   `json:"type"`
	Identity   ShardRequestIdentity              `json:"identity"`
	Bootstrap  *DataShardBootstrap               `json:"bootstrap,omitempty"`
	ReplicaIDs []uint64                          `json:"replica_ids,omitempty"`
	Epoch      *PermitIdentity                   `json:"epoch,omitempty"`
	Expect     RevisionExpectation               `json:"expect,omitempty"`
	Route      *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build      *clusterstate.BuildRecord         `json:"build,omitempty"`
	Fence      *clusterstate.ExecutionFence      `json:"fence,omitempty"`
	Compaction *FenceCompactionAuthorization     `json:"compaction,omitempty"`
}

type DataApplyResult struct {
	Applied         bool   `json:"applied"`
	Conflict        bool   `json:"conflict,omitempty"`
	Reason          string `json:"reason,omitempty"`
	CurrentRevision uint64 `json:"current_revision,omitempty"`
	Revision        uint64 `json:"revision,omitempty"`
}

func ApplyDataCommand(state *DataState, index uint64, command DataCommand) DataApplyResult {
	if state == nil {
		return dataConflict("nil data shard state", 0)
	}
	if index == 0 || index <= state.LastApplied {
		return dataConflict("non-monotonic committed log index", state.LastApplied)
	}
	conflict := func(reason string, revision uint64) DataApplyResult {
		if state.Initialized {
			advanceDataApplied(state, index)
		}
		return dataConflict(reason, revision)
	}
	switch command.Type {
	case DataInitializeShard:
		if state.Initialized || command.Bootstrap == nil || command.Identity.Validate() != nil {
			return conflict("data shard bootstrap requires empty state and fixed parameters", 0)
		}
		bootstrap := command.Bootstrap
		if bootstrap.Validate() != nil || command.Identity.SystemEpoch != 1 ||
			command.Identity.ClusterID != bootstrap.ClusterID ||
			command.Identity.StorageGeneration != bootstrap.StorageGeneration ||
			command.Identity.ManifestDigest != bootstrap.ManifestDigest ||
			command.Identity.ShardID != bootstrap.ShardID ||
			command.Identity.ShardID >= bootstrap.VirtualShardCount ||
			!slices.Equal(command.ReplicaIDs, bootstrap.ReplicaIDs) {
			return conflict("invalid data shard bootstrap identity", 0)
		}
		*state = DataState{
			Initialized: true, ClusterID: command.Identity.ClusterID, StorageGeneration: command.Identity.StorageGeneration,
			ShardID: command.Identity.ShardID, SchemaVersion: bootstrap.SchemaVersion,
			ProtocolVersion: bootstrap.ProtocolVersion, HashVersion: bootstrap.HashVersion,
			RouteBucketCount: bootstrap.RouteBucketCount, BuildBucketCount: bootstrap.BuildBucketCount,
			VirtualShardCount: bootstrap.VirtualShardCount,
			ReplicaIDs:        append([]uint64(nil), command.ReplicaIDs...),
			ServingEpochs:     []PermitIdentity{command.Identity.PermitIdentity},
			Routes:            make(map[string]clusterstate.RouteWorkflowRecord),
			Builds:            make(map[string]clusterstate.BuildRecord), Fences: make(map[string]clusterstate.ExecutionFence),
		}
	case DataPrepareEpoch:
		if !state.Accepts(command.Identity) || command.Epoch == nil || len(state.ServingEpochs) != 1 ||
			validateReplicaIDs(command.ReplicaIDs) != nil {
			return conflict("data shard cannot prepare another serving epoch", 0)
		}
		epoch := *command.Epoch
		current := state.ServingEpochs[0]
		if epoch.Validate() != nil || epoch.ClusterID != state.ClusterID || epoch.StorageGeneration != state.StorageGeneration ||
			epoch.SystemEpoch != current.SystemEpoch+1 {
			return conflict("invalid prepared serving epoch", 0)
		}
		state.ServingEpochs = append(append([]PermitIdentity(nil), state.ServingEpochs...), epoch)
		state.PreparedReplicaIDs = append([]uint64(nil), command.ReplicaIDs...)
	case DataRetireEpoch:
		if !state.Accepts(command.Identity) || command.Epoch == nil || len(state.ServingEpochs) != 2 ||
			*command.Epoch != state.ServingEpochs[0] || command.Identity.PermitIdentity != state.ServingEpochs[1] {
			return conflict("serving epoch retirement is not activated", 0)
		}
		state.ServingEpochs = []PermitIdentity{state.ServingEpochs[1]}
		state.ReplicaIDs = append([]uint64(nil), state.PreparedReplicaIDs...)
		state.PreparedReplicaIDs = nil
	case DataPutRoute:
		if !state.Accepts(command.Identity) || command.Route == nil {
			return conflict("Route mutation identity is fenced", 0)
		}
		record := cloneRouteRecord(*command.Route)
		bucket, shardID, err := clusterstate.RouteShardFor(
			record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount,
		)
		if err != nil || shardID != state.ShardID {
			return conflict("Route key belongs to another shard", 0)
		}
		key := routeMapKey(record.Group, record.RouteKey)
		current, found := state.Routes[key]
		if !command.Expect.matches(found, current.Revision.LogIndex) {
			return conflict("Route revision conflict", current.Revision.LogIndex)
		}
		record.Revision = revisionFor(*state, index)
		normalizeRouteRevision(&record)
		if err := record.Validate(); err != nil {
			return conflict(err.Error(), current.Revision.LogIndex)
		}
		if found {
			if err := validateRouteTransition(current, record, state.Fences); err != nil {
				return conflict(err.Error(), current.Revision.LogIndex)
			}
		} else if record.State != clusterstate.WorkflowRouteStarting {
			return conflict("new Route must begin in STARTING", 0)
		}
		state.Routes[key] = record
		state.RouteChanges = append(state.RouteChanges, RouteChange{
			Revision: index, Bucket: bucket, Group: record.Group, RouteKey: record.RouteKey, State: record.State,
		})
	case DataPutBuild:
		if !state.Accepts(command.Identity) || command.Build == nil {
			return conflict("Build mutation identity is fenced", 0)
		}
		record := cloneBuildRecord(*command.Build)
		_, shardID, err := clusterstate.BuildShardFor(record.Group, record.BuildID, state.BuildBucketCount, state.VirtualShardCount)
		if err != nil || shardID != state.ShardID {
			return conflict("Build key belongs to another shard", 0)
		}
		key := buildMapKey(record.Group, record.BuildID)
		current, found := state.Builds[key]
		if !command.Expect.matches(found, current.Revision.LogIndex) {
			return conflict("Build revision conflict", current.Revision.LogIndex)
		}
		record.Revision = revisionFor(*state, index)
		normalizeBuildRevision(&record)
		if err := record.Validate(); err != nil {
			return conflict(err.Error(), current.Revision.LogIndex)
		}
		if found {
			if err := validateBuildTransition(current, record); err != nil {
				return conflict(err.Error(), current.Revision.LogIndex)
			}
		} else if record.State != clusterstate.BuildStarting {
			return conflict("new Build must begin in BUILD_STARTING", 0)
		}
		state.Builds[key] = record
	case DataPutFence:
		if !state.Accepts(command.Identity) || command.Fence == nil {
			return conflict("execution fence mutation identity is fenced", 0)
		}
		fence := *command.Fence
		_, shardID, err := clusterstate.RouteShardFor(fence.Group, fence.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
		if err != nil || shardID != state.ShardID {
			return conflict("execution fence belongs to another shard", 0)
		}
		key := fenceMapKey(fence.Group, fence.RouteKey, fence.SandboxID)
		current, found := state.Fences[key]
		if !command.Expect.matches(found, current.Revision.LogIndex) {
			return conflict("execution fence revision conflict", current.Revision.LogIndex)
		}
		fence.Revision = revisionFor(*state, index)
		if err := fence.Validate(); err != nil {
			return conflict(err.Error(), current.Revision.LogIndex)
		}
		if found {
			if err := validateFenceTransition(current, fence); err != nil {
				return conflict(err.Error(), current.Revision.LogIndex)
			}
		} else {
			route, routeFound := state.Routes[routeMapKey(fence.Group, fence.RouteKey)]
			if !routeFound || !fenceMatchesRouteTombstone(fence, route) {
				return conflict("new execution fence has no matching Route tombstone", 0)
			}
		}
		state.Fences[key] = fence
	case DataCompactFence:
		if !state.Accepts(command.Identity) || command.Compaction == nil {
			return conflict("execution fence compaction identity is fenced", 0)
		}
		authorization := command.Compaction
		key := fenceMapKey(authorization.Group, authorization.RouteKey, authorization.SandboxID)
		fence, found := state.Fences[key]
		if !found {
			return conflict("execution fence is missing", 0)
		}
		if err := validateFenceCompaction(*state, fence, *authorization); err != nil {
			return conflict(err.Error(), fence.Revision.LogIndex)
		}
		delete(state.Fences, key)
	default:
		return conflict(fmt.Sprintf("unknown data command %q", command.Type), 0)
	}
	advanceDataApplied(state, index)
	if err := validateDataStateIdentity(*state); err != nil {
		return dataConflict(err.Error(), state.LastApplied)
	}
	return DataApplyResult{Applied: true, Revision: index}
}

func advanceDataApplied(state *DataState, index uint64) {
	state.LastApplied = index
	if index <= RouteChangefeedRetentionRevisions {
		return
	}
	floor := index - RouteChangefeedRetentionRevisions
	if floor <= state.RouteChangefeedFloor {
		return
	}
	state.RouteChangefeedFloor = floor
	firstRetained := sort.Search(len(state.RouteChanges), func(position int) bool {
		return state.RouteChanges[position].Revision > floor
	})
	if firstRetained == 0 {
		return
	}
	copy(state.RouteChanges, state.RouteChanges[firstRetained:])
	state.RouteChanges = state.RouteChanges[:len(state.RouteChanges)-firstRetained]
}

func revisionFor(state DataState, index uint64) clusterstate.Revision {
	return clusterstate.Revision{StorageGeneration: state.StorageGeneration, ShardID: state.ShardID, LogIndex: index}
}

func normalizeRouteRevision(record *clusterstate.RouteWorkflowRecord) {
	if record.Tombstone != nil && record.Tombstone.PlacementFailure == nil && record.Tombstone.FailureRevision == (clusterstate.Revision{}) {
		record.Tombstone.FailureRevision = record.Revision
	}
}

func normalizeBuildRevision(record *clusterstate.BuildRecord) {
	if record.Tombstone != nil && record.Tombstone.FailureRevision == (clusterstate.Revision{}) {
		record.Tombstone.FailureRevision = record.Revision
	}
}

func dataConflict(reason string, revision uint64) DataApplyResult {
	return DataApplyResult{Conflict: true, Reason: reason, CurrentRevision: revision}
}

func replicaIDsForPlacement(placement ShardPlacement) []uint64 {
	ids := make([]uint64, len(placement.Replicas))
	for i, replica := range placement.Replicas {
		ids[i] = replica.ReplicaID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func validateReplicaIDs(replicaIDs []uint64) error {
	if len(replicaIDs) != int(DefaultReplication) ||
		!sort.SliceIsSorted(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] }) {
		return errors.New("raftstore: replica IDs must contain exactly three sorted values")
	}
	for index, replicaID := range replicaIDs {
		if replicaID == 0 || index > 0 && replicaID == replicaIDs[index-1] {
			return errors.New("raftstore: replica IDs must be non-zero and unique")
		}
	}
	return nil
}

func routeMapKey(group, routeKey string) string { return lengthKey(group, routeKey) }
func buildMapKey(group, buildID string) string  { return lengthKey(group, buildID) }
func fenceMapKey(group, routeKey, sandboxID string) string {
	return lengthKey(group, routeKey, sandboxID)
}

func lengthKey(fields ...string) string {
	var key []byte
	for _, field := range fields {
		length := uint32(len(field))
		key = append(key, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
		key = append(key, field...)
	}
	return string(key)
}
