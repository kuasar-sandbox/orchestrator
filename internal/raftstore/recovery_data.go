package raftstore

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type DataRecoveryState struct {
	RecoveryEpoch           uint64         `json:"recovery_epoch"`
	SourceClusterID         string         `json:"source_cluster_id"`
	SourceStorageGeneration string         `json:"source_storage_generation"`
	SourceManifestDigest    string         `json:"source_manifest_digest"`
	Target                  PermitIdentity `json:"target"`
}

func (r DataRecoveryState) Validate(state DataState) error {
	if r.RecoveryEpoch == 0 || r.SourceClusterID != state.ClusterID || r.SourceStorageGeneration == "" ||
		r.SourceStorageGeneration == state.StorageGeneration || !isSHA256(r.SourceManifestDigest) ||
		r.Target.Validate() != nil || r.Target.ClusterID != state.ClusterID ||
		r.Target.StorageGeneration != state.StorageGeneration || r.Target.SystemEpoch != r.RecoveryEpoch {
		return errors.New("raftstore: invalid data-shard recovery identity")
	}
	if len(state.ServingEpochs) != 1 || r.RecoveryEpoch != state.ServingEpochs[0].SystemEpoch+1 ||
		r.Target.ManifestDigest != state.ServingEpochs[0].ManifestDigest {
		return errors.New("raftstore: data-shard recovery does not follow its initialized target generation")
	}
	return nil
}

type RecoveryObjectState string

const (
	RecoveryObjectStaged      RecoveryObjectState = "STAGED"
	RecoveryObjectRebound     RecoveryObjectState = "REBIND_ACKED"
	RecoveryObjectActivated   RecoveryObjectState = "ACTIVATED"
	RecoveryObjectQuarantined RecoveryObjectState = "QUARANTINED"
	RecoveryObjectTerminal    RecoveryObjectState = "TERMINAL"
)

type RecoveryObjectRecord struct {
	RecoveryEpoch uint64                     `json:"recovery_epoch"`
	Kind          clusterstate.ExecutionKind `json:"kind"`
	Group         string                     `json:"group"`
	RouteKey      string                     `json:"route_key,omitempty"`
	ObjectID      string                     `json:"object_id"`

	NodeID       string `json:"node_id"`
	NodeEpoch    uint64 `json:"node_epoch"`
	SessionSeq   uint64 `json:"session_seq"`
	EventSeq     uint64 `json:"event_seq"`
	ReportDigest string `json:"report_digest"`

	SourceStorageGeneration string                              `json:"source_storage_generation"`
	SourceManifestDigest    string                              `json:"source_manifest_digest"`
	SourceOpaqueBinding     string                              `json:"source_opaque_binding"`
	SourceBindingDigest     string                              `json:"source_binding_digest"`
	TargetBinding           clusterstate.ExecutionBindingIntent `json:"target_binding"`

	Route *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build *clusterstate.BuildRecord         `json:"build,omitempty"`

	State                    RecoveryObjectState   `json:"state"`
	QuarantineReason         string                `json:"quarantine_reason,omitempty"`
	ConflictingReportDigests []string              `json:"conflicting_report_digests,omitempty"`
	Revision                 clusterstate.Revision `json:"revision"`
}

func (r RecoveryObjectRecord) Validate(recovery DataRecoveryState) error {
	if r.RecoveryEpoch != recovery.RecoveryEpoch || r.Group == "" || r.ObjectID == "" ||
		r.NodeID == "" || r.NodeEpoch == 0 || r.SessionSeq == 0 || r.EventSeq == 0 ||
		!isSHA256(r.ReportDigest) || r.SourceStorageGeneration != recovery.SourceStorageGeneration ||
		r.SourceManifestDigest != recovery.SourceManifestDigest || r.SourceOpaqueBinding == "" ||
		!isSHA256(r.SourceBindingDigest) || r.Revision.StorageGeneration != recovery.Target.StorageGeneration ||
		r.Revision.LogIndex == 0 {
		return errors.New("raftstore: incomplete recovery object record")
	}
	source, err := clusterstate.DecodeExecutionBinding(r.SourceOpaqueBinding)
	if err != nil {
		return err
	}
	sourceDigest, err := clusterstate.ExecutionBindingDigest(r.SourceOpaqueBinding)
	if err != nil || sourceDigest != r.SourceBindingDigest ||
		source.StorageGeneration != recovery.SourceStorageGeneration || source.Kind != r.Kind ||
		source.ObjectID != r.ObjectID || source.Group != r.Group || source.RouteKey != r.RouteKey ||
		source.NodeID != r.NodeID || source.NodeEpoch != r.NodeEpoch {
		return errors.New("raftstore: recovery source Binding does not match the report")
	}
	if err := r.TargetBinding.Validate(r.Kind, r.ObjectID); err != nil {
		return err
	}
	target, err := clusterstate.DecodeExecutionBinding(r.TargetBinding.OpaqueBinding)
	if err != nil || target.StorageGeneration != recovery.Target.StorageGeneration ||
		target.Kind != source.Kind || target.ObjectID != source.ObjectID || target.Group != source.Group ||
		target.RouteKey != source.RouteKey || target.NodeID != source.NodeID || target.NodeEpoch != source.NodeEpoch ||
		target.DemandDigest != source.DemandDigest || target.DispatchSpecDigest != source.DispatchSpecDigest {
		return errors.New("raftstore: recovery target Binding changes immutable execution identity")
	}
	if r.TargetBinding.NodeID != r.NodeID || r.TargetBinding.NodeEpoch != r.NodeEpoch ||
		r.TargetBinding.StorageGeneration != recovery.Target.StorageGeneration {
		return errors.New("raftstore: recovery target Binding identifies another target")
	}
	if err := validateRecoveryProjection(r); err != nil {
		return err
	}
	var projectionRevision clusterstate.Revision
	if r.Route != nil {
		projectionRevision = r.Route.Revision
	} else {
		projectionRevision = r.Build.Revision
	}
	if projectionRevision.StorageGeneration != r.Revision.StorageGeneration ||
		projectionRevision.ShardID != r.Revision.ShardID || projectionRevision.LogIndex > r.Revision.LogIndex {
		return errors.New("raftstore: recovery projection revision is outside its recovery record history")
	}
	switch r.State {
	case RecoveryObjectStaged, RecoveryObjectRebound, RecoveryObjectActivated, RecoveryObjectTerminal:
		if r.QuarantineReason != "" || len(r.ConflictingReportDigests) != 0 {
			return errors.New("raftstore: non-quarantined recovery object contains conflict evidence")
		}
	case RecoveryObjectQuarantined:
		if r.QuarantineReason == "" || len(r.ConflictingReportDigests) == 0 ||
			!sort.StringsAreSorted(r.ConflictingReportDigests) {
			return errors.New("raftstore: quarantined recovery object lacks ordered conflict evidence")
		}
		for index, digest := range r.ConflictingReportDigests {
			if !isSHA256(digest) || index > 0 && digest == r.ConflictingReportDigests[index-1] {
				return errors.New("raftstore: invalid recovery conflict digest set")
			}
		}
	default:
		return errors.New("raftstore: invalid recovery object state")
	}
	return nil
}

func validateRecoveryProjection(record RecoveryObjectRecord) error {
	switch record.Kind {
	case clusterstate.ExecutionKindSandbox:
		if record.Route == nil || record.Build != nil || record.Route.Group != record.Group ||
			record.Route.RouteKey != record.RouteKey {
			return errors.New("raftstore: Sandbox recovery requires one matching Route projection")
		}
		if record.Route.State != clusterstate.WorkflowRouteReady &&
			record.Route.State != clusterstate.WorkflowRoutePaused {
			return errors.New("raftstore: recovery exposes only live READY or PAUSED Routes")
		}
		if err := record.Route.Validate(); err != nil {
			return err
		}
		var execution clusterstate.ReadyRoute
		if record.Route.Ready != nil {
			execution = *record.Route.Ready
		} else {
			execution = record.Route.Paused.Execution
		}
		if execution.SandboxID != record.ObjectID || execution.NodeID != record.NodeID ||
			execution.NodeEpoch != record.NodeEpoch || execution.StorageGeneration != record.TargetBinding.StorageGeneration ||
			execution.BindingDigest != record.TargetBinding.BindingDigest || execution.LastEventSeq != record.EventSeq {
			return errors.New("raftstore: recovered Route projection differs from the target Binding report")
		}
	case clusterstate.ExecutionKindBuild:
		if record.Build == nil || record.Route != nil || record.RouteKey != "" || record.Build.Group != record.Group ||
			record.Build.BuildID != record.ObjectID || record.Build.Projection == nil {
			return errors.New("raftstore: Build recovery requires one matching Build projection")
		}
		switch record.Build.State {
		case clusterstate.BuildQueued, clusterstate.BuildRegistered, clusterstate.BuildBuilding,
			clusterstate.BuildReady, clusterstate.BuildError:
		default:
			return errors.New("raftstore: recovered Build state is not a bound projection")
		}
		if err := record.Build.Validate(); err != nil {
			return err
		}
		projection := record.Build.Projection
		if projection.NodeID != record.NodeID || projection.NodeEpoch != record.NodeEpoch ||
			projection.StorageGeneration != record.TargetBinding.StorageGeneration ||
			projection.BindingDigest != record.TargetBinding.BindingDigest || projection.LastEventSeq != record.EventSeq {
			return errors.New("raftstore: recovered Build projection differs from the target Binding report")
		}
	default:
		return errors.New("raftstore: unsupported recovery object kind")
	}
	return nil
}

type RecoveryObjectUpdate struct {
	Kind                clusterstate.ExecutionKind `json:"kind"`
	Group               string                     `json:"group"`
	RouteKey            string                     `json:"route_key,omitempty"`
	ObjectID            string                     `json:"object_id"`
	RecoveryEpoch       uint64                     `json:"recovery_epoch"`
	ReportDigest        string                     `json:"report_digest"`
	SourceBindingDigest string                     `json:"source_binding_digest"`
	TargetBindingDigest string                     `json:"target_binding_digest"`
	Reason              string                     `json:"reason,omitempty"`
}

type RecoveryFinalization struct {
	RecoveryEpoch           uint64 `json:"recovery_epoch"`
	SourceStorageGeneration string `json:"source_storage_generation"`
	SourceManifestDigest    string `json:"source_manifest_digest"`
}

func beginDataRecovery(state *DataState, identity ShardRequestIdentity, recovery DataRecoveryState) error {
	if state == nil || !state.Initialized || state.Recovery != nil || len(state.RecoveryRecords) != 0 ||
		len(state.RecoveryClaims) != 0 || identity.ShardID != state.ShardID || identity.PermitIdentity != recovery.Target {
		return errors.New("raftstore: data shard cannot begin recovery")
	}
	if err := recovery.Validate(*state); err != nil {
		return err
	}
	copy := recovery
	state.Recovery = &copy
	return nil
}

func stageRecoveryObject(state *DataState, index uint64, identity ShardRequestIdentity, input RecoveryObjectRecord) error {
	if !dataRecoveryAccepts(state, identity) || input.State != RecoveryObjectStaged ||
		input.Revision != (clusterstate.Revision{}) || input.QuarantineReason != "" ||
		len(input.ConflictingReportDigests) != 0 {
		return errors.New("raftstore: invalid staged recovery object")
	}
	record := cloneRecoveryRecord(input)
	record.Revision = revisionFor(*state, index)
	if record.Route != nil {
		record.Route.Revision = record.Revision
		normalizeRouteRevision(record.Route)
	}
	if record.Build != nil {
		record.Build.Revision = record.Revision
		normalizeBuildRevision(record.Build)
	}
	if err := record.Validate(*state.Recovery); err != nil {
		return err
	}
	if err := recoveryRecordTargetsShard(*state, record); err != nil {
		return err
	}
	key := recoveryRecordKey(record)
	if existing, found := state.RecoveryRecords[key]; found {
		if sameRecoveryReport(existing, record) {
			return nil
		}
		quarantineRecoveryConflict(state, index, key, record.ReportDigest, "conflicting reports for one execution")
		return nil
	}
	claim := recoveryClaimKey(record)
	if ownerKey, found := state.RecoveryClaims[claim]; found && ownerKey != key {
		state.RecoveryRecords[key] = record
		quarantineRecoveryConflict(state, index, ownerKey, record.ReportDigest, "conflicting claims for one logical object")
		quarantineRecoveryConflict(state, index, key, state.RecoveryRecords[ownerKey].ReportDigest, "conflicting claims for one logical object")
		return nil
	}
	state.RecoveryClaims[claim] = key
	state.RecoveryRecords[key] = record
	return nil
}

func sameRecoveryReport(left, right RecoveryObjectRecord) bool {
	normalize := func(record RecoveryObjectRecord) RecoveryObjectRecord {
		record = cloneRecoveryRecord(record)
		record.State = RecoveryObjectStaged
		record.QuarantineReason = ""
		record.ConflictingReportDigests = nil
		record.Revision = clusterstate.Revision{}
		if record.Route != nil {
			record.Route.Revision = clusterstate.Revision{}
		}
		if record.Build != nil {
			record.Build.Revision = clusterstate.Revision{}
		}
		return record
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
}

func updateRecoveryObject(
	state *DataState,
	index uint64,
	identity ShardRequestIdentity,
	update RecoveryObjectUpdate,
	target RecoveryObjectState,
) error {
	if !dataRecoveryAccepts(state, identity) || update.RecoveryEpoch != state.Recovery.RecoveryEpoch ||
		!isSHA256(update.ReportDigest) || !isSHA256(update.SourceBindingDigest) ||
		!isSHA256(update.TargetBindingDigest) {
		return errors.New("raftstore: invalid recovery object update")
	}
	key := recoveryUpdateKey(update)
	record, found := state.RecoveryRecords[key]
	if !found || record.ReportDigest != update.ReportDigest ||
		record.SourceBindingDigest != update.SourceBindingDigest ||
		record.TargetBinding.BindingDigest != update.TargetBindingDigest {
		return errors.New("raftstore: recovery object update does not match staged intent")
	}
	switch target {
	case RecoveryObjectRebound:
		if update.Reason != "" || record.State != RecoveryObjectStaged && record.State != RecoveryObjectRebound {
			return errors.New("raftstore: recovery rebind acknowledgement is out of order")
		}
		if record.State == RecoveryObjectRebound {
			return nil
		}
		record.State = RecoveryObjectRebound
	case RecoveryObjectQuarantined:
		if update.Reason == "" || record.State == RecoveryObjectActivated {
			return errors.New("raftstore: recovery quarantine is invalid")
		}
		record.State = RecoveryObjectQuarantined
		record.QuarantineReason = update.Reason
		record.ConflictingReportDigests = addRecoveryDigest(record.ConflictingReportDigests, record.ReportDigest)
	default:
		return errors.New("raftstore: unsupported recovery object update")
	}
	record.Revision = revisionFor(*state, index)
	state.RecoveryRecords[key] = record
	return nil
}

func activateRecoveryObject(state *DataState, index uint64, identity ShardRequestIdentity, update RecoveryObjectUpdate) error {
	if !dataRecoveryAccepts(state, identity) || update.Reason != "" {
		return errors.New("raftstore: invalid recovery activation")
	}
	key := recoveryUpdateKey(update)
	record, found := state.RecoveryRecords[key]
	if !found || record.State != RecoveryObjectRebound || record.ReportDigest != update.ReportDigest ||
		record.SourceBindingDigest != update.SourceBindingDigest ||
		record.TargetBinding.BindingDigest != update.TargetBindingDigest {
		return errors.New("raftstore: recovery object is not durably rebound")
	}
	if record.Route != nil {
		mapKey := routeMapKey(record.Group, record.RouteKey)
		if _, exists := state.Routes[mapKey]; exists {
			return errors.New("raftstore: recovered Route conflicts with target Route history")
		}
		route := cloneRouteRecord(*record.Route)
		route.Revision = revisionFor(*state, index)
		state.Routes[mapKey] = route
		bucket, _, _ := clusterstate.RouteShardFor(route.Group, route.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
		state.RouteChanges = append(state.RouteChanges, RouteChange{
			Revision: index, Bucket: bucket, Group: route.Group, RouteKey: route.RouteKey, State: route.State,
		})
	} else {
		mapKey := buildMapKey(record.Group, record.ObjectID)
		if _, exists := state.Builds[mapKey]; exists {
			return errors.New("raftstore: recovered Build conflicts with target Build history")
		}
		build := cloneBuildRecord(*record.Build)
		build.Revision = revisionFor(*state, index)
		state.Builds[mapKey] = build
	}
	record.State = RecoveryObjectActivated
	record.Revision = revisionFor(*state, index)
	state.RecoveryRecords[key] = record
	return nil
}

func finalizeDataRecovery(state *DataState, identity ShardRequestIdentity, final RecoveryFinalization) error {
	if state == nil || state.Recovery == nil || identity.ShardID != state.ShardID ||
		identity.ClusterID != state.ClusterID || identity.StorageGeneration != state.StorageGeneration ||
		identity.ManifestDigest != state.Recovery.Target.ManifestDigest ||
		identity.SystemEpoch != state.Recovery.RecoveryEpoch+1 ||
		final.RecoveryEpoch != state.Recovery.RecoveryEpoch ||
		final.SourceStorageGeneration != state.Recovery.SourceStorageGeneration ||
		final.SourceManifestDigest != state.Recovery.SourceManifestDigest {
		return errors.New("raftstore: invalid data recovery finalization identity")
	}
	for _, record := range state.RecoveryRecords {
		if record.State != RecoveryObjectActivated && record.State != RecoveryObjectQuarantined &&
			record.State != RecoveryObjectTerminal {
			return errors.New("raftstore: data recovery retains an unresolved object")
		}
	}
	state.ServingEpochs = []PermitIdentity{identity.PermitIdentity}
	state.Recovery = nil
	state.RecoveryRecords = make(map[string]RecoveryObjectRecord)
	state.RecoveryClaims = make(map[string]string)
	return nil
}

func dataRecoveryAccepts(state *DataState, identity ShardRequestIdentity) bool {
	return state != nil && state.Recovery != nil && identity.ShardID == state.ShardID &&
		identity.PermitIdentity == state.Recovery.Target
}

func recoveryRecordTargetsShard(state DataState, record RecoveryObjectRecord) error {
	var shardID uint32
	var err error
	if record.Kind == clusterstate.ExecutionKindSandbox {
		_, shardID, err = clusterstate.RouteShardFor(record.Group, record.RouteKey, state.RouteBucketCount, state.VirtualShardCount)
	} else {
		_, shardID, err = clusterstate.BuildShardFor(record.Group, record.ObjectID, state.BuildBucketCount, state.VirtualShardCount)
	}
	if err != nil || shardID != state.ShardID || record.Revision.ShardID != state.ShardID {
		return errors.New("raftstore: recovery object belongs to another shard")
	}
	return nil
}

func validateStoredRecoveryRecord(state DataState, key string, record RecoveryObjectRecord) error {
	if state.Recovery == nil {
		return errors.New("raftstore: recovery object exists outside an open data recovery")
	}
	if key != recoveryRecordKey(record) || !revisionBelongsTo(state, record.Revision) {
		return errors.New("raftstore: stored recovery object identity or revision differs from its shard")
	}
	if err := record.Validate(*state.Recovery); err != nil {
		return err
	}
	return recoveryRecordTargetsShard(state, record)
}

func validateRecoveryClaims(state DataState) error {
	for claim, ownerKey := range state.RecoveryClaims {
		owner, found := state.RecoveryRecords[ownerKey]
		if !found || claim != recoveryClaimKey(owner) {
			return errors.New("raftstore: recovery claim points to another object")
		}
	}
	for key, record := range state.RecoveryRecords {
		ownerKey, found := state.RecoveryClaims[recoveryClaimKey(record)]
		if !found {
			return errors.New("raftstore: recovery object has no logical claim")
		}
		if ownerKey != key {
			owner := state.RecoveryRecords[ownerKey]
			if owner.State != RecoveryObjectQuarantined || record.State != RecoveryObjectQuarantined {
				return errors.New("raftstore: conflicting recovery claim is not quarantined")
			}
		}
	}
	return nil
}

func quarantineRecoveryConflict(state *DataState, index uint64, key, conflictingDigest, reason string) {
	record := state.RecoveryRecords[key]
	record.State = RecoveryObjectQuarantined
	record.QuarantineReason = reason
	record.ConflictingReportDigests = addRecoveryDigest(record.ConflictingReportDigests, record.ReportDigest)
	record.ConflictingReportDigests = addRecoveryDigest(record.ConflictingReportDigests, conflictingDigest)
	record.Revision = revisionFor(*state, index)
	state.RecoveryRecords[key] = record
}

func addRecoveryDigest(values []string, digest string) []string {
	values = append([]string(nil), values...)
	for _, current := range values {
		if current == digest {
			return values
		}
	}
	values = append(values, digest)
	sort.Strings(values)
	return values
}

func recoveryRecordKey(record RecoveryObjectRecord) string {
	return lengthKey(fmt.Sprint(uint8(record.Kind)), record.Group, record.RouteKey, record.ObjectID)
}

func recoveryUpdateKey(update RecoveryObjectUpdate) string {
	return lengthKey(fmt.Sprint(uint8(update.Kind)), update.Group, update.RouteKey, update.ObjectID)
}

func recoveryClaimKey(record RecoveryObjectRecord) string {
	if record.Kind == clusterstate.ExecutionKindSandbox {
		return "r" + routeMapKey(record.Group, record.RouteKey)
	}
	return "b" + buildMapKey(record.Group, record.ObjectID)
}

func cloneRecoveryRecord(source RecoveryObjectRecord) RecoveryObjectRecord {
	clone := source
	clone.ConflictingReportDigests = append([]string(nil), source.ConflictingReportDigests...)
	if source.Route != nil {
		route := cloneRouteRecord(*source.Route)
		clone.Route = &route
	}
	if source.Build != nil {
		build := cloneBuildRecord(*source.Build)
		clone.Build = &build
	}
	return clone
}
