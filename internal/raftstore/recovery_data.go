package raftstore

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type DataRecoveryState struct {
	RecoveryEpoch              uint64         `json:"recovery_epoch"`
	SourceClusterID            string         `json:"source_cluster_id"`
	SourceRegistryGeneration   string         `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string         `json:"source_registry_layout_digest"`
	Target                     PermitIdentity `json:"target"`
}

func (r DataRecoveryState) Validate(state DataState) error {
	if r.RecoveryEpoch == 0 || r.SourceClusterID != state.ClusterID || r.SourceRegistryGeneration == "" ||
		r.SourceRegistryGeneration == state.RegistryGeneration || !isSHA256(r.SourceRegistryLayoutDigest) ||
		r.Target.Validate() != nil || r.Target.ClusterID != state.ClusterID ||
		r.Target.RegistryGeneration != state.RegistryGeneration || r.Target.SystemEpoch != r.RecoveryEpoch {
		return errors.New("raftstore: invalid data-shard recovery identity")
	}
	if len(state.ServingEpochs) != 1 || r.RecoveryEpoch != state.ServingEpochs[0].SystemEpoch+1 ||
		r.Target.RegistryLayoutDigest != state.ServingEpochs[0].RegistryLayoutDigest {
		return errors.New("raftstore: data-shard recovery does not follow its initialized target Registry History Generation")
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

	SourceRegistryGeneration   string                              `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string                              `json:"source_registry_layout_digest"`
	SourceOpaqueBinding        string                              `json:"source_opaque_binding"`
	SourceBindingDigest        string                              `json:"source_binding_digest"`
	TargetBinding              clusterstate.ExecutionBindingIntent `json:"target_binding"`

	Route *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build *clusterstate.BuildRecord         `json:"build,omitempty"`

	State                    RecoveryObjectState   `json:"state"`
	QuarantineReason         string                `json:"quarantine_reason,omitempty"`
	ConflictingReportDigests []string              `json:"conflicting_report_digests,omitempty"`
	Revision                 clusterstate.Revision `json:"revision"`
}

func (r RecoveryObjectRecord) Validate(recovery DataRecoveryState) error {
	if r.RecoveryEpoch != recovery.RecoveryEpoch || r.Group == "" || r.ObjectID == "" ||
		r.NodeID == "" || r.NodeEpoch == 0 || r.SessionSeq == 0 ||
		!isSHA256(r.ReportDigest) || r.SourceRegistryGeneration != recovery.SourceRegistryGeneration ||
		r.SourceRegistryLayoutDigest != recovery.SourceRegistryLayoutDigest || r.SourceOpaqueBinding == "" ||
		!isSHA256(r.SourceBindingDigest) || r.Revision.RegistryGeneration != recovery.Target.RegistryGeneration ||
		r.Revision.LogIndex == 0 {
		return errors.New("raftstore: incomplete recovery object record")
	}
	if r.Kind == clusterstate.ExecutionKindSandbox && r.EventSeq == 0 {
		return errors.New("raftstore: Sandbox recovery record requires an event sequence")
	}
	if r.Kind == clusterstate.ExecutionKindBuild && r.EventSeq != 0 {
		return errors.New("raftstore: Build registration recovery does not carry a lifecycle event sequence")
	}
	source, err := clusterstate.DecodeExecutionBinding(r.SourceOpaqueBinding)
	if err != nil {
		return err
	}
	sourceDigest, err := clusterstate.ExecutionBindingDigest(r.SourceOpaqueBinding)
	if err != nil || sourceDigest != r.SourceBindingDigest ||
		source.RegistryGeneration != recovery.SourceRegistryGeneration || source.Kind != r.Kind ||
		source.ObjectID != r.ObjectID || source.Group != r.Group || source.RouteKey != r.RouteKey ||
		source.NodeID != r.NodeID || source.NodeEpoch != r.NodeEpoch {
		return errors.New("raftstore: recovery source Binding does not match the report")
	}
	if err := r.TargetBinding.Validate(r.Kind, r.ObjectID); err != nil {
		return err
	}
	target, err := clusterstate.DecodeExecutionBinding(r.TargetBinding.OpaqueBinding)
	if err != nil || target.RegistryGeneration != recovery.Target.RegistryGeneration ||
		target.Kind != source.Kind || target.ObjectID != source.ObjectID || target.Group != source.Group ||
		target.RouteKey != source.RouteKey || target.NodeID != source.NodeID || target.NodeEpoch != source.NodeEpoch ||
		target.DemandDigest != source.DemandDigest || target.DispatchSpecDigest != source.DispatchSpecDigest {
		return errors.New("raftstore: recovery target Binding changes immutable execution identity")
	}
	if r.TargetBinding.NodeID != r.NodeID || r.TargetBinding.NodeEpoch != r.NodeEpoch ||
		r.TargetBinding.RegistryGeneration != recovery.Target.RegistryGeneration {
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
	if projectionRevision.RegistryGeneration != r.Revision.RegistryGeneration ||
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
			execution.NodeEpoch != record.NodeEpoch || execution.RegistryGeneration != record.TargetBinding.RegistryGeneration ||
			execution.BindingDigest != record.TargetBinding.BindingDigest || execution.LastEventSeq != record.EventSeq {
			return errors.New("raftstore: recovered Route projection differs from the target Binding report")
		}
	case clusterstate.ExecutionKindBuild:
		if record.Build == nil || record.Route != nil || record.RouteKey != "" || record.Build.Group != record.Group ||
			record.Build.BuildID != record.ObjectID || record.Build.Projection == nil {
			return errors.New("raftstore: Build recovery requires one matching registration projection")
		}
		if record.Build.State != clusterstate.BuildRegistered {
			return errors.New("raftstore: recovered Build is not a registered binding")
		}
		if err := record.Build.Validate(); err != nil {
			return err
		}
		projection := record.Build.Projection
		if projection.NodeID != record.NodeID || projection.NodeEpoch != record.NodeEpoch ||
			projection.RegistryGeneration != record.TargetBinding.RegistryGeneration ||
			projection.BindingDigest != record.TargetBinding.BindingDigest {
			return errors.New("raftstore: recovered Build registration differs from the target Binding report")
		}
	default:
		return errors.New("raftstore: unsupported recovery object kind")
	}
	return nil
}

type RecoveryObjectUpdate struct {
	Kind                clusterstate.ExecutionKind        `json:"kind"`
	Group               string                            `json:"group"`
	RouteKey            string                            `json:"route_key,omitempty"`
	ObjectID            string                            `json:"object_id"`
	RecoveryEpoch       uint64                            `json:"recovery_epoch"`
	ReportDigest        string                            `json:"report_digest"`
	SourceBindingDigest string                            `json:"source_binding_digest"`
	TargetBindingDigest string                            `json:"target_binding_digest"`
	Reason              string                            `json:"reason,omitempty"`
	Route               *clusterstate.RouteWorkflowRecord `json:"route,omitempty"`
	Build               *clusterstate.BuildRecord         `json:"build,omitempty"`
}

type RecoveryFinalization struct {
	RecoveryEpoch              uint64 `json:"recovery_epoch"`
	SourceRegistryGeneration   string `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string `json:"source_registry_layout_digest"`
}

func beginDataRecovery(state *DataState, identity ShardRequestIdentity, recovery DataRecoveryState) error {
	if state != nil && state.Recovery != nil && *state.Recovery == recovery && identity.ShardID == state.ShardID &&
		identity.PermitIdentity == recovery.Target {
		return nil
	}
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
			if update.Route == nil && update.Build == nil || sameRecoveryRebindProjection(record, update) {
				return nil
			}
			if err := applyRecoveryRebindProjection(state, index, &record, update); err != nil {
				return err
			}
		} else if update.Route != nil || update.Build != nil {
			if err := applyRecoveryRebindProjection(state, index, &record, update); err != nil {
				return err
			}
		}
		record.State = RecoveryObjectRebound
	case RecoveryObjectQuarantined:
		if update.Reason == "" || record.State == RecoveryObjectActivated || update.Route != nil || update.Build != nil {
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

func sameRecoveryRebindProjection(record RecoveryObjectRecord, update RecoveryObjectUpdate) bool {
	if update.Route != nil {
		if record.Route == nil || update.Build != nil {
			return false
		}
		committed := cloneRouteRecord(*record.Route)
		retry := cloneRouteRecord(*update.Route)
		committed.Revision = clusterstate.Revision{}
		retry.Revision = clusterstate.Revision{}
		return reflect.DeepEqual(committed, retry)
	}
	if update.Build != nil {
		if record.Build == nil {
			return false
		}
		committed := cloneBuildRecord(*record.Build)
		retry := cloneBuildRecord(*update.Build)
		committed.Revision = clusterstate.Revision{}
		retry.Revision = clusterstate.Revision{}
		return reflect.DeepEqual(committed, retry)
	}
	return false
}

func applyRecoveryRebindProjection(
	state *DataState,
	index uint64,
	record *RecoveryObjectRecord,
	update RecoveryObjectUpdate,
) error {
	if record == nil || (update.Route == nil) == (update.Build == nil) {
		return errors.New("raftstore: rebind acknowledgement requires exactly one refreshed projection")
	}
	if update.Route != nil && update.Route.Revision != (clusterstate.Revision{}) ||
		update.Build != nil && update.Build.Revision != (clusterstate.Revision{}) {
		return errors.New("raftstore: refreshed recovery projection carries an uncommitted revision")
	}
	copy := cloneRecoveryRecord(*record)
	copy.Route, copy.Build = nil, nil
	copy.Revision = revisionFor(*state, index)
	if update.Route != nil {
		route := cloneRouteRecord(*update.Route)
		route.Revision = copy.Revision
		normalizeRouteRevision(&route)
		copy.Route = &route
		if route.Ready != nil {
			copy.EventSeq = route.Ready.LastEventSeq
		} else if route.Paused != nil {
			copy.EventSeq = route.Paused.Execution.LastEventSeq
		}
	} else {
		build := cloneBuildRecord(*update.Build)
		build.Revision = copy.Revision
		copy.Build = &build
	}
	if copy.EventSeq < record.EventSeq {
		return errors.New("raftstore: refreshed recovery projection regresses event sequence")
	}
	if record.Kind == clusterstate.ExecutionKindSandbox && record.State == RecoveryObjectRebound && copy.EventSeq == record.EventSeq {
		return errors.New("raftstore: changed rebound projection requires a newer durable event")
	}
	if err := copy.Validate(*state.Recovery); err != nil {
		return err
	}
	record.Route, record.Build, record.EventSeq = copy.Route, copy.Build, copy.EventSeq
	return nil
}

func activateRecoveryObject(state *DataState, index uint64, identity ShardRequestIdentity, update RecoveryObjectUpdate) error {
	if !dataRecoveryAccepts(state, identity) || update.Reason != "" {
		return errors.New("raftstore: invalid recovery activation")
	}
	key := recoveryUpdateKey(update)
	record, found := state.RecoveryRecords[key]
	if found && record.State == RecoveryObjectActivated && record.ReportDigest == update.ReportDigest &&
		record.SourceBindingDigest == update.SourceBindingDigest &&
		record.TargetBinding.BindingDigest == update.TargetBindingDigest {
		return nil
	}
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
	if state != nil && state.Recovery == nil && state.Initialized && identity.ShardID == state.ShardID &&
		state.Accepts(identity) && len(state.RecoveryRecords) == 0 && len(state.RecoveryClaims) == 0 &&
		final.RecoveryEpoch+1 == identity.SystemEpoch {
		return nil
	}
	if state == nil || state.Recovery == nil || identity.ShardID != state.ShardID ||
		identity.ClusterID != state.ClusterID || identity.RegistryGeneration != state.RegistryGeneration ||
		identity.RegistryLayoutDigest != state.Recovery.Target.RegistryLayoutDigest ||
		identity.SystemEpoch != state.Recovery.RecoveryEpoch+1 ||
		final.RecoveryEpoch != state.Recovery.RecoveryEpoch ||
		final.SourceRegistryGeneration != state.Recovery.SourceRegistryGeneration ||
		final.SourceRegistryLayoutDigest != state.Recovery.SourceRegistryLayoutDigest {
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
