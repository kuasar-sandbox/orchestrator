package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var (
	ErrNodeWorkflowConflict      = nodeexec.ErrWorkflowConflict
	ErrNodeWorkflowMissing       = nodeexec.ErrWorkflowMissing
	ErrNodeWorkflowState         = nodeexec.ErrWorkflowState
	ErrNodeWorkflowOutboxPending = nodeexec.ErrFinalOutboxPending
)

const workflowColumns = `object_kind,object_id,group_name,route_key,node_id,node_epoch,session_seq,data_endpoint,
  normalized_demand,demand_digest,dispatch_spec,dispatch_spec_digest,provider_policy_version,opaque_binding,binding_digest,
  build_demand_json,admission_state,result,reason,reservation_token,queue_sequence,resource_claimed,
  object_state,event_seq,acked_event_seq,latest_event_json,workflow_finalized`

func scanNodeWorkflow(row interface{ Scan(...any) error }) (*nodeexec.WorkflowRecord, error) {
	var record nodeexec.WorkflowRecord
	var kind int
	var nodeEpoch, sessionSeq, queueSequence, eventSeq, ackedEventSeq []byte
	var buildDemandJSON, admissionState, result, latestEventJSON string
	var resourceClaimed, finalized int
	if err := row.Scan(
		&kind, &record.ObjectID, &record.Group, &record.RouteKey, &record.NodeID, &nodeEpoch, &sessionSeq, &record.DataEndpoint,
		&record.NormalizedDemand, &record.DemandDigest, &record.DispatchSpec, &record.DispatchSpecDigest,
		&record.ProviderPolicyVersion, &record.OpaqueBinding, &record.BindingDigest, &buildDemandJSON, &admissionState, &result,
		&record.Reason, &record.ReservationToken, &queueSequence, &resourceClaimed,
		&record.ObjectState, &eventSeq, &ackedEventSeq, &latestEventJSON, &finalized,
	); err != nil {
		return nil, err
	}
	record.Kind = clusterstate.ExecutionKind(kind)
	var err error
	if record.NodeEpoch, err = decodeUint64(nodeEpoch); err != nil {
		return nil, err
	}
	if record.SessionSeq, err = decodeUint64(sessionSeq); err != nil {
		return nil, err
	}
	if record.QueueSequence, err = decodeUint64(queueSequence); err != nil {
		return nil, err
	}
	if record.EventSeq, err = decodeUint64(eventSeq); err != nil {
		return nil, err
	}
	if record.AckedEventSeq, err = decodeUint64(ackedEventSeq); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(buildDemandJSON), &record.BuildDemand); err != nil {
		return nil, fmt.Errorf("store: decode Build demand: %w", err)
	}
	record.AdmissionState = nodeexec.AdmissionState(admissionState)
	record.Result = clusterstate.DispatchOutcome(result)
	record.ResourceClaimed = resourceClaimed != 0
	record.WorkflowFinalized = finalized != 0
	if latestEventJSON != "" {
		var event routesync.ExecutionEvent
		if err := json.Unmarshal([]byte(latestEventJSON), &event); err != nil {
			return nil, fmt.Errorf("store: decode latest execution event: %w", err)
		}
		record.LatestEvent = &event
	}
	return &record, nil
}

func (s *Store) GetNodeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID string,
) (*nodeexec.WorkflowRecord, error) {
	record, err := scanNodeWorkflow(s.db.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM node_workflows WHERE object_kind=? AND object_id=?`, kind, objectID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get node workflow: %w", err)
	}
	return record, nil
}

func sameDispatch(existing *nodeexec.WorkflowRecord, dispatch nodeexec.DispatchRecord) bool {
	return existing.DispatchRecord.SameDispatch(dispatch)
}

// PrepareBuildWorkflow atomically records Build Admission, command dedupe,
// resource accounting, protected Binding, and the node-local Build object.
func (s *Store) PrepareBuildWorkflow(
	ctx context.Context,
	dispatch nodeexec.DispatchRecord,
	build *types.Build,
	capacity nodeexec.BuildCapacity,
	safetyRejectReason string,
) (*nodeexec.WorkflowRecord, error) {
	if err := dispatch.Validate(); err != nil {
		return nil, err
	}
	if dispatch.Kind != clusterstate.ExecutionKindBuild {
		return nil, errors.New("store: Build Admission requires a Build dispatch")
	}
	if err := capacity.Validate(); err != nil {
		return nil, err
	}
	if build != nil && build.BuildID != dispatch.ObjectID {
		return nil, errors.New("store: Build object and dispatch identity disagree")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	existing, err := scanNodeWorkflow(tx.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM node_workflows WHERE object_kind=? AND object_id=?`, dispatch.Kind, dispatch.ObjectID))
	if err == nil {
		if !sameDispatch(existing, dispatch) {
			return nil, ErrNodeWorkflowConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	occupied, err := executionObjectExistsTx(ctx, tx, dispatch.Kind, dispatch.ObjectID)
	if err != nil {
		return nil, err
	}
	if occupied {
		return nil, ErrNodeWorkflowConflict
	}

	record := &nodeexec.WorkflowRecord{DispatchRecord: dispatch}
	switch {
	case safetyRejectReason != "":
		record.AdmissionState = nodeexec.AdmissionRejected
		record.Result = clusterstate.DispatchDefinitiveReject
		record.Reason = safetyRejectReason
	case !capacity.CanEverFit(dispatch.BuildDemand):
		record.AdmissionState = nodeexec.AdmissionRejected
		record.Result = clusterstate.DispatchDefinitiveReject
		record.Reason = "exceeds_build_capacity"
	default:
		claimed, err := buildUsageTx(ctx, tx, dispatch.NodeID, dispatch.NodeEpoch, true)
		if err != nil {
			return nil, err
		}
		queueDepth, err := buildQueueDepthTx(ctx, tx, dispatch.NodeID, dispatch.NodeEpoch)
		if err != nil {
			return nil, err
		}
		if queueDepth == 0 && claimed.Fits(capacity, dispatch.BuildDemand) {
			record.AdmissionState = nodeexec.AdmissionAdmitted
			record.Result = clusterstate.DispatchAcceptedAdmitted
			record.ResourceClaimed = true
			record.ObjectState = string(types.BuildRegistered)
		} else if queueDepth < capacity.QueueLimit {
			record.AdmissionState = nodeexec.AdmissionQueued
			record.Result = clusterstate.DispatchAcceptedQueued
			record.QueueSequence, err = nextBuildQueueSequenceTx(ctx, tx, dispatch.NodeID, dispatch.NodeEpoch)
			if err != nil {
				return nil, err
			}
			record.ObjectState = string(types.BuildRegistered)
		} else {
			record.AdmissionState = nodeexec.AdmissionRejected
			record.Result = clusterstate.DispatchDefinitiveReject
			record.Reason = "build_queue_full"
		}
	}
	if record.Result == clusterstate.DispatchAcceptedAdmitted || record.Result == clusterstate.DispatchAcceptedQueued {
		if build == nil {
			return nil, errors.New("store: accepted Build Admission requires a Build object")
		}
		stored := cloneBuild(build)
		stored.Metadata, err = clusterstate.WithExecutionBinding(clusterstate.WithoutSystemMetadata(stored.Metadata), record.OpaqueBinding)
		if err != nil {
			return nil, err
		}
		stored.Status = types.BuildRegistered
		if err := s.putBuild(ctx, tx, stored); err != nil {
			return nil, err
		}
	}
	if err := insertNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// RecordSandboxWorkflow journals the durable resource-controller decision. The
// controller owns the reservation; this transaction owns command dedupe and the
// original Holder-visible result.
func (s *Store) RecordSandboxWorkflow(
	ctx context.Context,
	dispatch nodeexec.DispatchRecord,
	decision nodeexec.AdmissionDecision,
) (*nodeexec.WorkflowRecord, error) {
	if err := dispatch.Validate(); err != nil {
		return nil, err
	}
	if dispatch.Kind != clusterstate.ExecutionKindSandbox {
		return nil, errors.New("store: Sandbox Admission requires a Sandbox dispatch")
	}
	if err := decision.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	existing, err := getNodeWorkflowTx(ctx, tx, dispatch.Kind, dispatch.ObjectID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !sameDispatch(existing, dispatch) {
			return nil, ErrNodeWorkflowConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return existing, nil
	}
	occupied, err := executionObjectExistsTx(ctx, tx, dispatch.Kind, dispatch.ObjectID)
	if err != nil {
		return nil, err
	}
	if occupied {
		return nil, ErrNodeWorkflowConflict
	}
	record := &nodeexec.WorkflowRecord{
		DispatchRecord: dispatch,
		AdmissionState: decision.State,
		Result:         decision.Result, Reason: decision.Reason,
		ReservationToken: decision.ReservationToken,
		ResourceClaimed:  decision.State == nodeexec.AdmissionAdmitted,
	}
	if err := insertNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// AdmitQueuedSandbox records that the resource controller promoted an existing
// durable queue entry. The original ACCEPTED_QUEUED result remains unchanged.
func (s *Store) AdmitQueuedSandbox(
	ctx context.Context,
	sandboxID, demandDigest, reservationToken string,
) (*nodeexec.WorkflowRecord, error) {
	return s.advanceAdmission(ctx, clusterstate.ExecutionKindSandbox, sandboxID, demandDigest,
		reservationToken, nodeexec.AdmissionQueued, nodeexec.AdmissionAdmitted)
}

// ClaimSandboxWorkflow records ownership transfer to sandbox launch. Retrying a
// completed claim returns the same journal row and reservation token.
func (s *Store) ClaimSandboxWorkflow(
	ctx context.Context,
	sandboxID, demandDigest, reservationToken string,
) (*nodeexec.WorkflowRecord, error) {
	return s.advanceAdmission(ctx, clusterstate.ExecutionKindSandbox, sandboxID, demandDigest,
		reservationToken, nodeexec.AdmissionAdmitted, nodeexec.AdmissionLaunching)
}

func (s *Store) advanceAdmission(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, demandDigest, reservationToken string,
	from, to nodeexec.AdmissionState,
) (*nodeexec.WorkflowRecord, error) {
	if objectID == "" || demandDigest == "" || reservationToken == "" {
		return nil, errors.New("store: workflow identity, demand digest, and reservation token are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, kind, objectID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	if record.DemandDigest != demandDigest || record.ReservationToken != reservationToken {
		return nil, ErrNodeWorkflowConflict
	}
	if record.AdmissionState == to ||
		(to == nodeexec.AdmissionLaunching && (record.AdmissionState == nodeexec.AdmissionRunning || record.AdmissionState == nodeexec.AdmissionTerminal)) {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return record, nil
	}
	if record.AdmissionState != from {
		return nil, ErrNodeWorkflowState
	}
	record.AdmissionState = to
	if to == nodeexec.AdmissionAdmitted || to == nodeexec.AdmissionLaunching {
		record.ResourceClaimed = true
	}
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// PromoteBuildQueue admits a strict FIFO prefix whose resource claims fit. It
// never changes each command's original ACCEPTED_QUEUED result.
func (s *Store) PromoteBuildQueue(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	capacity nodeexec.BuildCapacity,
	maxCount int,
) ([]*nodeexec.WorkflowRecord, error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nil, errors.New("store: node identity is required for Build queue promotion")
	}
	if err := capacity.Validate(); err != nil {
		return nil, err
	}
	if maxCount <= 0 {
		maxCount = 64
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	usage, err := buildUsageTx(ctx, tx, nodeID, nodeEpoch, true)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT object_id FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND admission_state=? ORDER BY queue_sequence, object_id LIMIT ?`,
		clusterstate.ExecutionKindBuild, nodeID, encodeUint64(nodeEpoch), nodeexec.AdmissionQueued, maxCount)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	promoted := make([]*nodeexec.WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindBuild, id)
		if err != nil {
			return nil, err
		}
		if record == nil || record.AdmissionState != nodeexec.AdmissionQueued {
			continue
		}
		build, err := s.getBuildTx(ctx, tx, record.ObjectID)
		if err != nil {
			return nil, err
		}
		if build == nil {
			return nil, errors.New("store: queued Build object is missing")
		}
		if !capacity.CanEverFit(record.BuildDemand) {
			update := nodeexec.EventUpdate{State: string(types.BuildError), Reason: "exceeds_build_capacity"}
			applyBuildState(build, update)
			if err := s.putBuild(ctx, tx, build); err != nil {
				return nil, err
			}
			record.AdmissionState = nodeexec.AdmissionTerminal
			record.ResourceClaimed = false
			record.ObjectState = update.State
			if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
				return nil, err
			}
			continue
		}
		if !usage.Fits(capacity, record.BuildDemand) {
			break
		}
		record.AdmissionState = nodeexec.AdmissionAdmitted
		record.ResourceClaimed = true
		if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
			return nil, err
		}
		usage = usage.Add(record.BuildDemand)
		promoted = append(promoted, record)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return promoted, nil
}

// FailQueuedBuildWorkflows converts a bounded FIFO batch into durable terminal
// facts while a node-global safety fence rejects new Build execution. Accepted
// queue entries stay bound to this node/build_id and are never dispatched again.
func (s *Store) FailQueuedBuildWorkflows(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	reason string,
	maxCount int,
) ([]*nodeexec.WorkflowRecord, error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nil, errors.New("store: node identity is required for queued Build failure")
	}
	if reason == "" {
		return nil, errors.New("store: queued Build rejection requires a safety reason")
	}
	if maxCount <= 0 {
		maxCount = 64
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT object_id FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND admission_state=? ORDER BY queue_sequence, object_id LIMIT ?`,
		clusterstate.ExecutionKindBuild, nodeID, encodeUint64(nodeEpoch), nodeexec.AdmissionQueued, maxCount)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	failed := make([]*nodeexec.WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindBuild, id)
		if err != nil {
			return nil, err
		}
		if record == nil || record.AdmissionState != nodeexec.AdmissionQueued {
			continue
		}
		build, err := s.getBuildTx(ctx, tx, record.ObjectID)
		if err != nil {
			return nil, err
		}
		if build == nil {
			return nil, errors.New("store: queued Build object is missing")
		}
		update := nodeexec.EventUpdate{State: string(types.BuildError), Reason: reason}
		applyBuildState(build, update)
		if err := s.putBuild(ctx, tx, build); err != nil {
			return nil, err
		}
		record.AdmissionState = nodeexec.AdmissionTerminal
		record.ResourceClaimed = false
		record.ObjectState = update.State
		if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
			return nil, err
		}
		failed = append(failed, record)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return failed, nil
}

// ClaimBuildWorkflow moves a durably admitted Build into launch ownership.
func (s *Store) ClaimBuildWorkflow(ctx context.Context, buildID, demandDigest string) (*nodeexec.WorkflowRecord, error) {
	if buildID == "" || demandDigest == "" {
		return nil, errors.New("store: Build ID and demand digest are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindBuild, buildID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	if record.DemandDigest != demandDigest {
		return nil, ErrNodeWorkflowConflict
	}
	if record.AdmissionState == nodeexec.AdmissionLaunching || record.AdmissionState == nodeexec.AdmissionRunning ||
		record.AdmissionState == nodeexec.AdmissionTerminal {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return record, nil
	}
	if record.AdmissionState != nodeexec.AdmissionAdmitted || !record.ResourceClaimed {
		return nil, ErrNodeWorkflowState
	}
	record.AdmissionState = nodeexec.AdmissionLaunching
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// BuildAdmissionUsage returns node-authoritative queued plus resource-claimed
// totals. The former is the placement projection; the latter is physical usage.
func (s *Store) BuildAdmissionUsage(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
) (queuedAndClaimed, claimed nodeexec.BuildUsage, err error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, errors.New("store: node identity is required for Build usage")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	defer tx.Rollback()
	queuedAndClaimed, err = buildUsageTx(ctx, tx, nodeID, nodeEpoch, false)
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	claimed, err = buildUsageTx(ctx, tx, nodeID, nodeEpoch, true)
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	return queuedAndClaimed, claimed, nil
}

// CommitClusterBuildState atomically updates a cluster Build object, protected
// Binding metadata, and node-local Admission/resource ownership.
func (s *Store) CommitClusterBuildState(
	ctx context.Context,
	build *types.Build,
	update nodeexec.EventUpdate,
) (*nodeexec.WorkflowRecord, error) {
	if build == nil || build.BuildID == "" {
		return nil, errors.New("store: Build state update requires a Build object")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindBuild, build.BuildID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	if err := validateBuildStateTransition(record, update); err != nil {
		return nil, err
	}
	current, err := s.getBuildTx(ctx, tx, build.BuildID)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, errors.New("store: accepted Build object is missing")
	}
	if record.ObjectState == update.State {
		if !duplicateBuildUpdateMatches(current, build, update) {
			return nil, ErrNodeWorkflowConflict
		}
		changed, mergeErr := mergeBuildRuntimeFields(current, build, update)
		if mergeErr != nil {
			return nil, mergeErr
		}
		if changed {
			if err := s.putBuild(ctx, tx, current); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return record, nil
	}
	stored := cloneBuild(current)
	if _, err := mergeBuildRuntimeFields(stored, build, update); err != nil {
		return nil, err
	}
	stored.Metadata, err = clusterstate.WithExecutionBinding(clusterstate.WithoutSystemMetadata(stored.Metadata), record.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	applyBuildState(stored, update)
	if err := s.putBuild(ctx, tx, stored); err != nil {
		return nil, err
	}
	record.ObjectState = update.State
	switch update.State {
	case string(types.BuildBuilding):
		record.AdmissionState = nodeexec.AdmissionRunning
	case string(types.BuildReady), string(types.BuildError):
		record.AdmissionState = nodeexec.AdmissionTerminal
		record.ResourceClaimed = false
	}
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// CommitSandboxEvent atomically stores the protected Binding with the existing
// Sandbox model and advances its latest durable execution fact.
func (s *Store) CommitSandboxEvent(
	ctx context.Context,
	sandbox *types.Sandbox,
	update nodeexec.EventUpdate,
) (*nodeexec.WorkflowRecord, error) {
	if sandbox == nil || sandbox.ID == "" {
		return nil, errors.New("store: Sandbox event requires a Sandbox object")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindSandbox, sandbox.ID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	current, err := s.getSandboxTx(ctx, tx, sandbox.ID)
	if err != nil {
		return nil, err
	}
	eventSource := sandbox
	if current != nil {
		eventSource = current
	}
	if update.AccessToken == "" {
		update.AccessToken = eventSource.EnvdAccessToken
	}
	if update.TrafficAccessToken == "" {
		update.TrafficAccessToken = eventSource.TrafficAccessToken
	}
	if update.TemplateRef == "" {
		update.TemplateRef = eventSource.TemplateID
	}
	if update.SnapshotLocation == "" {
		update.SnapshotLocation = sandboxSnapshotLocation(eventSource.SnapshotRef)
	}
	if err := validateSandboxEventTransition(record, update); err != nil {
		return nil, err
	}
	if record.LatestEvent != nil && record.ObjectState == update.State {
		if eventUpdateMatches(record.LatestEvent, update) {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return record, nil
		}
		return nil, ErrNodeWorkflowConflict
	}
	stored := cloneSandbox(eventSource)
	stored.EnvdAccessToken = update.AccessToken
	stored.TrafficAccessToken = update.TrafficAccessToken
	stored.Metadata, err = clusterstate.WithExecutionBinding(clusterstate.WithoutSystemMetadata(stored.Metadata), record.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	applySandboxState(stored, update.State)
	event, err := executionEventFor(record, update)
	if err != nil {
		return nil, err
	}
	if err := s.putSandbox(ctx, tx, stored); err != nil {
		return nil, err
	}
	record.ObjectState = update.State
	record.EventSeq = event.EventSeq
	record.LatestEvent = event
	switch update.State {
	case string(clusterstate.WorkflowRouteReady), string(clusterstate.WorkflowRoutePaused):
		record.AdmissionState = nodeexec.AdmissionRunning
	case "ERROR", "DELETED":
		record.AdmissionState = nodeexec.AdmissionTerminal
	}
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.notifyEvent()
	return record, nil
}

func insertNodeWorkflowTx(ctx context.Context, tx *sql.Tx, record *nodeexec.WorkflowRecord) error {
	buildDemandJSON, err := json.Marshal(record.BuildDemand)
	if err != nil {
		return err
	}
	latestEventJSON := ""
	if record.LatestEvent != nil {
		encoded, err := json.Marshal(record.LatestEvent)
		if err != nil {
			return err
		}
		latestEventJSON = string(encoded)
	}
	now := time.Now().Unix()
	_, err = tx.ExecContext(ctx, `
	INSERT INTO node_workflows (
	  object_kind,object_id,group_name,route_key,node_id,node_epoch,session_seq,data_endpoint,normalized_demand,demand_digest,
	  dispatch_spec,dispatch_spec_digest,provider_policy_version,opaque_binding,binding_digest,build_demand_json,
	  admission_state,result,reason,reservation_token,queue_sequence,resource_claimed,object_state,
	  event_seq,acked_event_seq,latest_event_json,workflow_finalized,created_unix,updated_unix)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.Kind, record.ObjectID, record.Group, record.RouteKey, record.NodeID, encodeUint64(record.NodeEpoch),
		encodeUint64(record.SessionSeq), record.DataEndpoint, record.NormalizedDemand, record.DemandDigest, record.DispatchSpec, record.DispatchSpecDigest,
		record.ProviderPolicyVersion, record.OpaqueBinding, record.BindingDigest, string(buildDemandJSON), record.AdmissionState, record.Result,
		record.Reason, record.ReservationToken, encodeUint64(record.QueueSequence), boolInt(record.ResourceClaimed),
		record.ObjectState, encodeUint64(record.EventSeq), encodeUint64(record.AckedEventSeq), latestEventJSON,
		boolInt(record.WorkflowFinalized), now, now)
	if err != nil {
		return fmt.Errorf("store: insert node workflow: %w", err)
	}
	return nil
}

func getNodeWorkflowTx(
	ctx context.Context,
	tx *sql.Tx,
	kind clusterstate.ExecutionKind,
	objectID string,
) (*nodeexec.WorkflowRecord, error) {
	record, err := scanNodeWorkflow(tx.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM node_workflows WHERE object_kind=? AND object_id=?`, kind, objectID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get node workflow transaction: %w", err)
	}
	return record, nil
}

type sqlQueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) ExecutionObjectExists(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID string,
) (bool, error) {
	if objectID == "" {
		return false, errors.New("store: execution object ID is required")
	}
	return executionObjectExistsTx(ctx, s.db, kind, objectID)
}

func executionObjectExistsTx(
	ctx context.Context,
	query sqlQueryRower,
	kind clusterstate.ExecutionKind,
	objectID string,
) (bool, error) {
	table, idColumn, err := bindingTable(kind)
	if err != nil {
		return false, err
	}
	var one int
	err = query.QueryRowContext(ctx, "SELECT 1 FROM "+table+" WHERE "+idColumn+"=?", objectID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) getBuildTx(ctx context.Context, tx *sql.Tx, buildID string) (*types.Build, error) {
	build, err := s.scanBuild(tx.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE build_id=?`, buildID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get Build transaction: %w", err)
	}
	return build, nil
}

func (s *Store) getSandboxTx(ctx context.Context, tx *sql.Tx, sandboxID string) (*types.Sandbox, error) {
	sandbox, err := s.scan(tx.QueryRowContext(ctx, `SELECT `+cols+` FROM sandboxes WHERE id=?`, sandboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get Sandbox transaction: %w", err)
	}
	return sandbox, nil
}

func updateNodeWorkflowTx(ctx context.Context, tx *sql.Tx, record *nodeexec.WorkflowRecord) error {
	latestEventJSON := ""
	if record.LatestEvent != nil {
		encoded, err := json.Marshal(record.LatestEvent)
		if err != nil {
			return err
		}
		if len(encoded) > routesync.MaxExecutionEventBytes {
			return errors.New("store: execution event exceeds bounded payload size")
		}
		latestEventJSON = string(encoded)
	}
	result, err := tx.ExecContext(ctx, `UPDATE node_workflows SET
opaque_binding=?,binding_digest=?,admission_state=?,result=?,reason=?,reservation_token=?,queue_sequence=?,resource_claimed=?,
object_state=?,event_seq=?,acked_event_seq=?,latest_event_json=?,workflow_finalized=?,updated_unix=?
WHERE object_kind=? AND object_id=?`,
		record.OpaqueBinding, record.BindingDigest, record.AdmissionState, record.Result, record.Reason, record.ReservationToken,
		encodeUint64(record.QueueSequence), boolInt(record.ResourceClaimed), record.ObjectState,
		encodeUint64(record.EventSeq), encodeUint64(record.AckedEventSeq), latestEventJSON,
		boolInt(record.WorkflowFinalized), time.Now().Unix(), record.Kind, record.ObjectID)
	if err != nil {
		return fmt.Errorf("store: update node workflow: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrNodeWorkflowMissing
	}
	return nil
}

func validateBuildStateTransition(record *nodeexec.WorkflowRecord, update nodeexec.EventUpdate) error {
	if update.State == string(types.BuildReady) && update.ArtifactRef == "" {
		return errors.New("store: ready Build requires an artifact reference")
	}
	if update.State == string(types.BuildError) && update.Reason == "" {
		return errors.New("store: failed Build requires a reason")
	}
	if record.AdmissionState == nodeexec.AdmissionRejected || record.AdmissionState == nodeexec.AdmissionQueued ||
		record.WorkflowFinalized {
		return ErrNodeWorkflowState
	}
	if record.ObjectState == update.State {
		return nil
	}
	allowed := false
	switch record.ObjectState {
	case "", string(types.BuildWaiting):
		allowed = update.State == string(types.BuildRegistered) || update.State == string(types.BuildBuilding) ||
			update.State == string(types.BuildError)
	case string(types.BuildRegistered):
		allowed = update.State == string(types.BuildWaiting) || update.State == string(types.BuildBuilding) ||
			update.State == string(types.BuildError)
	case string(types.BuildBuilding):
		allowed = update.State == string(types.BuildReady) || update.State == string(types.BuildError)
	}
	if !allowed {
		return ErrNodeWorkflowState
	}
	return nil
}

func validateSandboxEventTransition(record *nodeexec.WorkflowRecord, update nodeexec.EventUpdate) error {
	if update.State == "ERROR" && update.Reason == "" {
		return errors.New("store: Sandbox ERROR event requires a reason")
	}
	if record.AdmissionState == nodeexec.AdmissionRejected || record.AdmissionState == nodeexec.AdmissionQueued ||
		record.WorkflowFinalized {
		return ErrNodeWorkflowState
	}
	if record.ObjectState == update.State {
		return nil
	}
	allowed := false
	switch record.ObjectState {
	case "":
		allowed = update.State == string(clusterstate.WorkflowRouteReady) || update.State == "ERROR"
	case string(clusterstate.WorkflowRouteReady):
		allowed = update.State == string(clusterstate.WorkflowRoutePaused) || update.State == "ERROR" || update.State == "DELETED"
	case string(clusterstate.WorkflowRoutePaused):
		allowed = update.State == string(clusterstate.WorkflowRouteReady) || update.State == "ERROR" || update.State == "DELETED"
	}
	if !allowed {
		return ErrNodeWorkflowState
	}
	return nil
}

func eventUpdateMatches(event *routesync.ExecutionEvent, update nodeexec.EventUpdate) bool {
	return event != nil && event.State == update.State && event.AccessToken == update.AccessToken &&
		event.TrafficAccessToken == update.TrafficAccessToken && event.TemplateRef == update.TemplateRef &&
		event.SnapshotLocation == update.SnapshotLocation && event.ArtifactRef == update.ArtifactRef &&
		event.Reason == update.Reason
}

func cloneBuild(source *types.Build) *types.Build {
	out := *source
	out.Steps = append([]types.TemplateStep(nil), source.Steps...)
	out.Names = append([]string(nil), source.Names...)
	out.Aliases = append([]string(nil), source.Aliases...)
	out.Metadata = cloneMetadata(source.Metadata)
	return &out
}

func cloneSandbox(source *types.Sandbox) *types.Sandbox {
	out := *source
	out.Metadata = cloneMetadata(source.Metadata)
	out.Env = cloneMetadata(source.Env)
	return &out
}

func mergeBuildRuntimeFields(stored, incoming *types.Build, update nodeexec.EventUpdate) (bool, error) {
	if stored == nil || incoming == nil {
		return false, errors.New("store: Build launch field merge requires both objects")
	}
	changed := false
	if incoming.RunID != "" {
		if stored.RunID != "" && stored.RunID != incoming.RunID {
			return false, ErrNodeWorkflowConflict
		}
		if stored.RunID == "" {
			stored.RunID = incoming.RunID
			changed = true
		}
	}
	for _, field := range []struct {
		name     string
		stored   *[]string
		incoming []string
	}{
		{name: "names", stored: &stored.Names, incoming: incoming.Names},
		{name: "aliases", stored: &stored.Aliases, incoming: incoming.Aliases},
	} {
		if len(field.incoming) == 0 || slices.Equal(*field.stored, field.incoming) {
			continue
		}
		if len(*field.stored) != 0 && update.State == string(types.BuildReady) &&
			update.ArtifactRef != "" && slices.Equal(appendUniqueString(*field.stored, update.ArtifactRef), field.incoming) {
			*field.stored = append([]string(nil), field.incoming...)
			changed = true
			continue
		}
		if len(*field.stored) == 0 {
			*field.stored = append([]string(nil), field.incoming...)
			changed = true
			continue
		}
		return false, fmt.Errorf("%w: Build %s changed", ErrNodeWorkflowConflict, field.name)
	}
	if update.State == string(types.BuildReady) {
		if incoming.PersistID != update.ArtifactRef || incoming.PersistID == "" {
			return false, ErrNodeWorkflowConflict
		}
		if stored.Kind != incoming.Kind || stored.StartCmd != incoming.StartCmd || stored.ReadyCmd != incoming.ReadyCmd {
			stored.Kind, stored.StartCmd, stored.ReadyCmd = incoming.Kind, incoming.StartCmd, incoming.ReadyCmd
			changed = true
		}
	}
	return changed, nil
}

func duplicateBuildUpdateMatches(stored, incoming *types.Build, update nodeexec.EventUpdate) bool {
	switch update.State {
	case string(types.BuildReady):
		return update.ArtifactRef != "" && stored.PersistID == update.ArtifactRef &&
			stored.Kind == incoming.Kind && stored.StartCmd == incoming.StartCmd && stored.ReadyCmd == incoming.ReadyCmd
	case string(types.BuildError):
		return update.Reason != "" && stored.Reason == update.Reason
	default:
		return true
	}
}

func appendUniqueString(values []string, value string) []string {
	result := append([]string(nil), values...)
	if !slices.Contains(result, value) {
		result = append(result, value)
	}
	return result
}

func cloneMetadata(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func applyBuildState(build *types.Build, update nodeexec.EventUpdate) {
	switch update.State {
	case string(types.BuildRegistered):
		build.Status = types.BuildRegistered
	case string(types.BuildWaiting):
		build.Status = types.BuildWaiting
	case string(types.BuildBuilding):
		build.Status = types.BuildBuilding
	case string(types.BuildReady):
		build.Status = types.BuildReady
		build.PersistID = update.ArtifactRef
	case string(types.BuildError):
		build.Status = types.BuildError
		build.Reason = update.Reason
	}
}

func applySandboxState(sandbox *types.Sandbox, state string) {
	switch state {
	case string(clusterstate.WorkflowRouteReady):
		sandbox.State = types.StateRunning
	case string(clusterstate.WorkflowRoutePaused):
		sandbox.State = types.StatePaused
	case "ERROR", "DELETED":
		sandbox.State = types.StateDead
	}
}

func sandboxSnapshotLocation(ref string) string {
	switch {
	case ref == "":
		return ""
	case strings.HasPrefix(ref, "manifest://"):
		return "remote"
	default:
		return "local"
	}
}

// LaunchableNodeWorkflows returns a bounded durable queue of admitted commands
// that still need local launch ownership. It is used both after a wake and after
// node-ctl restart.
func (s *Store) LaunchableNodeWorkflows(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	nodeID string,
	nodeEpoch uint64,
	afterObjectID string,
	limit int,
) ([]*nodeexec.WorkflowRecord, error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nil, errors.New("store: node identity is required for launch recovery")
	}
	if limit <= 0 {
		limit = 64
	}
	states := []nodeexec.AdmissionState{nodeexec.AdmissionAdmitted, nodeexec.AdmissionLaunching}
	if kind == clusterstate.ExecutionKindBuild {
		states = append(states, nodeexec.AdmissionRunning)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(states)), ",")
	args := []any{kind, nodeID, encodeUint64(nodeEpoch), afterObjectID}
	for _, state := range states {
		args = append(args, state)
	}
	stateFilter := ""
	if kind == clusterstate.ExecutionKindBuild {
		stateFilter = " AND object_state IN (?,?,?)"
		args = append(args, types.BuildRegistered, types.BuildWaiting, types.BuildBuilding)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT `+workflowColumns+` FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND object_id>? AND admission_state IN (`+placeholders+`)
`+stateFilter+` ORDER BY object_id LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workflows := make([]*nodeexec.WorkflowRecord, 0, limit)
	for rows.Next() {
		record, err := scanNodeWorkflow(rows)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, record)
	}
	return workflows, rows.Err()
}

// SandboxWorkflowsForReconcile returns the bounded set whose resource-controller
// state can be between durable handoff steps after a process crash.
func (s *Store) SandboxWorkflowsForReconcile(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	afterObjectID string,
	limit int,
) ([]*nodeexec.WorkflowRecord, error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nil, errors.New("store: node identity is required for Sandbox reconciliation")
	}
	if limit <= 0 {
		limit = 256
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+workflowColumns+` FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND object_id>? AND admission_state IN (?,?,?,?,?)
ORDER BY object_id LIMIT ?`,
		clusterstate.ExecutionKindSandbox, nodeID, encodeUint64(nodeEpoch), afterObjectID,
		nodeexec.AdmissionQueued, nodeexec.AdmissionAdmitted, nodeexec.AdmissionLaunching,
		nodeexec.AdmissionRunning, nodeexec.AdmissionTerminal, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]*nodeexec.WorkflowRecord, 0, limit)
	for rows.Next() {
		record, err := scanNodeWorkflow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// ReleaseSandboxWorkflow records completion of the idempotent resource-
// controller release. A terminal row remains replayable/finalizable.
func (s *Store) ReleaseSandboxWorkflow(
	ctx context.Context,
	sandboxID, demandDigest, reservationToken string,
) (*nodeexec.WorkflowRecord, error) {
	if sandboxID == "" || demandDigest == "" || reservationToken == "" {
		return nil, errors.New("store: Sandbox release requires identity, demand digest, and reservation token")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindSandbox, sandboxID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	if record.DemandDigest != demandDigest || record.ReservationToken != reservationToken {
		return nil, ErrNodeWorkflowConflict
	}
	if record.AdmissionState != nodeexec.AdmissionTerminal {
		return nil, ErrNodeWorkflowState
	}
	if !record.ResourceClaimed {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return record, nil
	}
	record.ResourceClaimed = false
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

// FailPendingNodeWorkflow durably terminates Sandbox work that never created a
// business object. The Binding and error fact are committed together and
// replayed through the normal outbox.
func (s *Store) FailPendingNodeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, demandDigest, reason string,
) (*nodeexec.WorkflowRecord, error) {
	if objectID == "" || demandDigest == "" || reason == "" {
		return nil, errors.New("store: failed workflow requires identity, demand digest, and reason")
	}
	if kind != clusterstate.ExecutionKindSandbox {
		return nil, errors.New("store: only pending Sandbox workflows publish terminal events")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, kind, objectID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNodeWorkflowMissing
	}
	if record.DemandDigest != demandDigest {
		return nil, ErrNodeWorkflowConflict
	}
	update := nodeexec.EventUpdate{State: "ERROR", Reason: reason}
	if record.AdmissionState == nodeexec.AdmissionTerminal {
		if record.LatestEvent != nil && eventUpdateMatches(record.LatestEvent, update) {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return record, nil
		}
		return nil, ErrNodeWorkflowConflict
	}
	if record.AdmissionState == nodeexec.AdmissionRejected || record.WorkflowFinalized {
		return nil, ErrNodeWorkflowState
	}
	event, err := executionEventFor(record, update)
	if err != nil {
		return nil, err
	}
	record.AdmissionState = nodeexec.AdmissionTerminal
	record.ObjectState = event.State
	record.EventSeq = event.EventSeq
	record.LatestEvent = event
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.notifyEvent()
	return record, nil
}

// EventWake is a coalesced hint for the node-link replay loop. Correctness does
// not depend on delivery of every hint; reconnect and periodic replay query the
// durable table.
func (s *Store) EventWake() <-chan struct{} { return s.eventWake }

func (s *Store) notifyEvent() {
	select {
	case s.eventWake <- struct{}{}:
	default:
	}
}

// PendingExecutionEvents returns the latest unacknowledged fact per object for
// the exact durable NodeEpoch. It never materializes an unbounded replay.
func (s *Store) PendingExecutionEvents(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	after routesync.EventCursor,
	maxCount, maxBytes int,
) ([]routesync.ExecutionEvent, routesync.EventCursor, error) {
	if nodeID == "" || nodeEpoch == 0 {
		return nil, routesync.EventCursor{}, errors.New("store: node identity is required for event replay")
	}
	afterKind, err := replayCursorKind(after)
	if err != nil {
		return nil, routesync.EventCursor{}, err
	}
	if maxCount <= 0 {
		maxCount = 128
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT latest_event_json FROM node_workflows
	WHERE object_kind=? AND node_id=? AND node_epoch=? AND event_seq>acked_event_seq AND latest_event_json<>''
  AND (object_kind>? OR (object_kind=? AND object_id>?))
ORDER BY object_kind,object_id LIMIT ?`,
		clusterstate.ExecutionKindSandbox, nodeID, encodeUint64(nodeEpoch), afterKind, afterKind, after.ObjectID, maxCount)
	if err != nil {
		return nil, routesync.EventCursor{}, fmt.Errorf("store: query pending execution events: %w", err)
	}
	defer rows.Close()
	events := make([]routesync.ExecutionEvent, 0, maxCount)
	used := 0
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, routesync.EventCursor{}, err
		}
		if len(encoded) > maxBytes && len(events) == 0 {
			return nil, routesync.EventCursor{}, errors.New("store: replay byte limit is smaller than one durable event")
		}
		if used+len(encoded) > maxBytes {
			break
		}
		var event routesync.ExecutionEvent
		if err := json.Unmarshal([]byte(encoded), &event); err != nil {
			return nil, routesync.EventCursor{}, fmt.Errorf("store: decode pending execution event: %w", err)
		}
		if err := event.Validate(); err != nil {
			return nil, routesync.EventCursor{}, err
		}
		events = append(events, event)
		used += len(encoded)
	}
	if err := rows.Err(); err != nil {
		return nil, routesync.EventCursor{}, err
	}
	next := after
	if len(events) > 0 {
		last := events[len(events)-1]
		next = routesync.EventCursor{ObjectKind: last.ObjectKind, ObjectID: last.ObjectID}
	}
	return events, next, nil
}

func replayCursorKind(cursor routesync.EventCursor) (clusterstate.ExecutionKind, error) {
	if cursor.ObjectKind == "" && cursor.ObjectID == "" {
		return 0, nil
	}
	if cursor.ObjectKind == "" || cursor.ObjectID == "" {
		return 0, errors.New("store: incomplete execution event cursor")
	}
	return parseExecutionKind(cursor.ObjectKind)
}

// AckExecutionEvent advances only the committed watermark named by the ACK.
// ACK(N) cannot discard a latest pending event N+1.
func (s *Store) AckExecutionEvent(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	ack routesync.EventAck,
) error {
	if err := ack.Validate(); err != nil {
		return err
	}
	kind, err := parseExecutionKind(ack.ObjectKind)
	if err != nil {
		return err
	}
	if kind != clusterstate.ExecutionKindSandbox {
		return errors.New("store: only Sandbox execution events can be acknowledged")
	}
	if nodeID == "" || nodeEpoch == 0 {
		return errors.New("store: incomplete execution event ACK")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, kind, ack.ObjectID)
	if err != nil {
		return err
	}
	if record == nil {
		return tx.Commit()
	}
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return err
	}
	if record.NodeID != nodeID || record.NodeEpoch != nodeEpoch ||
		binding.RegistryGeneration != ack.RegistryGeneration || record.BindingDigest != ack.BindingDigest {
		return ErrNodeWorkflowConflict
	}
	if ack.EventSeq > record.EventSeq {
		return errors.New("store: execution event ACK is ahead of local state")
	}
	if ack.EventSeq > record.AckedEventSeq {
		record.AckedEventSeq = ack.EventSeq
	}
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return err
	}
	return tx.Commit()
}

// FinalizeNodeWorkflow is the explicit Registry proof that command fencing is
// no longer required. Active or non-terminal accepted work cannot be finalized.
func (s *Store) FinalizeNodeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, bindingDigest string,
) error {
	if objectID == "" || bindingDigest == "" {
		return errors.New("store: workflow identity and Binding digest are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, kind, objectID)
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}
	if record.BindingDigest != bindingDigest {
		return ErrNodeWorkflowConflict
	}
	if record.AdmissionState != nodeexec.AdmissionRejected && record.AdmissionState != nodeexec.AdmissionTerminal {
		return ErrNodeWorkflowState
	}
	if kind == clusterstate.ExecutionKindBuild && record.Result != clusterstate.DispatchDefinitiveReject {
		return fmt.Errorf("%w: accepted Build lifecycle is node-local", ErrNodeWorkflowState)
	}
	if record.ResourceClaimed {
		return fmt.Errorf("%w: workflow cannot finalize before resource release", ErrNodeWorkflowState)
	}
	record.WorkflowFinalized = true
	if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if record.EventSeq != record.AckedEventSeq {
		return ErrNodeWorkflowOutboxPending
	}
	return nil
}

// CompactFinalizedNodeWorkflows removes only markers fenced by a newer durable
// NodeEpoch or SessionSeq. Commands from the old tuple can no longer pass
// node-link validation at that point.
func (s *Store) CompactFinalizedNodeWorkflows(
	ctx context.Context,
	nodeID string,
	nodeEpoch, currentSessionSeq uint64,
) error {
	if nodeID == "" || nodeEpoch == 0 || currentSessionSeq == 0 {
		return errors.New("store: current node session identity is required for workflow compaction")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM node_workflows
WHERE node_id=? AND (node_epoch<? OR (node_epoch=? AND session_seq<?))
AND workflow_finalized=1 AND event_seq=acked_event_seq`,
		nodeID, encodeUint64(nodeEpoch), encodeUint64(nodeEpoch), encodeUint64(currentSessionSeq))
	return err
}

func parseExecutionKind(value string) (clusterstate.ExecutionKind, error) {
	switch value {
	case string(placement.ObjectSandbox):
		return clusterstate.ExecutionKindSandbox, nil
	case string(placement.ObjectBuild):
		return clusterstate.ExecutionKindBuild, nil
	default:
		return 0, errors.New("store: invalid execution object kind")
	}
}

func buildUsageTx(
	ctx context.Context,
	tx *sql.Tx,
	nodeID string,
	nodeEpoch uint64,
	claimedOnly bool,
) (nodeexec.BuildUsage, error) {
	query := `SELECT build_demand_json FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND admission_state IN (?,?,?,?)`
	args := []any{
		clusterstate.ExecutionKindBuild, nodeID, encodeUint64(nodeEpoch),
		nodeexec.AdmissionQueued, nodeexec.AdmissionAdmitted,
		nodeexec.AdmissionLaunching, nodeexec.AdmissionRunning,
	}
	if claimedOnly {
		query += ` AND resource_claimed=1`
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nodeexec.BuildUsage{}, err
	}
	defer rows.Close()
	var usage nodeexec.BuildUsage
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nodeexec.BuildUsage{}, err
		}
		var demand placement.BuildDemand
		if err := json.Unmarshal([]byte(encoded), &demand); err != nil {
			return nodeexec.BuildUsage{}, err
		}
		usage = usage.Add(demand)
	}
	return usage, rows.Err()
}

func buildQueueDepthTx(ctx context.Context, tx *sql.Tx, nodeID string, nodeEpoch uint64) (int, error) {
	var depth int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND admission_state=?`,
		clusterstate.ExecutionKindBuild, nodeID, encodeUint64(nodeEpoch), nodeexec.AdmissionQueued).Scan(&depth)
	return depth, err
}

func nextBuildQueueSequenceTx(ctx context.Context, tx *sql.Tx, nodeID string, nodeEpoch uint64) (uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT queue_sequence FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=?`, clusterstate.ExecutionKindBuild, nodeID, encodeUint64(nodeEpoch))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var highest uint64
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return 0, err
		}
		value, err := decodeUint64(encoded)
		if err != nil {
			return 0, err
		}
		if value > highest {
			highest = value
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if highest == ^uint64(0) {
		return 0, errors.New("store: Build queue sequence exhausted")
	}
	return highest + 1, nil
}

func executionEventFor(
	record *nodeexec.WorkflowRecord,
	update nodeexec.EventUpdate,
) (*routesync.ExecutionEvent, error) {
	if record == nil || record.Kind != clusterstate.ExecutionKindSandbox {
		return nil, errors.New("store: only Sandbox workflows publish execution events")
	}
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	event := &routesync.ExecutionEvent{
		ObjectKind: executionKindName(record.Kind), ObjectID: record.ObjectID,
		NodeID: record.NodeID, NodeEpoch: record.NodeEpoch,
		RegistryGeneration: binding.RegistryGeneration, BindingDigest: record.BindingDigest,
		EventSeq: record.EventSeq + 1, State: update.State, DataEndpoint: record.DataEndpoint,
		AccessToken: update.AccessToken, TrafficAccessToken: update.TrafficAccessToken,
		TemplateRef: update.TemplateRef, SnapshotLocation: update.SnapshotLocation,
		ArtifactRef: update.ArtifactRef, Reason: update.Reason,
	}
	if err := event.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	if len(encoded) > routesync.MaxExecutionEventBytes {
		return nil, errors.New("store: execution event exceeds bounded payload size")
	}
	return event, nil
}

func executionKindName(kind clusterstate.ExecutionKind) string {
	if kind == clusterstate.ExecutionKindSandbox {
		return string(placement.ObjectSandbox)
	}
	return string(placement.ObjectBuild)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
