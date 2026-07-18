package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var (
	ErrNodeWorkflowConflict = nodeexec.ErrWorkflowConflict
	ErrNodeWorkflowMissing  = nodeexec.ErrWorkflowMissing
	ErrNodeWorkflowState    = nodeexec.ErrWorkflowState
)

const workflowColumns = `object_kind,object_id,group_name,route_key,node_id,node_epoch,data_endpoint,
  normalized_demand,demand_digest,dispatch_spec,dispatch_spec_digest,provider_policy_version,opaque_binding,binding_digest,
  build_demand_json,admission_state,result,reason,reservation_token,queue_sequence,resource_claimed,
  object_state,event_seq,acked_event_seq,latest_event_json,workflow_finalized`

func scanNodeWorkflow(row interface{ Scan(...any) error }) (*nodeexec.WorkflowRecord, error) {
	var record nodeexec.WorkflowRecord
	var kind int
	var nodeEpoch, queueSequence, eventSeq, ackedEventSeq []byte
	var buildDemandJSON, admissionState, result, latestEventJSON string
	var resourceClaimed, finalized int
	if err := row.Scan(
		&kind, &record.ObjectID, &record.Group, &record.RouteKey, &record.NodeID, &nodeEpoch, &record.DataEndpoint,
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
// resource accounting, Binding, and the initial queued outbox event.
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
		claimed, err := buildUsageTx(ctx, tx, true)
		if err != nil {
			return nil, err
		}
		queueDepth, err := buildQueueDepthTx(ctx, tx)
		if err != nil {
			return nil, err
		}
		if queueDepth == 0 && claimed.Fits(capacity, dispatch.BuildDemand) {
			record.AdmissionState = nodeexec.AdmissionAdmitted
			record.Result = clusterstate.DispatchAcceptedAdmitted
			record.ResourceClaimed = true
			event, err := executionEventFor(record, nodeexec.EventUpdate{State: string(clusterstate.BuildRegistered)})
			if err != nil {
				return nil, err
			}
			record.EventSeq = event.EventSeq
			record.ObjectState = event.State
			record.LatestEvent = event
		} else if queueDepth < capacity.QueueLimit {
			record.AdmissionState = nodeexec.AdmissionQueued
			record.Result = clusterstate.DispatchAcceptedQueued
			record.QueueSequence, err = nextBuildQueueSequenceTx(ctx, tx)
			if err != nil {
				return nil, err
			}
			event, err := executionEventFor(record, nodeexec.EventUpdate{State: string(clusterstate.BuildQueued)})
			if err != nil {
				return nil, err
			}
			record.EventSeq = event.EventSeq
			record.ObjectState = event.State
			record.LatestEvent = event
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
		if record.AdmissionState == nodeexec.AdmissionQueued {
			stored.Status = types.BuildWaiting
		} else {
			stored.Status = types.BuildRegistered
		}
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
	if record.LatestEvent != nil {
		s.notifyEvent()
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
	capacity nodeexec.BuildCapacity,
	maxCount int,
) ([]*nodeexec.WorkflowRecord, error) {
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
	usage, err := buildUsageTx(ctx, tx, true)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT object_id FROM node_workflows
WHERE object_kind=? AND admission_state=? ORDER BY queue_sequence, object_id LIMIT ?`,
		clusterstate.ExecutionKindBuild, nodeexec.AdmissionQueued, maxCount)
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
		if !usage.Fits(capacity, record.BuildDemand) {
			break
		}
		build, err := s.getBuildTx(ctx, tx, record.ObjectID)
		if err != nil {
			return nil, err
		}
		if build == nil {
			return nil, errors.New("store: queued Build object is missing")
		}
		event, err := executionEventFor(record, nodeexec.EventUpdate{State: string(clusterstate.BuildRegistered)})
		if err != nil {
			return nil, err
		}
		build.Status = types.BuildRegistered
		if err := s.putBuild(ctx, tx, build); err != nil {
			return nil, err
		}
		record.AdmissionState = nodeexec.AdmissionAdmitted
		record.ResourceClaimed = true
		record.ObjectState = event.State
		record.EventSeq = event.EventSeq
		record.LatestEvent = event
		if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
			return nil, err
		}
		usage = usage.Add(record.BuildDemand)
		promoted = append(promoted, record)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(promoted) > 0 {
		s.notifyEvent()
	}
	return promoted, nil
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
func (s *Store) BuildAdmissionUsage(ctx context.Context) (queuedAndClaimed, claimed nodeexec.BuildUsage, err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	defer tx.Rollback()
	queuedAndClaimed, err = buildUsageTx(ctx, tx, false)
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	claimed, err = buildUsageTx(ctx, tx, true)
	if err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodeexec.BuildUsage{}, nodeexec.BuildUsage{}, err
	}
	return queuedAndClaimed, claimed, nil
}

// CommitBuildEvent atomically updates the existing Build object, protected
// Binding metadata, resource ownership, and latest durable event.
func (s *Store) CommitBuildEvent(
	ctx context.Context,
	build *types.Build,
	update nodeexec.EventUpdate,
) (*nodeexec.WorkflowRecord, error) {
	if build == nil || build.BuildID == "" {
		return nil, errors.New("store: Build event requires a Build object")
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
	if err := validateBuildEventTransition(record, update); err != nil {
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
	current, err := s.getBuildTx(ctx, tx, build.BuildID)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, errors.New("store: accepted Build object is missing")
	}
	event, err := executionEventFor(record, update)
	if err != nil {
		return nil, err
	}
	stored := cloneBuild(current)
	stored.RunID = build.RunID
	stored.Names = append([]string(nil), build.Names...)
	stored.Aliases = append([]string(nil), build.Aliases...)
	stored.Metadata, err = clusterstate.WithExecutionBinding(clusterstate.WithoutSystemMetadata(stored.Metadata), record.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	applyBuildState(stored, update)
	if err := s.putBuild(ctx, tx, stored); err != nil {
		return nil, err
	}
	record.ObjectState = update.State
	record.EventSeq = event.EventSeq
	record.LatestEvent = event
	switch update.State {
	case string(clusterstate.BuildBuilding):
		record.AdmissionState = nodeexec.AdmissionRunning
	case string(clusterstate.BuildReady), string(clusterstate.BuildError):
		record.AdmissionState = nodeexec.AdmissionTerminal
		record.ResourceClaimed = false
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
	if update.AccessToken == "" {
		update.AccessToken = sandbox.EnvdAccessToken
	}
	if update.TrafficAccessToken == "" {
		update.TrafficAccessToken = sandbox.TrafficAccessToken
	}
	if update.TemplateRef == "" {
		update.TemplateRef = sandbox.TemplateID
	}
	if update.SnapshotLocation == "" {
		update.SnapshotLocation = sandboxSnapshotLocation(sandbox.SnapshotRef)
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
	stored := cloneSandbox(sandbox)
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
	  object_kind,object_id,group_name,route_key,node_id,node_epoch,data_endpoint,normalized_demand,demand_digest,
	  dispatch_spec,dispatch_spec_digest,provider_policy_version,opaque_binding,binding_digest,build_demand_json,
	  admission_state,result,reason,reservation_token,queue_sequence,resource_claimed,object_state,
	  event_seq,acked_event_seq,latest_event_json,workflow_finalized,created_unix,updated_unix)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.Kind, record.ObjectID, record.Group, record.RouteKey, record.NodeID, encodeUint64(record.NodeEpoch),
		record.DataEndpoint, record.NormalizedDemand, record.DemandDigest, record.DispatchSpec, record.DispatchSpecDigest,
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
admission_state=?,result=?,reason=?,reservation_token=?,queue_sequence=?,resource_claimed=?,
object_state=?,event_seq=?,acked_event_seq=?,latest_event_json=?,workflow_finalized=?,updated_unix=?
WHERE object_kind=? AND object_id=?`,
		record.AdmissionState, record.Result, record.Reason, record.ReservationToken,
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

func validateBuildEventTransition(record *nodeexec.WorkflowRecord, update nodeexec.EventUpdate) error {
	if update.State == string(clusterstate.BuildReady) && update.ArtifactRef == "" {
		return errors.New("store: BUILD_READY event requires an artifact reference")
	}
	if update.State == string(clusterstate.BuildError) && update.Reason == "" {
		return errors.New("store: BUILD_ERROR event requires a reason")
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
	case "", string(clusterstate.BuildQueued):
		allowed = update.State == string(clusterstate.BuildRegistered) || update.State == string(clusterstate.BuildError)
	case string(clusterstate.BuildRegistered):
		allowed = update.State == string(clusterstate.BuildBuilding) || update.State == string(clusterstate.BuildError)
	case string(clusterstate.BuildBuilding):
		allowed = update.State == string(clusterstate.BuildReady) || update.State == string(clusterstate.BuildError)
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
	case string(clusterstate.BuildRegistered):
		build.Status = types.BuildRegistered
	case string(clusterstate.BuildBuilding):
		build.Status = types.BuildBuilding
	case string(clusterstate.BuildReady):
		build.Status = types.BuildReady
		build.PersistID = update.ArtifactRef
	case string(clusterstate.BuildError):
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
	rows, err := s.db.QueryContext(ctx, `SELECT `+workflowColumns+` FROM node_workflows
WHERE object_kind=? AND node_id=? AND node_epoch=? AND object_id>? AND admission_state IN (?,?)
ORDER BY object_id LIMIT ?`,
		kind, nodeID, encodeUint64(nodeEpoch), afterObjectID,
		nodeexec.AdmissionAdmitted, nodeexec.AdmissionLaunching, limit)
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

// FailPendingNodeWorkflow durably terminates work that never created a business
// object, such as an expired Sandbox queue entry. The Binding and error fact are
// still replayed and fenced through the normal outbox.
func (s *Store) FailPendingNodeWorkflow(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID, demandDigest, reason string,
) (*nodeexec.WorkflowRecord, error) {
	if objectID == "" || demandDigest == "" || reason == "" {
		return nil, errors.New("store: failed workflow requires identity, demand digest, and reason")
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
	state := "ERROR"
	if kind == clusterstate.ExecutionKindBuild {
		state = string(clusterstate.BuildError)
	}
	update := nodeexec.EventUpdate{State: state, Reason: reason}
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
	if kind == clusterstate.ExecutionKindBuild {
		record.ResourceClaimed = false
	}
	record.ObjectState = state
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
// the exact durable node generation. It never materializes an unbounded replay.
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
WHERE node_id=? AND node_epoch=? AND event_seq>acked_event_seq AND latest_event_json<>''
  AND (object_kind>? OR (object_kind=? AND object_id>?))
ORDER BY object_kind,object_id LIMIT ?`,
		nodeID, encodeUint64(nodeEpoch), afterKind, afterKind, after.ObjectID, maxCount)
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
	kind, err := parseExecutionKind(ack.ObjectKind)
	if err != nil {
		return err
	}
	if nodeID == "" || nodeEpoch == 0 || ack.ObjectID == "" || ack.EventSeq == 0 {
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
	if record.NodeID != nodeID || record.NodeEpoch != nodeEpoch {
		return ErrNodeWorkflowConflict
	}
	if ack.EventSeq > record.EventSeq {
		return errors.New("store: execution event ACK is ahead of local state")
	}
	if ack.EventSeq > record.AckedEventSeq {
		record.AckedEventSeq = ack.EventSeq
	}
	if record.WorkflowFinalized && record.AckedEventSeq == record.EventSeq {
		if _, err := tx.ExecContext(ctx, `DELETE FROM node_workflows WHERE object_kind=? AND object_id=?`, kind, ack.ObjectID); err != nil {
			return err
		}
	} else if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
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
	if record.ResourceClaimed {
		return fmt.Errorf("%w: workflow cannot finalize before resource release", ErrNodeWorkflowState)
	}
	if record.EventSeq == record.AckedEventSeq {
		if _, err := tx.ExecContext(ctx, `DELETE FROM node_workflows WHERE object_kind=? AND object_id=?`, kind, objectID); err != nil {
			return err
		}
	} else {
		record.WorkflowFinalized = true
		if err := updateNodeWorkflowTx(ctx, tx, record); err != nil {
			return err
		}
	}
	return tx.Commit()
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

func buildUsageTx(ctx context.Context, tx *sql.Tx, claimedOnly bool) (nodeexec.BuildUsage, error) {
	query := `SELECT build_demand_json FROM node_workflows
WHERE object_kind=? AND admission_state IN (?,?,?,?)`
	args := []any{
		clusterstate.ExecutionKindBuild, nodeexec.AdmissionQueued, nodeexec.AdmissionAdmitted,
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

func buildQueueDepthTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var depth int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_workflows WHERE object_kind=? AND admission_state=?`,
		clusterstate.ExecutionKindBuild, nodeexec.AdmissionQueued).Scan(&depth)
	return depth, err
}

func nextBuildQueueSequenceTx(ctx context.Context, tx *sql.Tx) (uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT queue_sequence FROM node_workflows WHERE object_kind=?`, clusterstate.ExecutionKindBuild)
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
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return nil, err
	}
	event := &routesync.ExecutionEvent{
		ObjectKind: executionKindName(record.Kind), ObjectID: record.ObjectID,
		NodeID: record.NodeID, NodeEpoch: record.NodeEpoch,
		StorageGeneration: binding.StorageGeneration, BindingDigest: record.BindingDigest,
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
