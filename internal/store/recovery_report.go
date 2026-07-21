package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// RecoveryExecutionReport returns a transactionally consistent, ordered view
// of surviving cluster-managed executions for one exact source NodeEpoch.
// Objects without a durable workflow and protected Binding are deliberately
// absent: user metadata is never promoted into cluster authority.
func (s *Store) RecoveryExecutionReport(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	sourceGeneration string,
) ([]routesync.RecoveryExecutionFact, error) {
	facts := make([]routesync.RecoveryExecutionFact, 0)
	err := s.scanRecoveryExecutionReport(ctx, nodeID, nodeEpoch, sourceGeneration,
		func(fact routesync.RecoveryExecutionFact) error {
			facts = append(facts, fact)
			return nil
		})
	if err != nil {
		return nil, err
	}
	if _, err := routesync.CanonicalRecoveryReportDigest(facts); err != nil {
		return nil, err
	}
	return facts, nil
}

type RecoveryExecutionPage struct {
	Objects      []routesync.RecoveryExecutionFact
	ReportDigest string
	TotalObjects uint64
	NextOffset   uint64
}

// RecoveryExecutionReportPage scans one serializable snapshot, hashes the
// complete canonical report incrementally, and retains only the requested
// bounded page.
func (s *Store) RecoveryExecutionReportPage(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	sourceGeneration string,
	offset uint64,
	limit uint32,
	maxBytes uint32,
) (RecoveryExecutionPage, error) {
	if limit == 0 || maxBytes < 2 {
		return RecoveryExecutionPage{}, errors.New("store: recovery report page bounds are invalid")
	}
	page := RecoveryExecutionPage{Objects: make([]routesync.RecoveryExecutionFact, 0, limit)}
	digester := routesync.NewRecoveryReportDigester()
	pageBytes := uint64(2)
	seen := uint64(0)
	pageFull := false
	objectTooLarge := false
	err := s.scanRecoveryExecutionReport(ctx, nodeID, nodeEpoch, sourceGeneration,
		func(fact routesync.RecoveryExecutionFact) error {
			encoded, err := digester.Add(fact)
			if err != nil {
				return err
			}
			index := seen
			seen++
			if index < offset || pageFull || uint32(len(page.Objects)) == limit {
				return nil
			}
			additional := uint64(len(encoded))
			if len(page.Objects) != 0 {
				additional++
			}
			if pageBytes+additional > uint64(maxBytes) {
				pageFull = true
				objectTooLarge = len(page.Objects) == 0
				return nil
			}
			page.Objects = append(page.Objects, fact)
			pageBytes += additional
			return nil
		})
	if err != nil {
		return RecoveryExecutionPage{}, err
	}
	digest, total, err := digester.Finish()
	if err != nil {
		return RecoveryExecutionPage{}, err
	}
	if total != seen {
		return RecoveryExecutionPage{}, errors.New("store: recovery report digest count mismatch")
	}
	if offset > total {
		return RecoveryExecutionPage{}, errors.New("store: recovery report offset is beyond the snapshot")
	}
	if objectTooLarge && offset < total {
		return RecoveryExecutionPage{}, errors.New("store: one recovery report object exceeds the requested page byte limit")
	}
	page.ReportDigest = digest
	page.TotalObjects = total
	page.NextOffset = offset + uint64(len(page.Objects))
	return page, nil
}

func (s *Store) scanRecoveryExecutionReport(
	ctx context.Context,
	nodeID string,
	nodeEpoch uint64,
	sourceGeneration string,
	visit func(routesync.RecoveryExecutionFact) error,
) error {
	if nodeID == "" || nodeEpoch == 0 || sourceGeneration == "" || visit == nil {
		return errors.New("store: complete recovery report identity and visitor are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+workflowColumns+` FROM node_workflows
WHERE node_id=? AND node_epoch=?
ORDER BY CASE object_kind WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END,object_id`,
		nodeID, encodeUint64(nodeEpoch), clusterstate.ExecutionKindBuild, clusterstate.ExecutionKindSandbox)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		record, err := scanNodeWorkflow(rows)
		if err != nil {
			return err
		}
		fact, eligible, err := s.recoveryFactTx(ctx, tx, record, sourceGeneration)
		if err != nil {
			return fmt.Errorf("store: recovery report %s/%s: %w", executionKindName(record.Kind), record.ObjectID, err)
		}
		if eligible {
			if err := visit(fact); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecoveryExecutionFact(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	objectID string,
	registryGeneration string,
) (*routesync.RecoveryExecutionFact, error) {
	if objectID == "" || registryGeneration == "" {
		return nil, errors.New("store: recovery object identity is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	record, err := getNodeWorkflowTx(ctx, tx, kind, objectID)
	if err != nil {
		return nil, err
	}
	fact, eligible, err := s.recoveryFactTx(ctx, tx, record, registryGeneration)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return nil, ErrNodeWorkflowState
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &fact, nil
}

func (s *Store) recoveryFactTx(
	ctx context.Context,
	tx *sql.Tx,
	record *nodeexec.WorkflowRecord,
	sourceGeneration string,
) (routesync.RecoveryExecutionFact, bool, error) {
	if record == nil || record.WorkflowFinalized {
		return routesync.RecoveryExecutionFact{}, false, nil
	}
	if err := record.DispatchRecord.Validate(); err != nil {
		return routesync.RecoveryExecutionFact{}, false, err
	}
	binding, err := clusterstate.DecodeExecutionBinding(record.OpaqueBinding)
	if err != nil {
		return routesync.RecoveryExecutionFact{}, false, err
	}
	if binding.RegistryGeneration != sourceGeneration {
		return routesync.RecoveryExecutionFact{}, false, nil
	}
	live, err := s.recoveryObjectBindingMatchesTx(ctx, tx, record)
	if err != nil || !live {
		return routesync.RecoveryExecutionFact{}, false, err
	}
	var snapshot routesync.RecoveryObjectSnapshot
	switch record.Kind {
	case clusterstate.ExecutionKindSandbox:
		if record.LatestEvent == nil {
			return routesync.RecoveryExecutionFact{}, false, nil
		}
		event := *record.LatestEvent
		if err := event.Validate(); err != nil {
			return routesync.RecoveryExecutionFact{}, false, err
		}
		if event.RegistryGeneration != sourceGeneration || event.Binding != record.OpaqueBinding ||
			event.BindingDigest != record.BindingDigest || event.EventSeq != record.EventSeq {
			return routesync.RecoveryExecutionFact{}, false, errors.New("durable event and protected Binding disagree")
		}
		if event.State != string(clusterstate.WorkflowRouteReady) && event.State != string(clusterstate.WorkflowRoutePaused) {
			return routesync.RecoveryExecutionFact{}, false, nil
		}
		if !record.ResourceClaimed || record.AdmissionState != nodeexec.AdmissionRunning {
			return routesync.RecoveryExecutionFact{}, false, errors.New("live Sandbox lacks durable Admission ownership")
		}
		snapshot = recoverySnapshotFromEvent(event)
	case clusterstate.ExecutionKindBuild:
		build, buildErr := s.getBuildTx(ctx, tx, record.ObjectID)
		if buildErr != nil || build == nil {
			return routesync.RecoveryExecutionFact{}, false, buildErr
		}
		switch build.Status {
		case types.BuildRegistered, types.BuildWaiting, types.BuildBuilding, types.BuildReady, types.BuildError:
		default:
			return routesync.RecoveryExecutionFact{}, false, nil
		}
		if !validRecoverableBuildState(record.AdmissionState, record.ResourceClaimed, build.Status) {
			return routesync.RecoveryExecutionFact{}, false, errors.New("Build state and durable Admission state disagree")
		}
		snapshot = routesync.RecoveryObjectSnapshot{
			ObjectKind: "build", ObjectID: build.BuildID, NodeID: record.NodeID, NodeEpoch: record.NodeEpoch,
			RegistryGeneration: sourceGeneration, Binding: record.OpaqueBinding, BindingDigest: record.BindingDigest,
			State: string(build.Status), DataEndpoint: record.DataEndpoint, TemplateRef: build.TemplateID,
			ArtifactRef: build.PersistID, Reason: build.Reason,
		}
	default:
		return routesync.RecoveryExecutionFact{}, false, errors.New("unsupported execution kind")
	}
	fact := routesync.RecoveryExecutionFact{
		Object: snapshot, NormalizedDemand: append([]byte(nil), record.NormalizedDemand...),
		DemandDigest: record.DemandDigest, DispatchSpec: append([]byte(nil), record.DispatchSpec...),
		DispatchSpecDigest: record.DispatchSpecDigest, ProviderPolicyVersion: record.ProviderPolicyVersion,
		AdmissionState: string(record.AdmissionState), ResourceClaimed: record.ResourceClaimed,
	}
	return fact, true, fact.Validate()
}

func validRecoverableBuildState(admission nodeexec.AdmissionState, claimed bool, state types.BuildState) bool {
	switch admission {
	case nodeexec.AdmissionQueued:
		return !claimed && (state == types.BuildRegistered || state == types.BuildWaiting)
	case nodeexec.AdmissionAdmitted:
		return claimed && (state == types.BuildRegistered || state == types.BuildWaiting)
	case nodeexec.AdmissionLaunching:
		return claimed && state == types.BuildWaiting
	case nodeexec.AdmissionRunning:
		return claimed && state == types.BuildBuilding
	case nodeexec.AdmissionTerminal:
		return !claimed && (state == types.BuildReady || state == types.BuildError)
	default:
		return false
	}
}

func recoverySnapshotFromEvent(event routesync.ExecutionEvent) routesync.RecoveryObjectSnapshot {
	return routesync.RecoveryObjectSnapshot{
		ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, NodeID: event.NodeID, NodeEpoch: event.NodeEpoch,
		RegistryGeneration: event.RegistryGeneration, Binding: event.Binding, BindingDigest: event.BindingDigest,
		EventSeq: event.EventSeq, State: event.State, DataEndpoint: event.DataEndpoint,
		TargetPort: event.TargetPort, AccessToken: event.AccessToken,
		TrafficAccessToken: event.TrafficAccessToken, TemplateRef: event.TemplateRef,
		SnapshotRef: event.SnapshotRef, SnapshotLocation: event.SnapshotLocation,
		ArtifactRef: event.ArtifactRef, Reason: event.Reason,
	}
}

func (s *Store) recoveryObjectBindingMatchesTx(ctx context.Context, tx *sql.Tx, record *nodeexec.WorkflowRecord) (bool, error) {
	var metadata map[string]string
	switch record.Kind {
	case clusterstate.ExecutionKindSandbox:
		object, err := s.getSandboxTx(ctx, tx, record.ObjectID)
		if err != nil || object == nil {
			return false, err
		}
		metadata = object.Metadata
	case clusterstate.ExecutionKindBuild:
		object, err := s.getBuildTx(ctx, tx, record.ObjectID)
		if err != nil || object == nil {
			return false, err
		}
		metadata = object.Metadata
	default:
		return false, errors.New("unsupported execution kind")
	}
	_, opaque, err := clusterstate.ExecutionBindingFromMetadata(metadata)
	if err != nil {
		return false, err
	}
	if opaque != record.OpaqueBinding {
		return false, errors.New("object metadata and durable workflow Binding disagree")
	}
	return true, nil
}
