package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// CommitBuildTrigger accepts exactly one immutable trigger payload. The Build
// object and its cluster workflow wake-up become durable in the same transaction.
func (s *Store) CommitBuildTrigger(ctx context.Context, candidate *types.Build, digest string) (bool, error) {
	if candidate == nil || candidate.BuildID == "" || candidate.TemplateID == "" || len(digest) != 64 {
		return false, errors.New("store: complete Build trigger identity and digest are required")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return false, errors.New("store: Build trigger digest is not SHA-256")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	current, err := s.getBuildTx(ctx, tx, candidate.BuildID)
	if err != nil {
		return false, err
	}
	if current == nil {
		return false, errors.New("store: Build trigger target is missing")
	}
	if current.TriggerDigest != "" {
		if current.TriggerDigest != digest {
			return false, ErrBuildTriggerConflict
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	if current.Status != types.BuildRegistered || current.TemplateID != candidate.TemplateID ||
		current.AuthKey != candidate.AuthKey || current.ManifestKey != candidate.ManifestKey ||
		current.Profile != candidate.Profile || current.CPUCount != candidate.CPUCount ||
		current.MemoryMB != candidate.MemoryMB ||
		current.CreatedUnix != candidate.CreatedUnix {
		return false, ErrBuildTriggerConflict
	}
	if current.Metadata[clusterstate.ObjectMetadataKey] != candidate.Metadata[clusterstate.ObjectMetadataKey] {
		return false, ErrNodeWorkflowConflict
	}
	next := cloneBuild(candidate)
	next.TriggerDigest = digest
	next.Status = types.BuildWaiting
	if err := s.putBuild(ctx, tx, next); err != nil {
		return false, err
	}
	workflow, err := getNodeWorkflowTx(ctx, tx, clusterstate.ExecutionKindBuild, candidate.BuildID)
	if err != nil {
		return false, err
	}
	if workflow != nil {
		if workflow.WorkflowFinalized || workflow.ObjectState != string(types.BuildRegistered) ||
			workflow.AdmissionState == nodeexec.AdmissionRejected || workflow.AdmissionState == nodeexec.AdmissionTerminal {
			return false, ErrNodeWorkflowState
		}
		workflow.ObjectState = string(types.BuildWaiting)
		if err := updateNodeWorkflowTx(ctx, tx, workflow); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	if workflow != nil {
		s.notifyWorkflow()
	}
	return true, nil
}
