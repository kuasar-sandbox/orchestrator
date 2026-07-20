package store

import (
	"context"
	"database/sql"
	"errors"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
)

func (s *Store) PriorNodeEpochWorkflows(ctx context.Context, nodeID string, currentEpoch uint64) ([]*nodeexec.WorkflowRecord, error) {
	if nodeID == "" || currentEpoch == 0 {
		return nil, errors.New("store: current node identity is required for epoch reset")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+workflowColumns+` FROM node_workflows
WHERE node_id=? AND node_epoch<>? ORDER BY object_kind,object_id`, nodeID, encodeUint64(currentEpoch))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []*nodeexec.WorkflowRecord
	for rows.Next() {
		record, err := scanNodeWorkflow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) ResetPriorNodeEpoch(ctx context.Context, nodeID string, currentEpoch uint64) error {
	if nodeID == "" || currentEpoch == 0 {
		return errors.New("store: current node identity is required for epoch reset")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	args := []any{nodeID, encodeUint64(currentEpoch)}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sandbox_slot_admissions WHERE sandbox_id IN (
SELECT object_id FROM node_workflows WHERE node_id=? AND node_epoch<>? AND object_kind=`+
		`?)`, append(args, clusterstate.ExecutionKindSandbox)...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM sandboxes WHERE id IN (
SELECT object_id FROM node_workflows WHERE node_id=? AND node_epoch<>? AND object_kind=`+
		`?)`, append(args, clusterstate.ExecutionKindSandbox)...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM builds WHERE build_id IN (
SELECT object_id FROM node_workflows WHERE node_id=? AND node_epoch<>? AND object_kind=`+
		`?)`, append(args, clusterstate.ExecutionKindBuild)...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM node_workflows WHERE node_id=? AND node_epoch<>?`, args...); err != nil {
		return err
	}
	return tx.Commit()
}
