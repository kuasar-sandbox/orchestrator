package store

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// TerminalRetentionBatchLimit bounds every reaper pass. The conductor runs one
// pass per interval; it never turns terminal retention into an unbounded table
// transaction or an automatic VACUUM.
const TerminalRetentionBatchLimit = 128

func retentionBatchLimit(limit int) int {
	if limit <= 0 || limit > TerminalRetentionBatchLimit {
		return TerminalRetentionBatchLimit
	}
	return limit
}

// DeadSandboxesForRetention returns at most limit fully-cleaned diagnostic dead
// rows. The caller serializes each candidate with lifecycle admission before
// invoking DeleteDeadSandboxForRetention.
func (s *Store) DeadSandboxesForRetention(ctx context.Context, cutoffUnix int64, limit int) ([]*types.Sandbox, error) {
	if cutoffUnix <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+cols+` FROM sandboxes
		WHERE state=? AND dead_unix>0 AND dead_unix<=?
		  AND launch_mode='' AND run_id=''
		  AND floatingip='' AND vswitch_port='' AND inner_ip='' AND port_mac=''
		  AND run_dir='' AND base_dir='' AND envd_uds='' AND ci_uds=''
		  AND resume_source_kind='' AND resume_source_ref=''
		ORDER BY dead_unix ASC,id ASC LIMIT ?`,
		string(types.StateDead), cutoffUnix, retentionBatchLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("store: scan dead sandbox retention: %w", err)
	}
	var candidates []*types.Sandbox
	for rows.Next() {
		sandbox, scanErr := s.scan(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		candidates = append(candidates, sandbox)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: scan dead sandbox retention: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: close dead sandbox retention scan: %w", err)
	}

	return candidates, nil
}

// DeleteDeadSandboxForRetention removes the exact candidate only if it is still
// due and owner-free. Every cleanup ownership column remains in the predicate,
// so a changed or corrupt row fails closed instead of losing a local owner.
func (s *Store) DeleteDeadSandboxForRetention(ctx context.Context, sandbox *types.Sandbox, cutoffUnix int64) (bool, error) {
	if sandbox == nil || sandbox.ID == "" || cutoffUnix <= 0 {
		return false, nil
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM sandboxes
		WHERE id=? AND state=? AND dead_unix=? AND dead_unix<=?
		  AND launch_mode='' AND run_id=''
		  AND floatingip='' AND vswitch_port='' AND inner_ip='' AND port_mac=''
		  AND run_dir='' AND base_dir='' AND envd_uds='' AND ci_uds=''
		  AND resume_source_kind='' AND resume_source_ref=''`,
		sandbox.ID, string(types.StateDead), sandbox.DeadUnix, cutoffUnix)
	if err != nil {
		return false, fmt.Errorf("store: prune dead sandbox %s: %w", sandbox.ID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: prune dead sandbox %s rows: %w", sandbox.ID, err)
	}
	return changed == 1, nil
}

// TerminalBuildsForRetention returns at most limit fully-finalized ready/error
// rows. The caller serializes each candidate with Build lifecycle admission
// before invoking DeleteTerminalBuildForRetention.
func (s *Store) TerminalBuildsForRetention(ctx context.Context, cutoffUnix int64, limit int) ([]*types.Build, error) {
	if cutoffUnix <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds
		WHERE status IN (?,?) AND finished_unix>0 AND finished_unix<=?
		  AND execution_claimed=0 AND execution_claimed_unix=0 AND run_id=''
		  AND enforcement_status='' AND phase='' AND phase_sandbox_id=''
		  AND runtime_vswitch_port='' AND runtime_floating_ip='' AND runtime_port_mac=''
		  AND runtime_envd_access_token_enc='' AND runtime_prepare_json='' AND execution_result_json=''
		ORDER BY finished_unix ASC,build_id ASC LIMIT ?`,
		string(types.BuildReady), string(types.BuildError), cutoffUnix, retentionBatchLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("store: scan terminal build retention: %w", err)
	}
	var candidates []*types.Build
	for rows.Next() {
		build, scanErr := s.scanBuild(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		candidates = append(candidates, build)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: scan terminal build retention: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: close terminal build retention scan: %w", err)
	}

	return candidates, nil
}

// DeleteTerminalBuildForRetention removes the exact candidate only while it is
// still due, terminal, and free of all execution/runtime/result ownership.
func (s *Store) DeleteTerminalBuildForRetention(ctx context.Context, build *types.Build, cutoffUnix int64) (bool, error) {
	if build == nil || build.BuildID == "" || cutoffUnix <= 0 {
		return false, nil
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM builds
		WHERE build_id=? AND status=? AND finished_unix=? AND finished_unix<=?
		  AND execution_claimed=0 AND execution_claimed_unix=0 AND run_id=''
		  AND enforcement_status='' AND phase='' AND phase_sandbox_id=''
		  AND runtime_vswitch_port='' AND runtime_floating_ip='' AND runtime_port_mac=''
		  AND runtime_envd_access_token_enc='' AND runtime_prepare_json='' AND execution_result_json=''`,
		build.BuildID, string(build.Status), build.FinishedUnix, cutoffUnix)
	if err != nil {
		return false, fmt.Errorf("store: prune terminal build %s: %w", build.BuildID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: prune terminal build %s rows: %w", build.BuildID, err)
	}
	return changed == 1, nil
}
