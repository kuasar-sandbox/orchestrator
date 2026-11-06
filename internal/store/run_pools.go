package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// UnbindPreparedRunner is used only before the host publishes any final launch
// spec. No network or VM may have been started. The old unit remains durably
// owned in run_pool_ownership until Stop confirms exit. The caller holds the
// lifecycle lock across Stop and this CAS: only the prepare-stage result from
// that retired unit may be cleared; a start/run result still fences relocation.
func (s *Store) UnbindPreparedRunner(ctx context.Context, sid, runID string) (bool, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET run_id='',sandbox_result_run_id='',sandbox_result_json=''
 WHERE id=? AND run_id=? AND state='starting' AND vswitch_port=''
 AND (sandbox_result_run_id='' OR (sandbox_result_run_id=run_id AND json_extract(sandbox_result_json,'$.stage')='prepare'))`, sid, runID)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("unbind preparation runner", sid, r)
}

func (s *Store) UnbindPreparedBuilder(ctx context.Context, bid, runID string) (bool, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE builds SET run_id='' WHERE build_id=? AND run_id=? AND status='building' AND execution_claimed=1 AND phase='' AND runtime_prepare_json='' AND execution_result_json='' AND cancel_requested_unix=0 AND delete_requested_unix=0`, bid, runID)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// RunPoolOwnership lives from before StartUnit through confirmed exit. It also
// records unassigned prewarm instances, so restart cannot hide their capacity.
type RunPoolOwnership struct {
	RunID          string `json:"run_id"`
	Kind           string `json:"kind"`
	Pool           string `json:"pool"`
	ResourcePool   string `json:"resource_pool"`
	Unit           string `json:"unit"`
	TaskID         string `json:"task_id,omitempty"`
	CapacityMemory uint64 `json:"capacity_memory,omitempty"`
	CapacityCPU    uint64 `json:"capacity_cpu,omitempty"`
}

func (s *Store) SaveRunPoolOwnership(ctx context.Context, r RunPoolOwnership) error {
	if r.RunID == "" || r.Pool == "" || r.ResourcePool == "" || r.Unit == "" {
		return fmt.Errorf("incomplete run pool ownership")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO run_pool_ownership(run_id,ownership) VALUES(?,?) ON CONFLICT(run_id) DO UPDATE SET ownership=excluded.ownership
 WHERE json_extract(run_pool_ownership.ownership,'$.kind')=json_extract(excluded.ownership,'$.kind')
 AND json_extract(run_pool_ownership.ownership,'$.pool')=json_extract(excluded.ownership,'$.pool')
 AND json_extract(run_pool_ownership.ownership,'$.resource_pool')=json_extract(excluded.ownership,'$.resource_pool')
 AND json_extract(run_pool_ownership.ownership,'$.unit')=json_extract(excluded.ownership,'$.unit')`, r.RunID, string(raw))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return fmt.Errorf("run %s has different durable ownership", r.RunID)
	}
	return err
}
func (s *Store) DeleteRunPoolOwnership(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM run_pool_ownership WHERE run_id=?`, runID)
	return err
}
func (s *Store) RunPoolOwnerships(ctx context.Context) ([]RunPoolOwnership, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT ownership FROM run_pool_ownership ORDER BY run_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunPoolOwnership
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r RunPoolOwnership
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
