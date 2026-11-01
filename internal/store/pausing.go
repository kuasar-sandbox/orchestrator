package store

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SetPausing journals the acknowledged gate before capture. A terminal result
// received concurrently wins; rollback never clears that result's ownership.
func (s *Store) SetPausing(ctx context.Context, id, runID string, pausing bool) (bool, error) {
	from, to := types.StateRunning, types.StatePausing
	if !pausing {
		from, to = to, from
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=? WHERE id=? AND run_id=? AND state=? AND sandbox_result_run_id=''`, string(to), id, runID, string(from))
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("set pausing", id, r)
}
