package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrHoldNotMatched = errors.New("hold condition did not match")
	ErrHoldState      = errors.New("sandbox is not the current running instance")
)

// SetHoldValue changes only hold_value. Ordinary sandbox upserts deliberately
// neither insert nor overwrite this field. Each new local parent starts at zero.
// BEGIN IMMEDIATE serializes the comparison, its returned actual value and the
// update even with a second SQLite connection. A zero-to-zero write stays NULL.
func (s *Store) SetHoldValue(ctx context.Context, id, instanceID, runID string, value int64, ifMatch *int64) (actual int64, changed bool, err error) {
	if value < 0 || (ifMatch != nil && *ifMatch < 0) {
		return 0, false, errors.New("negative hold value")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(hold_value,0) FROM sandboxes
		WHERE id=? AND instance_id=? AND run_id=? AND state='running' AND run_id<>''
		AND dead_run_id<>run_id AND sandbox_result_run_id<>run_id`, id, instanceID, runID).Scan(&actual)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, ErrHoldState
	}
	if err != nil {
		return 0, false, fmt.Errorf("read running sandbox hold: %w", err)
	}
	if ifMatch != nil && actual != *ifMatch {
		return actual, false, ErrHoldNotMatched
	}
	if actual == value {
		return actual, false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sandboxes SET hold_value=? WHERE id=?`, value, id); err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return value, true, nil
}
