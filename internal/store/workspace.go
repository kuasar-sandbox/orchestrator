package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// WorkspaceDefault is the first default selected for this slot in this run.
// Other sources already identify their disk explicitly; the owner arbitrates
// concurrent binds and records the actual device state and private writer.
type WorkspaceDefault struct {
	Disk      rtconfig.DiskV1
	Locations rtconfig.RefLocations
}

var ErrWorkspaceGeneration = errors.New("workspace run changed")

func (s *Store) WorkspaceDefault(ctx context.Context, id, instance, run, name string, candidate *WorkspaceDefault) (*WorkspaceDefault, error) {
	// The primary key bounds storage to one intent per slot; deleting the
	// parent also deletes all intents. A resume can never replay an old intent.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var current string
	err = tx.QueryRowContext(ctx, `SELECT run_id FROM sandboxes WHERE id=? AND instance_id=? AND state='running' AND run_id<>'' AND run_id<>dead_run_id AND run_id<>sandbox_result_run_id`, id, instance).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) || err == nil && current != run {
		return nil, ErrWorkspaceGeneration
	}
	if err != nil {
		return nil, err
	}
	aad, _ := json.Marshal([]string{"workspace-default", id, instance, run, name})
	var enc string
	err = tx.QueryRowContext(ctx, `SELECT source_enc FROM workspace_defaults WHERE sandbox_id=? AND name=? AND run_id=?`, id, name, run).Scan(&enc)
	if err == nil {
		raw, err := s.box.DecryptAAD(enc, aad)
		if err != nil {
			return nil, fmt.Errorf("decrypt workspace default: %w", err)
		}
		var fixed WorkspaceDefault
		err = json.Unmarshal(raw, &fixed)
		return &fixed, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if candidate == nil {
		return nil, nil
	}
	raw, err := json.Marshal(candidate)
	if err != nil {
		return nil, err
	}
	enc, err = s.box.EncryptAAD(raw, aad)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO workspace_defaults(sandbox_id,name,run_id,source_enc) VALUES(?,?,?,?)
	 ON CONFLICT(sandbox_id,name) DO UPDATE SET run_id=excluded.run_id,source_enc=excluded.source_enc`, id, name, run, enc)
	if err != nil {
		return nil, err
	}
	return candidate, tx.Commit()
}
