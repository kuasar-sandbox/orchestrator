package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// WorkspaceDefault is the first default selected for the slot in this sandbox instance.
// Other sources already identify their disk explicitly; the owner arbitrates
// concurrent binds and records the actual device state and private writer.
type WorkspaceDefault struct {
	Disk      rtconfig.DiskV1
	Locations rtconfig.RefLocations
}

var ErrWorkspaceUnavailable = errors.New("workspace sandbox is not the current running instance")

func (s *Store) WorkspaceDefault(ctx context.Context, id, instance, name string, candidate *WorkspaceDefault) (*WorkspaceDefault, error) {
	// The primary key bounds storage to one intent per slot; deleting the
	// parent also deletes all intents. Resume reuses the same disk selection.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var found int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM sandboxes WHERE id=? AND instance_id=? AND state='running'`, id, instance).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrWorkspaceUnavailable
	}
	if err != nil {
		return nil, err
	}
	aad, _ := json.Marshal([]string{"workspace-default", id, instance, name})
	var enc string
	err = tx.QueryRowContext(ctx, `SELECT source_enc FROM workspace_defaults WHERE sandbox_id=? AND name=?`, id, name).Scan(&enc)
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
	_, err = tx.ExecContext(ctx, `INSERT INTO workspace_defaults(sandbox_id,name,source_enc) VALUES(?,?,?)`, id, name, enc)
	if err != nil {
		return nil, err
	}
	return candidate, tx.Commit()
}
