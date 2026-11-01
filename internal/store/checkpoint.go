package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Checkpoint returns the sandbox's single retained capture. Parent deletion
// cascades to this row; it never becomes an independent artifact owner.
func (s *Store) Checkpoint(ctx context.Context, sid string) (*types.SandboxCheckpoint, error) {
	c := &types.SandboxCheckpoint{SandboxID: sid}
	err := s.db.QueryRowContext(ctx, `SELECT checkpoint_id,source_kind,source_ref,sandbox_ref FROM sandbox_checkpoints WHERE sandbox_id=?`, sid).Scan(&c.ID, &c.Source.Kind, &c.Source.Ref, &c.Source.SandboxRef)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !c.Source.Valid() {
		return nil, errors.New("store: invalid retained checkpoint source")
	}
	return c, nil
}

// DeleteCheckpoint consumes exactly the immutable capture accepted by the
// caller. The parent still owns physical cleanup of its artifact directory.
func (s *Store) DeleteCheckpoint(ctx context.Context, sid, id string) (bool, error) {
	r, err := s.db.ExecContext(ctx, `DELETE FROM sandbox_checkpoints WHERE sandbox_id=? AND checkpoint_id=?`, sid, id)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("delete checkpoint", sid, r)
}
