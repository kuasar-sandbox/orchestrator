package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

var ErrCheckpointLimit = types.ErrCheckpointLimit

// Checkpoints is the complete durable root set owned by this sandbox. Parent
// deletion cascades to every row; these are not independent artifact owners.
func (s *Store) Checkpoints(ctx context.Context, sid string) ([]types.SandboxCheckpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT checkpoint_id,source_kind,source_ref,sandbox_ref FROM sandbox_checkpoints WHERE sandbox_id=? ORDER BY checkpoint_id`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []types.SandboxCheckpoint{}
	for rows.Next() {
		c := types.SandboxCheckpoint{SandboxID: sid}
		if err := rows.Scan(&c.ID, &c.Source.Kind, &c.Source.Ref, &c.Source.SandboxRef); err != nil {
			return nil, err
		}
		if !c.Source.Valid() {
			return nil, errors.New("store: invalid retained checkpoint source")
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Checkpoint selects an exact ID. The optional legacy call is accepted only
// when zero or one checkpoint exists; it never silently selects one of many.
func (s *Store) Checkpoint(ctx context.Context, sid string, id ...string) (*types.SandboxCheckpoint, error) {
	if len(id) == 0 {
		all, err := s.Checkpoints(ctx, sid)
		if err != nil {
			return nil, err
		}
		if len(all) == 0 {
			return nil, nil
		}
		if len(all) != 1 {
			return nil, errors.New("store: checkpoint ID is required")
		}
		return &all[0], nil
	}
	if len(id) != 1 || id[0] == "" {
		return nil, errors.New("store: invalid checkpoint ID")
	}
	c := &types.SandboxCheckpoint{SandboxID: sid}
	err := s.db.QueryRowContext(ctx, `SELECT checkpoint_id,source_kind,source_ref,sandbox_ref FROM sandbox_checkpoints WHERE sandbox_id=? AND checkpoint_id=?`, sid, id[0]).Scan(&c.ID, &c.Source.Kind, &c.Source.Ref, &c.Source.SandboxRef)
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

type checkpointQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func checkCheckpointQuota(ctx context.Context, q checkpointQuerier, sid string) error {
	var limit, count int
	if err := q.QueryRowContext(ctx, `SELECT max_checkpoints,(SELECT count(*) FROM sandbox_checkpoints WHERE sandbox_id=sandboxes.id) FROM sandboxes WHERE id=?`, sid).Scan(&limit, &count); err != nil {
		return err
	}
	if count >= limit {
		return ErrCheckpointLimit
	}
	return nil
}

// CheckCheckpointQuota is the pre-freeze check. CommitPaused repeats it in the
// same immediate transaction as source, state and holder ownership publication.
func (s *Store) CheckCheckpointQuota(ctx context.Context, sid string) error {
	return checkCheckpointQuota(ctx, s.db, sid)
}

func (s *Store) DeleteCheckpoint(ctx context.Context, sid, id string) (bool, error) {
	r, err := s.db.ExecContext(ctx, `DELETE FROM sandbox_checkpoints WHERE sandbox_id=? AND checkpoint_id=?`, sid, id)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("delete checkpoint", sid, r)
}
