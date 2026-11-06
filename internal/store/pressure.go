package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// BeginResourcePause journals the obligation before any capture I/O. The SID
// lifecycle fence belongs to orch; this exact-run CAS also protects recovery.
func (s *Store) BeginResourcePause(ctx context.Context, sb *types.Sandbox, placements ...RecoveryPlacement) (bool, error) {
	if sb == nil || sb.State != types.StateRunning || sb.RunID == "" {
		return false, errors.New("resource pause requires an owned running sandbox")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE sandboxes SET pause_reason='resource-pressure', resource_obligation=1, pressure_version=pressure_version+1, pressure_since_ns=?
 WHERE id=? AND state='running' AND run_id=? AND resource_obligation=0 AND pressure_version=? AND sandbox_result_run_id=''`, time.Now().UnixNano(), sb.ID, sb.RunID, sb.PressureVersion)
	if err != nil {
		return false, err
	}
	changed, err := sandboxUpdateChanged("begin resource pause", sb.ID, r)
	if err != nil || !changed {
		return changed, err
	}
	if len(placements) > 0 {
		raw, err := json.Marshal(placements[0])
		if err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_recovery_placement(sandbox_id,placement) VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET placement=excluded.placement`, sb.ID, string(raw)); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// CancelResourcePause only cancels an uncommitted capture intent. A committed
// paused source or a starting resume can never be cleared by a late failure.
func (s *Store) CancelResourcePause(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil {
		return false, errors.New("missing resource pause owner")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET pause_reason='', resource_obligation=0, pressure_version=pressure_version+1
 WHERE id=? AND state='running' AND run_id=? AND resource_obligation=1 AND pause_reason='resource-pressure' AND pressure_version=?`, sb.ID, sb.RunID, sb.PressureVersion)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("cancel resource pause", sb.ID, r)
}

// AdoptResourcePause changes intent without touching source, runner/network
// cleanup ownership, running charge, credentials, or logical sandbox identity.
func (s *Store) AdoptResourcePause(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || !sb.ResumeSource.Valid() {
		return false, errors.New("resource pause has no valid saved source")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET pause_reason='explicit', resource_obligation=0, pressure_version=pressure_version+1
 WHERE id=? AND state='paused' AND run_id=? AND resource_obligation=1 AND pause_reason='resource-pressure' AND pressure_version=?
 AND resume_source_kind=? AND resume_source_ref=? AND resume_sandbox_ref=?`, sb.ID, sb.RunID, sb.PressureVersion, string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.ResumeSource.SandboxRef)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("adopt resource pause", sb.ID, r)
}

// NodePressure is the small low-frequency controller journal in the existing
// node database. Resource transactions and heartbeats do not write it.
type NodePressure struct {
	Zone      string
	Version   uint64
	Reason    string
	SinceUnix int64
}

func (s *Store) LoadNodePressure(ctx context.Context) (NodePressure, error) {
	var p NodePressure
	err := s.db.QueryRowContext(ctx, `SELECT zone,version,reason,since_unix FROM node_pressure WHERE singleton=1`).Scan(&p.Zone, &p.Version, &p.Reason, &p.SinceUnix)
	if errors.Is(err, sql.ErrNoRows) {
		return NodePressure{}, nil
	}
	return p, err
}

func (s *Store) SaveNodePressure(ctx context.Context, p NodePressure) error {
	switch p.Zone {
	case "green", "yellow", "red", "critical":
	default:
		return fmt.Errorf("invalid pressure zone %q", p.Zone)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO node_pressure(singleton,zone,version,reason,since_unix) VALUES(1,?,?,?,?)
 ON CONFLICT(singleton) DO UPDATE SET zone=excluded.zone,version=excluded.version,reason=excluded.reason,since_unix=excluded.since_unix WHERE excluded.version>=node_pressure.version`, p.Zone, p.Version, p.Reason, p.SinceUnix)
	return err
}

// InitializeRunningInterval gives an adopted legacy VM a conservative local
// running interval once. Schema migration itself never rewrites business rows.
func (s *Store) InitializeRunningInterval(ctx context.Context, id, runID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET running_since_ns=? WHERE id=? AND run_id=? AND state='running' AND running_since_ns=0`, time.Now().UnixNano(), id, runID)
	return err
}
