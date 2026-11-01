package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func randomLifecycleID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// MarkSandboxRunEnded retains a confirmed exact-run end fact before cleanup.
// It does not fabricate an execution result or start dead TTL/automatic Reset.
func (s *Store) MarkSandboxRunEnded(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.State != types.StateRunning || sb.RunID == "" || sb.InstanceID == "" {
		return false, errors.New("store: missing exact running identity")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET dead_run_id=run_id
	 WHERE id=? AND instance_id=? AND state='running' AND run_id=? AND pressure_version=?`,
		sb.ID, sb.InstanceID, sb.RunID, sb.PressureVersion)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("record ended run", sb.ID, r)
}

// BeginSandboxReset joins an existing pending operation; only an explicit new
// manual admission can replace a failed operation. Automatic responsibility is
// created exclusively by CommitSandboxDead, never by scans or callbacks.
func (s *Store) BeginSandboxReset(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.State != types.StateDead || sb.InstanceID == "" || sb.DeadToken == "" {
		return false, errors.New("store: Reset requires an exact dead parent")
	}
	if sb.ResetState == "pending" {
		return true, nil
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET reset_id=?,reset_state='pending',reset_automatic=0,reset_error='',pressure_version=pressure_version+1
	 WHERE id=? AND instance_id=? AND state='dead' AND dead_token=? AND pressure_version=? AND reset_state<>'pending'`,
		randomLifecycleID(), sb.ID, sb.InstanceID, sb.DeadToken, sb.PressureVersion)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("begin reset", sb.ID, r)
}

// CommitSandboxReset publishes E and paused together. Delete/TTL or another
// terminal incarnation invalidates this operation even if SID/deadAt coincide.
func (s *Store) CommitSandboxReset(ctx context.Context, sb *types.Sandbox, source types.ResumeSource) (bool, error) {
	if sb == nil || sb.State != types.StateDead || sb.ResetState != "pending" || sb.ResetID == "" || source.Kind != types.ResumeSourceSandbox || !source.Valid() {
		return false, errors.New("store: invalid reset commit")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state='paused',resume_source_kind=?,resume_source_ref=?,resume_sandbox_ref='',
	 dead_unix=0,dead_token='',dead_run_id='',sandbox_result_run_id='',sandbox_result_json='',
	 reset_state='succeeded',reset_automatic=0,reset_error='',auto_reset_error='',pressure_version=pressure_version+1
	 WHERE id=? AND instance_id=? AND state='dead' AND dead_token=? AND pressure_version=? AND reset_id=? AND reset_state='pending'`,
		string(source.Kind), source.Ref, sb.ID, sb.InstanceID, sb.DeadToken, sb.PressureVersion, sb.ResetID)
	if err != nil {
		return false, err
	}
	return sandboxUpdateChanged("commit reset", sb.ID, r)
}

func (s *Store) FailSandboxReset(ctx context.Context, sb *types.Sandbox, reason string) (bool, error) {
	if sb == nil || sb.ResetID == "" || sb.ResetState != "pending" {
		return false, errors.New("store: missing Reset operation")
	}
	if reason == "" {
		return false, errors.New("store: missing Reset failure")
	}
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	r, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET reset_state='failed',reset_error=?,
	 auto_reset_error=CASE WHEN reset_automatic=1 THEN ? ELSE auto_reset_error END,pressure_version=pressure_version+1
	 WHERE id=? AND instance_id=? AND state='dead' AND dead_token=? AND pressure_version=? AND reset_id=? AND reset_state='pending'`,
		reason, reason, sb.ID, sb.InstanceID, sb.DeadToken, sb.PressureVersion, sb.ResetID)
	if err != nil {
		return false, fmt.Errorf("store: fail Reset: %w", err)
	}
	return sandboxUpdateChanged("fail reset", sb.ID, r)
}
