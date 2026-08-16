package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ErrBuildExecutionUnfit means a waiting Build can never fit the currently
// configured execution vector. It is distinct from temporary aggregate
// pressure so the FIFO scheduler can terminally reject the row instead of
// blocking every smaller Build behind it.
var ErrBuildExecutionUnfit = errors.New("build cannot fit configured execution limits")

type BuildAdmissionUsage struct {
	RegistrationBuilds int64
	Registration       types.BuildResources
	ExecutionBuilds    int64
	Execution          types.BuildResources
	WaitingBuilds      int64
	OldestWaitingUnix  int64
}

// BuildUsage reconstructs both ledgers exclusively from durable Build rows.
func (s *Store) BuildUsage(ctx context.Context) (BuildAdmissionUsage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status,resources_cpu,resources_memory,resources_storage,
		execution_claimed,waiting_unix FROM builds WHERE status NOT IN (?,?)`,
		string(types.BuildReady), string(types.BuildError))
	if err != nil {
		return BuildAdmissionUsage{}, fmt.Errorf("store: build admission usage: %w", err)
	}
	defer rows.Close()
	var usage BuildAdmissionUsage
	for rows.Next() {
		var status string
		var resources types.BuildResources
		var claimed int
		var waitingUnix int64
		if err := rows.Scan(&status, &resources.CPU, &resources.Memory, &resources.Storage, &claimed, &waitingUnix); err != nil {
			return BuildAdmissionUsage{}, fmt.Errorf("store: build admission usage: %w", err)
		}
		usage.RegistrationBuilds++
		usage.Registration, err = usage.Registration.Add(resources)
		if err != nil {
			return BuildAdmissionUsage{}, fmt.Errorf("store: registration usage: %w", err)
		}
		if types.BuildState(status) == types.BuildWaiting {
			usage.WaitingBuilds++
			if waitingUnix > 0 && (usage.OldestWaitingUnix == 0 || waitingUnix < usage.OldestWaitingUnix) {
				usage.OldestWaitingUnix = waitingUnix
			}
		}
		if claimed != 0 {
			usage.ExecutionBuilds++
			usage.Execution, err = usage.Execution.Add(resources)
			if err != nil {
				return BuildAdmissionUsage{}, fmt.Errorf("store: execution usage: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return BuildAdmissionUsage{}, fmt.Errorf("store: build admission usage: %w", err)
	}
	return usage, nil
}

// ClaimBuildExecution is the execution-admission linearization point. The
// complete vector is checked in the same conditional UPDATE that changes the
// waiting row to building and records durable ownership.
func (s *Store) ClaimBuildExecution(ctx context.Context, buildID string, limit types.BuildAdmissionLimit, now time.Time) (bool, error) {
	b, err := s.GetBuild(ctx, buildID)
	if err != nil {
		return false, err
	}
	if b == nil || b.Status != types.BuildWaiting || b.ExecutionClaimed {
		return false, nil
	}
	if !limit.AllowsOne(b.Resources) {
		return false, fmt.Errorf("%w: %s", ErrBuildExecutionUnfit, buildID)
	}
	headroom := func(configured, requested int64) int64 {
		if configured == 0 {
			return 0
		}
		return configured - requested
	}
	res, err := s.db.ExecContext(ctx, `UPDATE builds
		SET status=?, execution_claimed=1, execution_claimed_unix=?, enforcement_status='pending'
		WHERE build_id=? AND status=? AND execution_claimed=0
		  AND (?=0 OR (SELECT COUNT(*) FROM builds WHERE execution_claimed=1) <= ?)
		  AND (?=0 OR COALESCE((SELECT SUM(resources_cpu) FROM builds WHERE execution_claimed=1),0) <= ?)
		  AND (?=0 OR COALESCE((SELECT SUM(resources_memory) FROM builds WHERE execution_claimed=1),0) <= ?)
		  AND (?=0 OR COALESCE((SELECT SUM(resources_storage) FROM builds WHERE execution_claimed=1),0) <= ?)`,
		string(types.BuildBuilding), now.Unix(), buildID, string(types.BuildWaiting),
		limit.MaxBuilds, headroom(limit.MaxBuilds, 1),
		limit.Resources.CPU, headroom(limit.Resources.CPU, b.Resources.CPU),
		limit.Resources.Memory, headroom(limit.Resources.Memory, b.Resources.Memory),
		limit.Resources.Storage, headroom(limit.Resources.Storage, b.Resources.Storage))
	if err != nil {
		return false, fmt.Errorf("store: claim build %s execution: %w", buildID, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: claim build %s execution rows: %w", buildID, err)
	}
	return changed == 1, nil
}

// BindBuildRun records the run-id only after runtime properties were applied
// and verified. It cannot bind an unclaimed or already-assigned Build.
func (s *Store) BindBuildRun(ctx context.Context, buildID, runID, enforcementStatus string) (bool, error) {
	if runID == "" {
		return false, fmt.Errorf("store: bind build %s: empty run id", buildID)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE builds SET run_id=?,enforcement_status=?
		WHERE build_id=? AND status=? AND execution_claimed=1 AND run_id=''`,
		runID, enforcementStatus, buildID, string(types.BuildBuilding))
	if err != nil {
		return false, fmt.Errorf("store: bind build %s: %w", buildID, err)
	}
	changed, err := res.RowsAffected()
	return changed == 1, err
}

// SetBuildRuntimeOwnership persists host resources acquired after execution
// admission but before assignment. A restart can then recover a live unit's
// result channel and reclaim the exact connector port without rerunning the
// Build. The access token is encrypted with the store key.
func (s *Store) SetBuildRuntimeOwnership(ctx context.Context, buildID, vswitchPort, floatingIP, portMAC, envdAccessToken string) (bool, error) {
	if vswitchPort == "" {
		return false, fmt.Errorf("store: set build %s runtime ownership: empty vswitch port", buildID)
	}
	tokenEnc := ""
	var err error
	if envdAccessToken != "" {
		tokenEnc, err = s.box.EncryptString(envdAccessToken)
		if err != nil {
			return false, fmt.Errorf("store: set build %s runtime ownership: encrypt envd token: %w", buildID, err)
		}
	}
	res, err := s.db.ExecContext(ctx, `UPDATE builds
		SET runtime_vswitch_port=?,runtime_floating_ip=?,runtime_port_mac=?,runtime_envd_access_token_enc=?
		WHERE build_id=? AND status=? AND execution_claimed=1
		  AND runtime_vswitch_port='' AND run_id=''`,
		vswitchPort, floatingIP, portMAC, tokenEnc, buildID, string(types.BuildBuilding))
	if err != nil {
		return false, fmt.Errorf("store: set build %s runtime ownership: %w", buildID, err)
	}
	changed, err := res.RowsAffected()
	return changed == 1, err
}

// ClearBuildRuntimeOwnership records that connector/workdir cleanup completed
// while retaining the execution claim until the terminal Build row commits.
func (s *Store) ClearBuildRuntimeOwnership(ctx context.Context, buildID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE builds
		SET runtime_vswitch_port='',runtime_floating_ip='',runtime_port_mac='',runtime_envd_access_token_enc=''
		WHERE build_id=? AND status=? AND execution_claimed=1`,
		buildID, string(types.BuildBuilding))
	if err != nil {
		return false, fmt.Errorf("store: clear build %s runtime ownership: %w", buildID, err)
	}
	changed, err := res.RowsAffected()
	return changed == 1, err
}

func (s *Store) SetBuildPhase(ctx context.Context, buildID, phase, sandboxID, state string) error {
	var query string
	var args []any
	switch state {
	case "starting":
		query = `UPDATE builds SET phase=?,phase_sandbox_id=?
			WHERE build_id=? AND status=? AND execution_claimed=1
			  AND (phase='' OR (phase=? AND phase_sandbox_id=?))`
		args = []any{phase, sandboxID, buildID, string(types.BuildBuilding), phase, sandboxID}
	case "finished":
		query = `UPDATE builds SET phase='',phase_sandbox_id=''
			WHERE build_id=? AND status=? AND execution_claimed=1
			  AND phase=? AND phase_sandbox_id=?`
		args = []any{buildID, string(types.BuildBuilding), phase, sandboxID}
	case "failed":
		// Keep the failed phase/SID visible until the Build terminal row is
		// committed, but fence stale events from a prior phase.
		query = `UPDATE builds SET phase=phase
			WHERE build_id=? AND status=? AND execution_claimed=1
			  AND phase=? AND phase_sandbox_id=?`
		args = []any{buildID, string(types.BuildBuilding), phase, sandboxID}
	default:
		return fmt.Errorf("store: set build %s phase: unknown state %q", buildID, state)
	}
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: set build %s phase: %w", buildID, err)
	}
	changed, _ := res.RowsAffected()
	if changed != 1 {
		return fmt.Errorf("store: set build %s phase: execution ownership lost", buildID)
	}
	return nil
}

// ExpireBuild atomically terminates an untriggered or queued Build. It returns
// false if another lifecycle transition won first.
func (s *Store) ExpireBuild(ctx context.Context, buildID string, from types.BuildState, reason string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: expire build %s: %w", buildID, err)
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE builds SET status=?,reason=?,execution_claimed=0,
		execution_claimed_unix=0,enforcement_status='',phase='',phase_sandbox_id=''
		WHERE build_id=? AND status=? AND execution_claimed=0`,
		string(types.BuildError), reason, buildID, string(from))
	if err != nil {
		return false, fmt.Errorf("store: expire build %s: %w", buildID, err)
	}
	changed, err := res.RowsAffected()
	if err != nil || changed != 1 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM build_mmds_route_secret_values WHERE build_id=?`, buildID); err != nil {
		return false, fmt.Errorf("store: expire build %s MMDS cleanup: %w", buildID, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: expire build %s: %w", buildID, err)
	}
	return true, nil
}
