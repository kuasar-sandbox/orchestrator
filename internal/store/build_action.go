package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const BuildCancelledReason = "build cancelled by user"

var ErrBuildDeleteConflict = errors.New("build still has execution or cleanup ownership")

// Shared by explicit deletion and retention. Intent prevents new work, never
// cleanup; terminal status alone is not an ownership-release fence.
const buildOwnerFreeSQL = `execution_claimed=0 AND execution_claimed_unix=0 AND run_id=''
 AND enforcement_status='' AND phase='' AND phase_sandbox_id=''
 AND runtime_vswitch_port='' AND runtime_floating_ip='' AND runtime_port_mac=''
 AND runtime_envd_access_token_enc='' AND runtime_prepare_json='' AND execution_result_json=''`

// RequestBuildAction commits one exact registration's action against the same
// writer lock used by Trigger/claim. The caller authenticates expected first.
// A nil returned Build means that identity no longer exists. Deleted/pending
// describe committed facts, not an in-memory cancellation notification.
func (s *Store) RequestBuildAction(ctx context.Context, expected *types.Build, remove, cancel bool, now time.Time) (build *types.Build, deleted, pending bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, false, err
	}
	defer tx.Rollback()
	build, err = s.scanBuild(tx.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE build_id=? AND template_id=?`, expected.BuildID, expected.TemplateID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	var free bool
	if err = tx.QueryRowContext(ctx, `SELECT `+buildOwnerFreeSQL+` FROM builds WHERE build_id=? AND template_id=?`, build.BuildID, build.TemplateID).Scan(&free); err != nil {
		return nil, false, false, err
	}
	free = free && build.Status != types.BuildBuilding
	if free && (remove || build.DeleteRequestedUnix != 0) {
		if _, err = tx.ExecContext(ctx, `DELETE FROM builds WHERE build_id=? AND template_id=? AND `+buildOwnerFreeSQL, build.BuildID, build.TemplateID); err != nil {
			return nil, false, false, err
		}
		if err = tx.Commit(); err != nil {
			return nil, false, false, err
		}
		return build, true, false, nil
	}
	if free && (build.Status == types.BuildReady || build.Status == types.BuildError) {
		return build, false, false, tx.Commit()
	}
	if remove && !cancel && build.DeleteRequestedUnix == 0 {
		return nil, false, false, ErrBuildDeleteConflict
	}
	stamp := now.Unix()
	if stamp <= 0 {
		return nil, false, false, fmt.Errorf("invalid build action time")
	}
	if build.CancelRequestedUnix == 0 {
		build.CancelRequestedUnix = stamp
	}
	if remove && build.DeleteRequestedUnix == 0 {
		build.DeleteRequestedUnix = stamp
	}
	if _, err = tx.ExecContext(ctx, `UPDATE builds SET cancel_requested_unix=?,delete_requested_unix=? WHERE build_id=? AND template_id=?`, build.CancelRequestedUnix, build.DeleteRequestedUnix, build.BuildID, build.TemplateID); err != nil {
		return nil, false, false, err
	}
	if free {
		build.Status, build.Reason, build.FinishedUnix = types.BuildError, BuildCancelledReason, stamp
		build.Metadata = terminalBuildMetadata(build.Metadata)
		if _, err = tx.ExecContext(ctx, `UPDATE builds SET status=?,reason=?,finished_unix=?,metadata_json=? WHERE build_id=? AND template_id=? AND `+buildOwnerFreeSQL, string(build.Status), build.Reason, stamp, mj(build.Metadata), build.BuildID, build.TemplateID); err != nil {
			return nil, false, false, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM build_mmds_route_secret_values WHERE build_id=?`, build.BuildID); err != nil {
			return nil, false, false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, false, false, err
	}
	return build, false, !free, nil
}

// DeleteRequestedBuild is the post-terminal commit, independent of TTL. Its
// complete identity predicate prevents a stale finalizer deleting a new row.
func (s *Store) DeleteRequestedBuild(ctx context.Context, build *types.Build) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM builds WHERE build_id=? AND template_id=?
 AND delete_requested_unix>0 AND status IN ('ready','error') AND `+buildOwnerFreeSQL, build.BuildID, build.TemplateID)
	if err != nil {
		return false, fmt.Errorf("store: delete requested build %s: %w", build.BuildID, err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) BuildsPendingDeletion(ctx context.Context, limit int) ([]*types.Build, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds WHERE delete_requested_unix>0
 AND status IN ('ready','error') AND `+buildOwnerFreeSQL+` ORDER BY delete_requested_unix,build_id LIMIT ?`, retentionBatchLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var builds []*types.Build
	for rows.Next() {
		build, err := s.scanBuild(rows)
		if err != nil {
			return nil, err
		}
		builds = append(builds, build)
	}
	return builds, rows.Err()
}

// BuildsRequiringRecovery includes every retained ownership shape, including
// terminal rows whose runner or local cleanup was not yet released.
func (s *Store) BuildsRequiringRecovery(ctx context.Context) ([]*types.Build, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds
 WHERE status='building' OR NOT (`+buildOwnerFreeSQL+`) ORDER BY created_unix,build_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var builds []*types.Build
	for rows.Next() {
		build, err := s.scanBuild(rows)
		if err != nil {
			return nil, err
		}
		builds = append(builds, build)
	}
	return builds, rows.Err()
}
