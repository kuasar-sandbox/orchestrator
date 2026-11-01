package store

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func resetDeadFixture(t *testing.T, st *Store, id string, automatic bool) (*types.Sandbox, *types.Sandbox) {
	t.Helper()
	ctx := context.Background()
	running := sandboxInsertFixture(id, 0)
	running.AutoReset = automatic
	if err := st.InsertSandbox(ctx, running); err != nil {
		t.Fatal(err)
	}
	result := sandboxRunResult(id, running.RunID, 7)
	if _, err := st.AcceptSandboxExecutionResult(ctx, id, running.RunID, result); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitSandboxDead(ctx, running); err != nil || !changed {
		t.Fatalf("dead: %v %v", changed, err)
	}
	dead, err := st.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return running, dead
}

func TestResetDurableResponsibilityFailureAndExplicitRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reset.db")
	box, _ := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { st.Close() }()
	running, dead := resetDeadFixture(t, st, "automatic-reset", true)
	if dead.ResetState != "pending" || dead.ResetID == "" || !dead.ResetAutomatic || dead.DeadToken == "" || dead.DeadRunID != running.RunID || dead.BaseDir != running.BaseDir || dead.ResumeSource != running.ResumeSource || dead.RunDir != "" || dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("incomplete terminal commit: %+v", dead)
	}
	// No process-local callback has run: reopening must discover this same work.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := st.Get(ctx, dead.ID)
	if err != nil || !reflect.DeepEqual(reopened, dead) {
		t.Fatalf("restart changed terminal responsibility: %+v %v", reopened, err)
	}
	if changed, err := st.BeginSandboxReset(ctx, reopened); err != nil || !changed {
		t.Fatalf("join: %v %v", changed, err)
	}
	joined, _ := st.Get(ctx, dead.ID)
	if joined.ResetID != dead.ResetID || joined.PressureVersion != dead.PressureVersion {
		t.Fatal("manual join created another operation")
	}
	if changed, err := st.FailSandboxReset(ctx, joined, "current disk not initialized"); err != nil || !changed {
		t.Fatalf("fail: %v %v", changed, err)
	}
	failed, _ := st.Get(ctx, dead.ID)
	if failed.State != types.StateDead || failed.ResetState != "failed" || failed.AutoResetError == "" || failed.DeadUnix != dead.DeadUnix || failed.DeadToken != dead.DeadToken || !reflect.DeepEqual(failed.ExecutionResult, dead.ExecutionResult) {
		t.Fatalf("failure lost result/TTL: %+v", failed)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	failedAgain, _ := st.Get(ctx, dead.ID)
	if !reflect.DeepEqual(failed, failedAgain) {
		t.Fatal("confirmed failure changed on restart")
	}
	if changed, err := st.BeginSandboxReset(ctx, failedAgain); err != nil || !changed {
		t.Fatalf("retry: %v %v", changed, err)
	}
	retry, _ := st.Get(ctx, dead.ID)
	if retry.ResetID == failed.ResetID || retry.ResetAutomatic || retry.DeadUnix != dead.DeadUnix {
		t.Fatalf("invalid explicit retry: %+v", retry)
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///base/current.sandbox"}
	if changed, err := st.CommitSandboxReset(ctx, joined, source); err != nil || changed {
		t.Fatalf("failed old operation committed: %v %v", changed, err)
	}
	if changed, err := st.CommitSandboxReset(ctx, retry, source); err != nil || !changed {
		t.Fatalf("commit: %v %v", changed, err)
	}
	paused, _ := st.Get(ctx, dead.ID)
	if paused.State != types.StatePaused || paused.ResumeSource != source || paused.DeadUnix != 0 || paused.ExecutionResult != nil || paused.AutoResetError != "" || paused.BaseDir != running.BaseDir || paused.ResetID != retry.ResetID || paused.ResetState != "succeeded" {
		t.Fatalf("invalid paused commit: %+v", paused)
	}
}

func TestResetDeleteTTLAndTerminalInstanceFences(t *testing.T) {
	for _, winner := range []string{"reset", "delete", "ttl", "successor"} {
		t.Run(winner, func(t *testing.T) {
			st := testStore(t)
			ctx := context.Background()
			_, dead := resetDeadFixture(t, st, "reset-race", true)
			source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///base/current.sandbox"}
			switch winner {
			case "reset":
				if ok, err := st.CommitSandboxReset(ctx, dead, source); err != nil || !ok {
					t.Fatal(ok, err)
				}
				if ok, err := st.DeleteDeadSandboxForRetention(ctx, dead, dead.DeadUnix); err != nil || ok {
					t.Fatal("old TTL deleted Reset", ok, err)
				}
			case "delete", "ttl":
				var ok bool
				var err error
				if winner == "ttl" {
					ok, err = st.DeleteDeadSandboxForRetention(ctx, dead, dead.DeadUnix)
				} else {
					ok, err = st.BeginSandboxDelete(ctx, dead)
				}
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if ok, err = st.CommitSandboxReset(ctx, dead, source); err != nil || ok {
					t.Fatal("Reset resurrected deleting", ok, err)
				}
				deleting, _ := st.Get(ctx, dead.ID)
				if deleting.State != types.StateDeleting || deleting.BaseDir != dead.BaseDir {
					t.Fatal("finalizer lost data owner")
				}
			case "successor":
				// A later terminal instance can have the same SID and second-level
				// timestamp. Its token/version, not the clock, defeats old work.
				if _, err := st.db.ExecContext(ctx, `UPDATE sandboxes SET dead_token='next-terminal',pressure_version=pressure_version+1,reset_id='next-operation' WHERE id=?`, dead.ID); err != nil {
					t.Fatal(err)
				}
				if ok, err := st.CommitSandboxReset(ctx, dead, source); err != nil || ok {
					t.Fatal("old Reset affected successor", ok, err)
				}
				if ok, err := st.DeleteDeadSandboxForRetention(ctx, dead, dead.DeadUnix); err != nil || ok {
					t.Fatal("old TTL affected successor", ok, err)
				}
			}
		})
	}
}

func TestResetDefaultsUnknownExitAndCheckpointOwnership(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("default-reset", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.CommitSandboxDead(ctx, sb); err != nil || !ok {
		t.Fatal(ok, err)
	}
	dead, _ := st.Get(ctx, sb.ID)
	if dead.AutoReset || dead.ResetID != "" || dead.ExecutionResult != nil {
		t.Fatalf("default/unknown exit synthesized: %+v", dead)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO sandbox_checkpoints(sandbox_id,checkpoint_id,source_kind,source_ref,sandbox_ref) VALUES(?,?,?,?,?)`, sb.ID, "checkpoint-A", "sandbox", "file:///base/A.sandbox", ""); err != nil {
		t.Fatal(err)
	}
	before, _ := st.Checkpoint(ctx, sb.ID)
	if ok, err := st.BeginSandboxReset(ctx, dead); err != nil || !ok {
		t.Fatal(ok, err)
	}
	pending, _ := st.Get(ctx, sb.ID)
	if ok, err := st.CommitSandboxReset(ctx, pending, types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///base/B.sandbox"}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	after, _ := st.Checkpoint(ctx, sb.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("Reset consumed or replaced user checkpoint")
	}
}

func TestResetAdditiveUpgradePreservesAllStatesAndTerminalTime(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pre-reset.db")
	box, err := secretbox.NewFromColonHex(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { st.Close() }()
	states := []types.State{types.StateStarting, types.StateRunning, types.StatePausing, types.StatePaused, types.StateDead, types.StateDeleting}
	before := make(map[string]*types.Sandbox)
	for i, state := range states {
		sb := sandboxInsertFixture("upgrade-"+string(state), i)
		sb.State = state
		if state == types.StateStarting {
			sb.LaunchMode = types.LaunchMemory
		}
		if state == types.StateDead {
			sb = retentionDeadSandbox(sb.ID, 42)
		}
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		before[sb.ID], err = st.Get(ctx, sb.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Reconstruct the immediately preceding supported schema, retaining its
	// actual terminal timestamp and all source/credential/lifecycle columns.
	for _, column := range []string{"instance_id", "auto_reset", "dead_run_id", "dead_token", "reset_id", "reset_state", "reset_automatic", "reset_error", "auto_reset_error"} {
		if _, err := st.db.ExecContext(ctx, "ALTER TABLE sandboxes DROP COLUMN "+column); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range before {
		got, err := st.Get(ctx, id)
		if err != nil || got == nil || got.InstanceID == "" || got.AutoReset || got.ResetID != "" {
			t.Fatalf("upgrade default %s: %+v %v", id, got, err)
		}
		if (got.State == types.StateDead) != (got.DeadToken != "") {
			t.Fatalf("terminal identity was not scoped to dead: %+v", got)
		}
		want.InstanceID, want.DeadToken = got.InstanceID, got.DeadToken
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("upgrade changed existing parent %s: got %+v want %+v", id, got, want)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range before {
		got, err := st.Get(ctx, id)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("reopen refreshed migration identity/time %s: %+v %v", id, got, err)
		}
	}
}
