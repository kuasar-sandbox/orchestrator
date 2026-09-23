package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func sandboxRunResult(sid, runID string, code int) types.SandboxExecutionResult {
	return types.SandboxExecutionResult{SID: sid, RunID: runID, Stage: types.SandboxResultRun, ExitCode: &code}
}

func TestAcceptSandboxExecutionResultIsIdempotentAndRejectsConflicts(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("sandbox-result", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, "run-stale", sandboxRunResult(sb.ID, "run-stale", 7)); !errors.Is(err, ErrSandboxExecutionOwnership) {
		t.Fatalf("stale AcceptSandboxExecutionResult = %v", err)
	}
	result := sandboxRunResult(sb.ID, sb.RunID, 7)
	inserted, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result)
	if err != nil || !inserted {
		t.Fatalf("AcceptSandboxExecutionResult = %t, %v", inserted, err)
	}
	inserted, err = st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result)
	if err != nil || inserted {
		t.Fatalf("duplicate AcceptSandboxExecutionResult = %t, %v", inserted, err)
	}
	changed := sandboxRunResult(sb.ID, sb.RunID, 8)
	if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, changed); !errors.Is(err, ErrSandboxResultConflict) {
		t.Fatalf("conflicting AcceptSandboxExecutionResult = %v", err)
	}
}

func TestSandboxExecutionResultBlocksLateRunningCommits(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	fresh := sandboxInsertFixture("result-before-running", 1)
	fresh.State = types.StateStarting
	fresh.ResumeSource = types.ResumeSource{}
	fresh.LaunchMode = types.LaunchImage
	fresh.RunID = "run-fresh"
	if err := st.InsertSandbox(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, fresh.ID, fresh.RunID, sandboxRunResult(fresh.ID, fresh.RunID, 1)); err != nil || !inserted {
		t.Fatalf("accept fresh result = %t, %v", inserted, err)
	}
	if changed, err := st.CommitStartingRunning(ctx, fresh.ID, fresh.RunID); err != nil || changed {
		t.Fatalf("CommitStartingRunning after result = %t, %v; want CAS miss", changed, err)
	}

	prepared := sandboxInsertFixture("prepared-result-before-running", 2)
	prepared.State = types.StateStarting
	prepared.ResumeSource = types.ResumeSource{}
	prepared.TemplateID = types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindSbx, Ref: "manifest://" + strings.Repeat("3", 64)}.String()
	prepared.LaunchMode = types.LaunchCold
	prepared.RunID = "run-prepared"
	if err := st.InsertSandbox(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, prepared.ID, prepared.RunID, sandboxRunResult(prepared.ID, prepared.RunID, 2)); err != nil || !inserted {
		t.Fatalf("accept prepared result = %t, %v", inserted, err)
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + strings.Repeat("3", 64)}
	if changed, err := st.CommitPreparedRunning(ctx, prepared, source); err != nil || changed {
		t.Fatalf("CommitPreparedRunning after result = %t, %v; want CAS miss", changed, err)
	}
}

func TestSandboxExecutionResultSurvivesDeadCommitAndReopen(t *testing.T) {
	dir := t.TempDir()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test.db")
	st, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sb := sandboxInsertFixture("result-dead-reopen", 3)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	result := sandboxRunResult(sb.ID, sb.RunID, 9)
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil || !inserted {
		t.Fatalf("accept result = %t, %v", inserted, err)
	}
	if changed, err := st.CommitSandboxDead(ctx, sb); err != nil || !changed {
		t.Fatalf("CommitSandboxDead = %t, %v", changed, err)
	}
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil || inserted {
		t.Fatalf("replay after dead = %t, %v", inserted, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, box)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.State != types.StateDead || loaded.ExecutionResult == nil || loaded.ExecutionResult.RunID != sb.RunID || loaded.ExecutionResult.ExitCode == nil || *loaded.ExecutionResult.ExitCode != 9 {
		t.Fatalf("reopened result = %+v", loaded)
	}
}

func TestGetClaimedSandboxIDByRunIDReplaysOnlyLiveStartingAssignment(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("assignment-replay", 4)
	sb.State = types.StateStarting
	sb.ResumeSource = types.ResumeSource{}
	sb.LaunchMode = types.LaunchImage
	sb.RunID = "run-replay"
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	got, found, err := st.GetClaimedSandboxIDByRunID(ctx, sb.RunID)
	if err != nil || !found || got != sb.ID {
		t.Fatalf("GetClaimedSandboxIDByRunID = %q, %t, %v", got, found, err)
	}
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, sandboxRunResult(sb.ID, sb.RunID, 1)); err != nil || !inserted {
		t.Fatalf("accept result = %t, %v", inserted, err)
	}
	got, found, err = st.GetClaimedSandboxIDByRunID(ctx, sb.RunID)
	if err != nil || found || got != "" {
		t.Fatalf("replay after result = %q, %t, %v; want absent", got, found, err)
	}
}
