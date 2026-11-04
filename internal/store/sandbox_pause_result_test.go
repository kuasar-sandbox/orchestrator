package store

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCommitPausedPreservesAcceptedResult(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	sb := sandboxInsertFixture("pause-result-cas", 0)
	sb.State = types.StatePausing
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	code := 23
	result := types.SandboxExecutionResult{SID: sb.ID, RunID: sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
	if inserted, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil || !inserted {
		t.Fatalf("accept terminal result = %t, %v", inserted, err)
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if changed, err := st.CommitPaused(ctx, sb.ID, sb.RunID, source, ""); err != nil || !changed {
		t.Fatalf("pause after accepted result = %t, %v, want commit", changed, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got == nil || got.State != types.StatePaused || got.ResumeSource != source || got.ExecutionResult == nil || got.ExecutionResult.ExitCode == nil || *got.ExecutionResult.ExitCode != code {
		t.Fatalf("pause lost the independent runner result: %+v, %v", got, err)
	}
}
