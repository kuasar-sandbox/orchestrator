package store

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPausedHolderRoleAndSourceCommitAtomically(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	sb := sandboxInsertFixture("holder-commit", 0)
	sb.State = types.StatePausing
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	prepared := RunPoolOwnership{RunID: sb.RunID, Kind: "sandbox", Pool: "runner", ResourcePool: "memory", Unit: "runner.service", TaskID: sb.ID, RetirementOperationID: "capture-1"}
	if err := st.SaveRunPoolOwnership(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	holder := prepared
	holder.Role, holder.MemoryID, holder.MemoryEpoch = RunRoleMemoryHolder, "memory-1", 1
	source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "snapshot-current", SandboxRef: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if _, err := st.db.Exec(`CREATE TRIGGER fail_holder_update BEFORE UPDATE ON run_pool_ownership BEGIN SELECT RAISE(ABORT,'injected holder failure'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitPausedWithRetirement(ctx, sb.ID, sb.RunID, source, "capture-1", &holder); err == nil || changed {
		t.Fatalf("partial commit = %v %v", changed, err)
	}
	current, err := st.Get(ctx, sb.ID)
	if err != nil || current.State != types.StatePausing || current.ResumeSource != sb.ResumeSource {
		t.Fatalf("source committed without holder: %+v %v", current, err)
	}
	if cp, err := st.Checkpoint(ctx, sb.ID); err != nil || cp != nil {
		t.Fatalf("retained checkpoint escaped rollback: %+v %v", cp, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_holder_update`); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.CommitPausedWithRetirement(ctx, sb.ID, sb.RunID, source, "capture-1", &holder); err != nil || !changed {
		t.Fatalf("holder commit = %v %v", changed, err)
	}
	if changed, err := st.ClearPausedRunner(ctx, sb.ID, sb.RunID); err != nil || !changed {
		t.Fatalf("clear runner = %v %v", changed, err)
	}
	if _, err := st.db.Exec(`DELETE FROM sandboxes WHERE id=?`, sb.ID); err != nil {
		t.Fatal(err)
	}
	got, found, err := st.RunPoolOwnership(ctx, sb.RunID)
	if err != nil || !found || got != holder {
		t.Fatalf("sandbox delete lost holder owner: %+v %v %v", got, found, err)
	}
}
