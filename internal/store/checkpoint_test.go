package store

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRetainedCheckpointAtomicQuotaAndParentOwnership(t *testing.T) {
	ctx, st := context.Background(), testStore(t)
	sb := sandboxInsertFixture("retained", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.SetPausing(ctx, sb.ID, sb.RunID, true); err != nil || !ok {
		t.Fatalf("gate=%v %v", ok, err)
	}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://first.sandbox"}
	if _, err := st.db.Exec(`CREATE TRIGGER fail_retained BEFORE INSERT ON sandbox_checkpoints BEGIN SELECT RAISE(ABORT,'retain failed'); END`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, source, "first"); err == nil || ok {
		t.Fatalf("failed atomic retain=%v %v", ok, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got.State != types.StatePausing || got.ResumeSource != sb.ResumeSource {
		t.Fatalf("torn pause=%+v %v", got, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER fail_retained`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, source, "first"); err != nil || !ok {
		t.Fatalf("retain=%v %v", ok, err)
	}
	// A later run preserves the capture and cannot overwrite its one-slot quota.
	got.State = types.StateRunning
	if err := st.Put(ctx, got); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.SetPausing(ctx, sb.ID, sb.RunID, true); err != nil || !ok {
		t.Fatalf("gate=%v %v", ok, err)
	}
	second := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://second.sandbox"}
	if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, second, "second"); err == nil || ok {
		t.Fatalf("quota overwritten=%v %v", ok, err)
	}
	retained, err := st.Checkpoint(ctx, sb.ID)
	if err != nil || retained == nil || retained.ID != "first" || retained.Source != source {
		t.Fatalf("retained=%+v %v", retained, err)
	}
	if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, second, ""); err != nil || !ok {
		t.Fatalf("ordinary pause blocked by quota=%v %v", ok, err)
	}
	if err := st.Delete(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	retained, err = st.Checkpoint(ctx, sb.ID)
	if err != nil || retained != nil {
		t.Fatalf("checkpoint outlived parent=%+v %v", retained, err)
	}
}
