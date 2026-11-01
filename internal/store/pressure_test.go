package store

import (
	"context"
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestResourcePauseAdoptionPreservesSourceAndCleanup(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	sb := sandboxInsertFixture("pressure-adoption", 0)
	sb.RunningSinceUnixNano = 123
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.BeginResourcePause(ctx, sb); err != nil || !ok {
		t.Fatalf("intent: %v %v", ok, err)
	}
	intent, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !intent.ResourceObligation || intent.PauseReason != types.PauseReasonResource {
		t.Fatal("intent was not durable")
	}
	if ok, err := st.SetPausing(ctx, sb.ID, sb.RunID, true); err != nil || !ok {
		t.Fatalf("gate: %v %v", ok, err)
	}
	if ok, err := st.CommitPaused(ctx, sb.ID, sb.RunID, sb.ResumeSource, ""); err != nil || !ok {
		t.Fatalf("capture: %v %v", ok, err)
	}
	if ok, err := st.CancelResourcePause(ctx, intent); err != nil || ok {
		t.Fatalf("late failure erased paused obligation: %v %v", ok, err)
	}
	paused, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.RunningSinceUnixNano != 0 || paused.RunID != sb.RunID {
		t.Fatal("pause lost cleanup ownership")
	}
	if ok, err := st.AdoptResourcePause(ctx, intent); err != nil || ok {
		t.Fatalf("stale version adopted: %v %v", ok, err)
	}
	if ok, err := st.AdoptResourcePause(ctx, paused); err != nil || !ok {
		t.Fatalf("adopt: %v %v", ok, err)
	}
	adopted, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := *paused
	want.PauseReason, want.ResourceObligation = types.PauseReasonExplicit, false
	want.PressureVersion++
	if !reflect.DeepEqual(adopted, &want) {
		t.Fatal("adoption changed source, identity or cleanup ownership")
	}
	if ok, err := st.AdoptResourcePause(ctx, paused); err != nil || ok {
		t.Fatalf("duplicate adoption: %v %v", ok, err)
	}
}

func TestResourceAdoptionFailureAndSourceCAS(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	sb := sandboxInsertFixture("pressure-failure", 0)
	sb.State, sb.PauseReason, sb.ResourceObligation, sb.PressureVersion = types.StatePaused, types.PauseReasonResource, true, 4
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	wrong := *sb
	wrong.ResumeSource.Ref = "different-snapshot"
	if ok, err := st.AdoptResourcePause(ctx, &wrong); err != nil || ok {
		t.Fatalf("wrong source: %v %v", ok, err)
	}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER reject_adoption BEFORE UPDATE OF resource_obligation ON sandboxes BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.AdoptResourcePause(ctx, sb); err == nil || ok {
		t.Fatalf("failed commit succeeded: %v %v", ok, err)
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || !reflect.DeepEqual(got, sb) {
		t.Fatal("failed adoption changed truth")
	}
}

func TestNodePressureJournalRejectsOlderTransition(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	critical := NodePressure{Zone: "critical", Version: 8, Reason: "sustained_memory_demand", SinceUnix: 123}
	if err := st.SaveNodePressure(ctx, critical); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveNodePressure(ctx, NodePressure{Zone: "green", Version: 7}); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadNodePressure(ctx)
	if err != nil || got != critical {
		t.Fatalf("stale episode overwrite: %+v %v", got, err)
	}
}
