package store

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestUnbindPreparedRunnerOnlyClearsRetiredPreparationResult(t *testing.T) {
	for _, stage := range []types.SandboxExecutionStage{"", types.SandboxResultPrepare, types.SandboxResultStart, types.SandboxResultRun} {
		t.Run(string(stage), func(t *testing.T) {
			st, ctx := testStore(t), context.Background()
			sb := sandboxInsertFixture("retired-prepare", 0)
			sb.State, sb.RunID, sb.VswitchPort = types.StateStarting, "old-run", ""
			sb.LaunchMode = types.LaunchMemory
			if err := st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
			result := types.SandboxExecutionResult{SID: sb.ID, RunID: sb.RunID, Stage: stage, Error: "stopped"}
			if stage != "" {
				if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); err != nil {
					t.Fatal(err)
				}
			}
			changed, err := st.UnbindPreparedRunner(ctx, sb.ID, sb.RunID)
			if err != nil || changed != (stage == "" || stage == types.SandboxResultPrepare) {
				t.Fatalf("unbind: %v %v", changed, err)
			}
			current, err := st.Get(ctx, sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				if current.RunID != "" || current.ExecutionResult != nil {
					t.Fatal("retired result retained")
				}
				result.Stage = types.SandboxResultPrepare
				if _, err := st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, result); !errors.Is(err, ErrSandboxExecutionOwnership) {
					t.Fatalf("late old result accepted: %v", err)
				}
			} else if current.RunID != sb.RunID || current.ExecutionResult == nil {
				t.Fatal("runtime result was cleared")
			}
		})
	}
}

func TestResourceRecoveryPlacementAtomicWithIntent(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	sb := sandboxInsertFixture("pool-recovery", 0)
	if err := st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	p := RecoveryPlacement{SourcePool: "numa0", DestinationPool: "numa1", CapacityMemory: 1000, CapacityCPU: 2000, Budget: 200}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER reject_placement BEFORE INSERT ON resource_recovery_placement BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.BeginResourcePause(ctx, sb, p); err == nil || ok {
		t.Fatal("failed placement write committed obligation")
	}
	got, err := st.Get(ctx, sb.ID)
	if err != nil || got.ResourceObligation || got.PressureVersion != sb.PressureVersion {
		t.Fatal("failed transaction changed sandbox")
	}
	if _, err := st.db.ExecContext(ctx, `DROP TRIGGER reject_placement`); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.BeginResourcePause(ctx, sb, p); err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	if got, err := st.RecoveryPlacement(ctx, sb.ID); err != nil || got != p {
		t.Fatalf("saved placement %+v %v", got, err)
	}
	if ok, err := st.CancelResourcePause(ctx, sb); err != nil || ok {
		t.Fatal("stale cancellation changed intent")
	}
	current, err := st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.CancelResourcePause(ctx, current); err != nil || !ok {
		t.Fatalf("cancel %v %v", ok, err)
	}
	if got, err := st.RecoveryPlacement(ctx, sb.ID); err != nil || got != (RecoveryPlacement{}) {
		t.Fatal("cancelled intent retained stale cross-pool destination")
	}
}

func TestResourcePoolPressureJournalIsIndependentAndMonotonic(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	critical := NodePressure{Zone: "critical", Version: 8, Reason: "sustained_memory_demand", SinceUnix: 123}
	if err := st.SavePoolPressure(ctx, "numa0", critical); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePoolPressure(ctx, "numa1", NodePressure{Zone: "green", Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePoolPressure(ctx, "numa0", NodePressure{Zone: "green", Version: 7}); err != nil {
		t.Fatal(err)
	}
	if got, err := st.LoadPoolPressure(ctx, "numa0"); err != nil || got != critical {
		t.Fatalf("stale pressure overwritten: %+v %v", got, err)
	}
	if got, err := st.LoadPoolPressure(ctx, "numa1"); err != nil || got.Zone != "green" {
		t.Fatal("healthy pool pressure overwritten")
	}
}

func TestRunPoolOwnershipCannotChangePoolOrUnit(t *testing.T) {
	st, ctx := testStore(t), context.Background()
	m := RunPoolOwnership{RunID: "run", Kind: "sandbox", Pool: "runner0", ResourcePool: "numa0", Unit: "r0@run.service"}
	if err := st.SaveRunPoolOwnership(ctx, m); err != nil {
		t.Fatal(err)
	}
	bound := m
	bound.TaskID = "sid"
	bound.CapacityMemory = 1000
	if err := st.SaveRunPoolOwnership(ctx, bound); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*RunPoolOwnership){func(m *RunPoolOwnership) { m.Pool = "runner1" }, func(m *RunPoolOwnership) { m.ResourcePool = "numa1" }, func(m *RunPoolOwnership) { m.Unit = "other@run.service" }} {
		changed := bound
		mutate(&changed)
		if err := st.SaveRunPoolOwnership(ctx, changed); err == nil {
			t.Fatal("running ownership changed")
		}
	}
	rows, err := st.RunPoolOwnerships(ctx)
	if err != nil || len(rows) != 1 || rows[0] != bound {
		t.Fatalf("ownership=%+v %v", rows, err)
	}
}
