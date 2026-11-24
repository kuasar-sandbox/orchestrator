package store

import (
	"context"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestDeleteShutdownIntentIsAtomicAndOnlyForActiveRun(t *testing.T) {
	for _, state := range []types.State{types.StateRunning, types.StateStarting, types.StatePausing, types.StatePaused, types.StateDead, types.StateDeleting} {
		t.Run(string(state), func(t *testing.T) {
			st := testStore(t)
			ctx := context.Background()
			sb := sandboxInsertFixture("shutdown-intent", 0)
			sb.State, sb.LaunchMode = state, ""
			if state == types.StateDead {
				sb = retentionDeadSandbox("shutdown-intent", 1)
			}
			if state == types.StateStarting {
				sb.LaunchMode = types.LaunchCold
			}
			if err := st.InsertSandbox(ctx, sb); err != nil {
				t.Fatal(err)
			}
			current, err := st.Get(ctx, sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := st.BeginSandboxDelete(ctx, current); !ok || err != nil {
				t.Fatalf("accept = %t, %v", ok, err)
			}
			deleting, err := st.Get(ctx, sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if state == types.StateRunning {
				want = sb.RunID
			}
			if deleting.DeleteShutdownRunID != want {
				t.Fatalf("intent=%q, want %q", deleting.DeleteShutdownRunID, want)
			}
			if ok, err := st.BeginSandboxDelete(ctx, deleting); !ok || err != nil {
				t.Fatalf("repeat = %t, %v", ok, err)
			}
			repeated, _ := st.Get(ctx, sb.ID)
			if repeated.DeleteShutdownRunID != want {
				t.Fatal("repeat changed the accepted generation")
			}
		})
	}
}

func TestDeleteAndExecutionResultRaceNeverInventsActiveShutdown(t *testing.T) {
	for range 10 {
		st := testStore(t)
		ctx := context.Background()
		sb := sandboxInsertFixture("shutdown-result-race", 0)
		sb.State, sb.LaunchMode = types.StateRunning, ""
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		sb, _ = st.Get(ctx, sb.ID)
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); _, _ = st.BeginSandboxDelete(ctx, sb) }()
		go func() {
			defer group.Done()
			_, _ = st.AcceptSandboxExecutionResult(ctx, sb.ID, sb.RunID, sandboxRunResult(sb.ID, sb.RunID, 0))
		}()
		group.Wait()
		deleting, err := st.Get(ctx, sb.ID)
		if err != nil || deleting == nil {
			t.Fatalf("read: %v", err)
		}
		if deleting.DeleteShutdownRunID != "" && deleting.DeleteShutdownRunID != sb.RunID {
			t.Fatalf("accepted another run: %+v", deleting)
		}
		if deleting.ExecutionResult != nil && deleting.DeleteShutdownRunID != "" {
			t.Fatal("delete accepted shutdown after the result was already durable")
		}
		// A result that loses to deleting is rejected by its existing exact-run
		// gate. If it wins, SQL records no unnecessary normal-shutdown action.
		if deleting.State != types.StateDeleting {
			t.Fatalf("delete lost accepted lifecycle: %s", deleting.State)
		}
	}
}

func TestDeleteShutdownIntentRejectsStaleGeneration(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	old := sandboxInsertFixture("shutdown-stale", 0)
	if err := st.InsertSandbox(ctx, old); err != nil {
		t.Fatal(err)
	}
	current, err := st.Get(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.RunID = "replacement-run"
	if err := st.Put(ctx, current); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.BeginSandboxDelete(ctx, old); err != nil || ok {
		t.Fatalf("stale delete: %t, %v", ok, err)
	}
	current, _ = st.Get(ctx, old.ID)
	if current.DeleteShutdownRunID != "" || current.State != types.StateRunning {
		t.Fatalf("stale delete altered successor: %+v", current)
	}
	if ok, err := st.BeginSandboxDelete(ctx, current); err != nil || !ok {
		t.Fatalf("current delete: %t, %v", ok, err)
	}
	deleting, _ := st.Get(ctx, old.ID)
	if deleting.DeleteShutdownRunID != "replacement-run" {
		t.Fatalf("accepted wrong generation: %q", deleting.DeleteShutdownRunID)
	}
}

func TestColdResumeDeleteRaceNeverMarksPausedSourceForShutdown(t *testing.T) {
	for range 10 {
		st := testStore(t)
		ctx := context.Background()
		sb := sandboxInsertFixture("shutdown-resume-race", 0)
		sb.State, sb.LaunchMode = types.StatePaused, ""
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC = "", "", "", "", ""
		sb.RunDir, sb.EnvdUDS, sb.CiUDS = "", "", ""
		if err := st.InsertSandbox(ctx, sb); err != nil {
			t.Fatal(err)
		}
		sb, _ = st.Get(ctx, sb.ID)
		var deleted, resumed bool
		var deleteErr, resumeErr error
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); deleted, deleteErr = st.BeginSandboxDelete(ctx, sb) }()
		go func() {
			defer group.Done()
			resumed, resumeErr = st.BeginResume(ctx, sb.ID, 200, types.LaunchCold, "/run/replacement", "", "")
		}()
		group.Wait()
		if deleteErr != nil || resumeErr != nil || deleted == resumed {
			t.Fatalf("delete=%t/%v resume=%t/%v", deleted, deleteErr, resumed, resumeErr)
		}
		current, err := st.Get(ctx, sb.ID)
		if err != nil || current.DeleteShutdownRunID != "" {
			t.Fatalf("frozen source received shutdown intent: %+v, %v", current, err)
		}
	}
}
