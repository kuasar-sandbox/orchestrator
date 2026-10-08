package orch

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type cgroupCleanupLauncher struct {
	*sandboxFinalizerLauncher
	pruneErr error
	pruned   []string
}

func (l *cgroupCleanupLauncher) PruneSandboxCgroup(_ context.Context, unit string) error {
	l.pruned = append(l.pruned, unit)
	return l.pruneErr
}

func TestSandboxResultRetainsOwnershipUntilDelegatedCgroupRemoved(t *testing.T) {
	for _, missingUnit := range []bool{false, true} {
		name := "stopped-unit"
		if missingUnit {
			name = "collected-unit-after-restart"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newSandboxFinalizerFixture(t, "cgroup-"+name)
			unit := fixture.lc.unit
			if missingUnit {
				fixture.lc.unit = ""
			} else {
				fixture.o.runs.restore(fixture.sb.RunID, unit)
			}
			fault := errors.New("delegated cgroup is still busy")
			lc := &cgroupCleanupLauncher{sandboxFinalizerLauncher: fixture.lc, pruneErr: fault}
			fixture.o.lc = lc
			result := types.SandboxExecutionResult{SID: fixture.sb.ID, RunID: fixture.sb.RunID, Stage: types.SandboxResultRun, Error: "runtime exited"}
			ctx := context.Background()
			if accepted, err := fixture.o.st.AcceptSandboxExecutionResult(ctx, fixture.sb.ID, fixture.sb.RunID, result); err != nil || !accepted {
				t.Fatalf("accept result = %t, %v", accepted, err)
			}
			if err := fixture.o.finalizeSandboxResultOnce(ctx, fixture.sb.ID, fixture.sb.RunID); !errors.Is(err, fault) {
				t.Fatalf("cleanup committed without removing delegated cgroup: %v", err)
			}
			stored, err := fixture.o.st.Get(ctx, fixture.sb.ID)
			if err != nil || stored == nil || stored.State != types.StateRunning || stored.RunID != fixture.sb.RunID || stored.ExecutionResult == nil || stored.VswitchPort != fixture.sb.VswitchPort {
				t.Fatalf("cleanup failure lost retry ownership/result: %+v, %v", stored, err)
			}
			for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("cleanup failure removed owned directory: %v", err)
				}
			}
			lc.pruneErr = nil
			if err := fixture.o.finalizeSandboxResultOnce(ctx, fixture.sb.ID, fixture.sb.RunID); err != nil {
				t.Fatalf("cleanup retry: %v", err)
			}
			dead, err := fixture.o.st.Get(ctx, fixture.sb.ID)
			if err != nil || dead == nil || dead.State != types.StateDead || dead.RunID != "" || dead.ExecutionResult == nil || dead.ExecutionResult.RunID != fixture.sb.RunID {
				t.Fatalf("successful cleanup did not preserve dead result: %+v, %v", dead, err)
			}
			if len(lc.pruned) != 2 || lc.pruned[0] != unit || lc.pruned[1] != unit {
				t.Fatalf("pruned unexpected unit(s): %v", lc.pruned)
			}
		})
	}
}
