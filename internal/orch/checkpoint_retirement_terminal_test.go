package orch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
)

func terminalRetirementRestart(t *testing.T, acceptedResult bool) (sandboxFinalizerFixture, *Orchestrator, store.RunPoolOwnership, *memorycheckpoint.RecordWriter) {
	t.Helper()
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	f.o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	m := retiredTestOwnership(t, f)
	w := testCaptureRecord(t, f, m)
	// Persist the running successor separately from the old holder. The new
	// conductor must reconstruct both owners from disk without a live session.
	f.sb = cloneSandbox(f.sb)
	f.sb.RunID = "sr-00000000-0000-7000-8000-000000000288"
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if acceptedResult {
		code := 137
		result := types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
		if inserted, err := f.o.st.AcceptSandboxExecutionResult(context.Background(), f.sb.ID, f.sb.RunID, result); err != nil || !inserted {
			t.Fatalf("accept successor result: inserted=%t err=%v", inserted, err)
		}
	}
	restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
	restarted.lc, restarted.vs = f.lc, f.vs
	lifecycle, cancel := context.WithCancel(context.Background())
	restarted.SetLifecycleContext(lifecycle)
	t.Cleanup(func() { cancel(); drainRunEnds(t, restarted) })
	return f, restarted, m, w
}

func TestReconcileTerminalSuccessorDefersPreviousMemoryRetirement(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		kind := "missing-run"
		if accepted {
			kind = "accepted-result"
		}
		for _, saved := range []bool{false, true} {
			stage := "save-pending"
			if saved {
				stage = "saved-controller-pending"
			}
			t.Run(kind+"/"+stage, func(t *testing.T) {
				f, restarted, m, w := terminalRetirementRestart(t, accepted)
				finishSave := func() {
					t.Helper()
					if err := w.Update(func(r *memorycheckpoint.CaptureRecord) {
						r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
					}); err != nil {
						t.Fatal(err)
					}
				}
				if saved {
					finishSave()
				}
				assertPending := func() {
					t.Helper()
					row, err := restarted.st.Get(context.Background(), f.sb.ID)
					if err != nil || row == nil || row.State != types.StateRunning || row.RunID != f.sb.RunID || row.RunDir != f.sb.RunDir || row.BaseDir != f.sb.BaseDir || (row.ExecutionResult != nil) != accepted {
						t.Fatalf("pending successor ownership changed: err=%v", err)
					}
					if !accepted && row.DeadRunID != f.sb.RunID {
						t.Fatal("missing successor did not retain its known end fact")
					}
					if _, found, err := restarted.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
						t.Fatalf("pending predecessor lost ownership: found=%t err=%v", found, err)
					}
					for _, path := range []string{w.Path(), f.sb.RunDir, f.sb.BaseDir} {
						if _, err := os.Stat(path); err != nil {
							t.Fatalf("pending cleanup removed owned path: %v", err)
						}
					}
					if _, stops := f.lc.stopSnapshot(); stops != 0 {
						t.Fatalf("pending retirement stopped holder: %d", stops)
					}
					workerKey := f.sb.RunID
					if !accepted {
						workerKey = resultCleanupKey(f.sb.ID, f.sb.RunID)
					}
					restarted.runEndMu.Lock()
					_, retrying := restarted.runEndActive[workerKey]
					restarted.runEndMu.Unlock()
					if !retrying {
						t.Fatal("startup did not retain the successor's existing cleanup worker")
					}
				}
				// This is conductor's real startup order: reconciliation precedes
				// configuring the controller and opening its inventory/listener.
				if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
					t.Fatalf("previous memory retirement blocked conductor startup: %v", err)
				}
				assertPending()
				state := nodectl.NewState(4<<30, 4000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
				admission := nodectl.NewAdmissionController(nodectl.AdmissionPolicy{Rate: 100, Burst: 100})
				if err := restarted.ConfigureMemoryPressure(context.Background(), state, admission, nodectl.DefaultPressurePolicy()); err != nil {
					t.Fatal(err)
				}
				if state.InventoryReady() {
					t.Fatal("configured controller became ready before Listen")
				}
				if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
					t.Fatalf("unrecovered controller blocked startup: %v", err)
				}
				assertPending()
				listenResourceStateForTest(t, state)
				if !saved {
					if err := restarted.ensureRetiredMemoryReleased(context.Background(), f.sb.ID); !errors.Is(err, api.ErrPreviousMemorySavePending) {
						t.Fatalf("ready controller bypassed unfinished save: %v", err)
					}
					assertPending()
					finishSave()
				}
				// No second reconciliation or explicit cleanup call is needed: the
				// worker retained by the startup pass must complete automatically.
				drainRunEnds(t, restarted)
				dead, err := restarted.st.Get(context.Background(), f.sb.ID)
				if err != nil || dead == nil || dead.State != types.StateDead || dead.RunID != "" || dead.RunDir != "" || dead.BaseDir != f.sb.BaseDir || (dead.ExecutionResult != nil) != accepted {
					t.Fatalf("automatic terminal retry lost final history: err=%v", err)
				}
				if accepted && dead.ExecutionResult.RunID != f.sb.RunID {
					t.Fatal("terminal retry changed the accepted result identity")
				}
				if _, found, err := restarted.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
					t.Fatalf("released predecessor retained ownership: found=%t err=%v", found, err)
				}
				assertTerminatedDataRetained(t, f.sb)
			})
		}
	}
}

func TestReconcileTerminalRetirementReadErrorStillFails(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		name := "missing-run"
		if accepted {
			name = "accepted-result"
		}
		t.Run(name, func(t *testing.T) {
			f, restarted, m, w := terminalRetirementRestart(t, accepted)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(w.Path(), []byte("invalid capture record"), 0600); err != nil {
				t.Fatal(err)
			}
			var syntaxError *json.SyntaxError
			if err := restarted.ReconcileSandboxes(context.Background()); !errors.As(err, &syntaxError) {
				t.Fatalf("capture read failure was hidden as pending: %v", err)
			}
			if _, found, err := restarted.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
				t.Fatalf("failed recovery lost holder proof: found=%t err=%v", found, err)
			}
			if _, err := os.Stat(f.sb.RunDir); err != nil {
				t.Fatal(err)
			}
		})
	}
}
