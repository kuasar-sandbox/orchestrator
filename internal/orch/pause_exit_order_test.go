package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Uses the real Orchestrator/Store/ACK/finalizer with a controlled capture helper.
// This is not a real VM test.
func TestPauseExitBarrierOrders(t *testing.T) {
	for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
		for _, outcome := range []string{"success", "capture_failure", "commit_failure"} {
			success := outcome == "success"
			for _, first := range []string{"exit", "pause"} {
				t.Run(fmt.Sprintf("%s/%s/%s_first", kind, outcome, first), func(t *testing.T) {
					installCheckpointSandboxCtl(t)
					ctx := context.Background()
					f := newSandboxFinalizerFixture(t, "pause-exit-barrier")
					f.o.cfg.Checkpoint.Mode = config.CheckpointLocal
					// Keep old ownership after a successful commit so the late result can
					// still be accepted, independent of cleanup speed.
					if success {
						f.lc.stopErr = errors.New("injected cleanup barrier")
					}
					if outcome == "capture_failure" {
						t.Setenv("CHECKPOINT_STDOUT", "invalid capture report")
					}
					if outcome == "commit_failure" {
						installStoreTrigger(t, f.dbPath, `CREATE TRIGGER reject_pause_commit BEFORE UPDATE OF state ON sandboxes WHEN NEW.state='paused' BEGIN SELECT RAISE(ABORT, 'injected commit failure'); END`)
					}
					started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
					t.Setenv("CHECKPOINT_STARTED_FILE", started)
					t.Setenv("CHECKPOINT_RELEASE_FILE", release)
					t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600); f.lc.clearFaults() })
					completed := make(chan error, 1)
					go func() {
						_, err := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: kind})
						completed <- err
					}()
					waitForCheckpointFile(t, started)
					code := 17
					result := types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code}
					post := func() {
						t.Helper()
						if err := f.o.PostSandboxResult(ctx, f.sb.RunID, f.sb.ID, result); err != nil {
							t.Fatalf("result ACK: %v", err)
						}
					}
					if first == "exit" {
						post()
						got, err := f.o.st.Get(ctx, f.sb.ID)
						if err != nil || got.State != types.StatePausing || got.ExecutionResult == nil {
							t.Fatalf("exit must be durable without ending active Pause: %v", err)
						}
					}
					if err := os.WriteFile(release, nil, 0600); err != nil {
						t.Fatal(err)
					}
					pauseErr := waitCheckpointResult(t, completed)
					if success && pauseErr != nil {
						t.Fatalf("completed capture rejected: %v", pauseErr)
					}
					if !success && pauseErr == nil {
						t.Fatal("invalid capture accepted")
					}
					if first == "pause" {
						post()
					}
					if success {
						if err := f.o.finalizeSandboxResultOnce(ctx, f.sb.ID, f.sb.RunID); err != nil {
							t.Fatal(err)
						}
						got, err := f.o.st.Get(ctx, f.sb.ID)
						if err != nil || got.State != types.StatePaused || got.ExecutionResult == nil {
							t.Fatalf("late finalizer changed paused state: %v", err)
						}
						f.lc.clearFaults()
						unlock := f.o.lifecycle.Lock(f.sb.ID)
						got, err = f.o.st.Get(ctx, f.sb.ID)
						if err == nil {
							err = f.o.cleanupPausedOwnership(ctx, got)
						}
						unlock()
						if err != nil {
							t.Fatal(err)
						}
						drainRunEnds(t, f.o)
						got, err = f.o.st.Get(ctx, f.sb.ID)
						if err != nil || got.State != types.StatePaused || got.RunID != "" || got.ExecutionResult == nil {
							t.Fatalf("cleanup lost independent facts: %v", err)
						}
						if err := f.o.ReconcileSandboxes(ctx); err != nil {
							t.Fatal(err)
						}
						got, err = f.o.st.Get(ctx, f.sb.ID)
						if err != nil || got.State != types.StatePaused {
							t.Fatalf("restart changed completed Pause: %v", err)
						}
					} else {
						drainRunEnds(t, f.o)
						got, err := f.o.st.Get(ctx, f.sb.ID)
						if err != nil || got.State != types.StateDead || got.ExecutionResult == nil {
							t.Fatalf("failed Pause lost terminal outcome: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestUncommittedPauseRestart(t *testing.T) {
	for _, exited := range []bool{false, true} {
		t.Run(fmt.Sprint(exited), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "pause-exit-interrupted")
			ctx := context.Background()
			if ok, err := f.o.st.SetPausing(ctx, f.sb.ID, f.sb.RunID, true); err != nil || !ok {
				t.Fatalf("pausing: %v %v", ok, err)
			}
			if exited {
				code := 0
				_, err := f.o.st.AcceptSandboxExecutionResult(ctx, f.sb.ID, f.sb.RunID, types.SandboxExecutionResult{SID: f.sb.ID, RunID: f.sb.RunID, Stage: types.SandboxResultRun, ExitCode: &code})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.ReconcileSandboxes(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := f.o.st.Get(ctx, f.sb.ID)
			if err != nil || got.State != types.StateDead {
				t.Fatalf("uncommitted operation restart: %v", err)
			}
		})
	}
}
