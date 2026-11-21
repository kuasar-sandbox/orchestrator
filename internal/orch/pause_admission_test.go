package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func pendingPauseFixture(t *testing.T) sandboxFinalizerFixture {
	t.Helper()
	f := newSandboxFinalizerFixture(t, "pending-memory")
	var err error
	f.sb, err = f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func captureStateFor(q ctl.CaptureStateQuery, pending bool) ctl.CaptureState {
	return ctl.CaptureState{LifecycleVersion: ctl.CheckpointLifecycleVersion, SandboxID: q.SandboxID, InstanceID: q.InstanceID, RunID: q.RunID, Pending: pending, MemoryID: "previous", Epoch: 1}
}

func TestPauseAdmissionDoesNotWaitForOlderHolders(t *testing.T) {
	for _, noWait := range []bool{false, true} {
		f := pendingPauseFixture(t)
		calls := 0
		f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
			calls++
			if !q.ForCapture || !q.NoWaitForPreviousMemorySave {
				t.Fatal("issued blocking save query")
			}
			return captureStateFor(q, true), nil
		}
		ctx, unlock, sb, err := f.o.lockPauseAdmission(context.Background(), f.sb.ID, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, NoWaitForPreviousMemorySave: noWait}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pending, err := f.o.captureMemorySavePending(ctx, sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, true); err != nil || pending {
			t.Fatalf("locked recheck gated capture: %t %v", pending, err)
		}
		unlock()
		if calls != 1 {
			t.Fatalf("capability queries=%d", calls)
		}
	}
}

func TestPauseAdmissionRejectsOldLifecycleAndWrongRuntime(t *testing.T) {
	for _, mode := range []string{"old-version", "wrong-run", "read-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := pendingPauseFixture(t)
			f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
				state := captureStateFor(q, true)
				switch mode {
				case "old-version":
					state.LifecycleVersion = 1
				case "wrong-run":
					state.RunID = "successor"
				case "read-failure":
					return state, errors.New("disconnected")
				}
				return state, nil
			}
			_, unlock, _, err := f.o.lockPauseAdmission(context.Background(), f.sb.ID, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, nil)
			if unlock != nil {
				unlock()
			}
			if err == nil {
				t.Fatal("unsafe admission succeeded")
			}
			current, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || current.State != types.StateRunning {
				t.Fatalf("failed admission changed state: %+v %v", current, err)
			}
		})
	}
}

func TestOldRuntimeUnsupportedIsNotPendingMemoryReadiness(t *testing.T) {
	f := pendingPauseFixture(t)
	f.o.captureStateReader = func(context.Context, string, ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		return ctl.CaptureState{}, ctl.ErrCaptureStateUnsupported
	}
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0 // durable intent, not a fabricated ready state
	if err := f.o.st.SaveRunPoolOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, err := f.o.previousMemorySavePending(context.Background(), f.sb, true); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("unsupported with older pending capture = %v", err)
	}
}

func TestCaptureVersionCheckPrecedesExtensionHook(t *testing.T) {
	f := pendingPauseFixture(t)
	f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		state := captureStateFor(q, true)
		state.LifecycleVersion = 1
		return state, nil
	}
	f.o.SetExtensionHooks(sandboxHookFunc(func(context.Context, *conductorextension.SandboxOperation) error {
		t.Error("hook ran before version check")
		return nil
	}), nil)
	if _, err := f.o.Pause(context.Background(), f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}); err == nil {
		t.Fatal("old lifecycle accepted")
	}
}

func TestResumedRuntimeMustExplicitlySupportPendingMemory(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, m)
	next := cloneSandbox(f.sb)
	next.RunID = "successor-run"
	for _, mode := range []string{"unsupported", "unknown", "wrong-run", "old-version", "supported"} {
		t.Run(mode, func(t *testing.T) {
			f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
				switch mode {
				case "unsupported":
					return ctl.CaptureState{}, ctl.ErrCaptureStateUnsupported
				case "unknown":
					return ctl.CaptureState{}, errors.New("disconnected")
				case "wrong-run":
					q.RunID = m.RunID
				}
				state := captureStateFor(q, true)
				if mode == "old-version" {
					state.LifecycleVersion = 1
				}
				return state, nil
			}
			err := f.o.requirePreviousMemorySaveSupport(context.Background(), next)
			if (err == nil) != (mode == "supported") {
				t.Fatalf("%s resume capability = %v", mode, err)
			}
		})
	}
}

func TestCaptureMemoryWaitTracksNewRetentionNotCheckpointRetentionOption(t *testing.T) {
	for _, retained := range []bool{false, true} {
		f := pendingPauseFixture(t)
		called := false
		f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
			called = true
			if !q.ForCapture {
				t.Error("capture admission lost runtime eligibility check")
			}
			return captureStateFor(q, true), nil
		}
		pending, err := f.o.captureMemorySavePending(context.Background(), f.sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, Checkpoint: retained}, true)
		if err != nil || pending || !called {
			t.Fatalf("snapshot retention=%t bypassed wait: %t %v", retained, pending, err)
		}
		called = false
		pending, err = f.o.captureMemorySavePending(context.Background(), f.sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox, Checkpoint: retained}, true)
		if err != nil || pending || called {
			t.Fatalf("disk retention=%t entered memory wait: %t %v", retained, pending, err)
		}
	}
}

func TestCaptureMemoryWaitSkipsKnownLegacyParentOrRuntime(t *testing.T) {
	for _, legacy := range []string{"parent", "runtime"} {
		t.Run(legacy, func(t *testing.T) {
			f := pendingPauseFixture(t)
			if legacy == "parent" {
				f.o.runSessions = map[runSessionKey]*runSessionGeneration{{kind: runKindSandbox, runID: f.sb.RunID}: {}}
			}
			f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
				if legacy == "parent" {
					t.Error("legacy parent queried new retention admission")
				}
				return ctl.CaptureState{}, ctl.ErrCaptureStateUnsupported
			}
			if pending, err := f.o.captureMemorySavePending(context.Background(), f.sb, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}, true); err != nil || pending {
				t.Fatalf("synchronous fallback entered memory wait: %t %v", pending, err)
			}
		})
	}
}

func TestCaptureSynchronousFallbackPreservesPreviousMemoryWithoutNewIntent(t *testing.T) {
	args := installCheckpointSandboxCtl(t)
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	f.o.cfg.Checkpoint.Mode = config.CheckpointLocal
	old := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, old)
	f.sb.RunID = "sr-00000000-0000-7000-8000-000000000288"
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	current := store.RunPoolOwnership{RunID: f.sb.RunID, Kind: runKindSandbox, TaskID: f.sb.ID, Pool: old.Pool, ResourcePool: old.ResourcePool, Unit: "sandbox-runner@" + f.sb.RunID + ".service"}
	if err := f.o.st.SaveRunPoolOwnership(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	// The new runner knows retirement, but its runtime/controller cannot
	// retain RAM. The old holder still owns a genuine pending generation.
	f.o.runSessions = map[runSessionKey]*runSessionGeneration{{kind: runKindSandbox, runID: f.sb.RunID}: {holderRetirement: true}}
	calls := 0
	f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		calls++
		if !q.ForCapture {
			t.Fatal("not a capture capability probe")
		}
		state := captureStateFor(q, false)
		supported := false
		state.RetentionSupported = &supported
		return state, nil
	}
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, NoWaitForPreviousMemorySave: true}
	ctx, unlock, sb, err := f.o.lockPauseAdmission(context.Background(), f.sb.ID, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if pending, err := f.o.captureMemorySavePending(ctx, sb, request, true); err != nil || pending {
		t.Fatalf("recheck blocked fallback: %t %v", pending, err)
	}
	source, err := f.o.snapshotLocal(ctx, sb, request)
	if err != nil || source.Kind != types.ResumeSourceSnapshot {
		t.Fatalf("synchronous fallback: %+v %v", source, err)
	}
	if calls != 1 {
		t.Fatalf("runtime capability queried %d times under one lifecycle lock", calls)
	}
	if _, err := os.Stat(args); err != nil {
		t.Fatalf("capture command was not executed: %v", err)
	}
	got, found, err := f.o.st.RunPoolOwnership(context.Background(), sb.RunID)
	if err != nil || !found || got.RetirementOperationID != "" {
		t.Fatalf("fallback retained new intent: %+v %v", got, err)
	}
	if pending, err := f.o.retainedMemoryPending(context.Background(), sb.ID); err != nil || !pending {
		t.Fatalf("fallback erased old memory responsibility: %t %v", pending, err)
	}
	if _, err := os.Stat(filepath.Join(f.sb.BaseDir, "owned")); err != nil {
		t.Fatal(err)
	}
}
