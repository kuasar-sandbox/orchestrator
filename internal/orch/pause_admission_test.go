package orch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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
	return ctl.CaptureState{SandboxID: q.SandboxID, InstanceID: q.InstanceID, RunID: q.RunID, Pending: pending, MemoryID: "previous", Epoch: 1}
}

func TestPausePreviousMemoryDefaultWaitIsCancelableAndKeepsRunning(t *testing.T) {
	f := pendingPauseFixture(t)
	entered := make(chan struct{})
	var once sync.Once
	f.o.captureStateReader = func(ctx context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		if q.NoWaitForPreviousMemorySave {
			return captureStateFor(q, true), nil
		}
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctl.CaptureState{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("pause did not enter default wait")
	}
	locked := make(chan error, 1)
	go func() {
		unlock := f.o.lifecycle.Lock(f.sb.ID)
		defer unlock()
		_, release, err := f.o.trySourceOperation(context.Background(), f.sb)
		if err == nil {
			release()
		}
		locked <- err
	}()
	select {
	case err := <-locked:
		if err != nil {
			t.Fatalf("source grant was retained: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SID lock was retained during wait")
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StateRunning {
		t.Fatalf("waiting changed running state: %+v %v", current, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pause cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait ignored request cancellation")
	}
}

func TestPausePreviousMemoryNoWaitAndAllLockedCallersRejectBeforeIntent(t *testing.T) {
	f := pendingPauseFixture(t)
	f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		if !q.NoWaitForPreviousMemorySave {
			t.Error("no-wait caller issued a waiting query")
		}
		return captureStateFor(q, true), nil
	}
	request := sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, NoWaitForPreviousMemorySave: true}
	if _, err := f.o.Pause(context.Background(), f.sb.ID, f.apiKey, request); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("public no-wait = %v", err)
	}
	if err := f.o.pauseSandbox(context.Background(), f.sb); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("TTL no-wait = %v", err)
	}
	intent := false
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	_, err := f.o.pauseSandboxLocked(context.Background(), f.sb, request, func() error { intent = true; return nil })
	unlock()
	if !errors.Is(err, api.ErrPreviousMemorySavePending) || intent {
		t.Fatalf("locked admission = %v, intent=%v", err, intent)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StateRunning {
		t.Fatalf("no-wait changed state: %+v %v", current, err)
	}
}

func TestPauseWaitCannotCaptureSuccessorRun(t *testing.T) {
	f := pendingPauseFixture(t)
	entered, ready := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.o.captureStateReader = func(ctx context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		if q.NoWaitForPreviousMemorySave {
			return captureStateFor(q, true), nil
		}
		once.Do(func() { close(entered) })
		select {
		case <-ready:
			return captureStateFor(q, false), nil
		case <-ctx.Done():
			return ctl.CaptureState{}, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, unlock, _, err := f.o.lockPauseAdmission(ctx, f.sb.ID, sandboxcfg.CaptureRequest{}, nil)
		if unlock != nil {
			unlock()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("wait not entered")
	}
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	next := cloneSandbox(f.sb)
	next.RunID = "successor-run"
	if err := f.o.st.Delete(context.Background(), next.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.Put(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	unlock()
	close(ready)
	select {
	case err := <-done:
		if !errors.Is(err, api.ErrPauseRunChanged) {
			t.Fatalf("successor admission = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not revalidate identity")
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

func TestPauseNoWaitPrecedesExtensionHook(t *testing.T) {
	f := pendingPauseFixture(t)
	f.o.captureStateReader = func(_ context.Context, _ string, q ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		return captureStateFor(q, true), nil
	}
	f.o.SetExtensionHooks(sandboxHookFunc(func(context.Context, *conductorextension.SandboxOperation) error {
		t.Error("extension hook ran before previous save admission")
		return nil
	}), nil)
	_, err := f.o.Pause(context.Background(), f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, NoWaitForPreviousMemorySave: true})
	if !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("hook no-wait = %v", err)
	}
}

func TestResumedRuntimeMustExplicitlySupportPendingMemory(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, m)
	next := cloneSandbox(f.sb)
	next.RunID = "successor-run"
	for _, mode := range []string{"unsupported", "unknown", "wrong-run", "supported"} {
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
				return captureStateFor(q, true), nil
			}
			err := f.o.requirePreviousMemorySaveSupport(context.Background(), next)
			if (err == nil) != (mode == "supported") {
				t.Fatalf("%s resume capability = %v", mode, err)
			}
		})
	}
}
