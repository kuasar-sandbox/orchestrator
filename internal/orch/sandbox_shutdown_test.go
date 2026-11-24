package orch

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

func TestNormalDeleteLocalctl(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "normal-shutdown")
	beginDeletingForTest(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	serverCtx, cancelServer := context.WithCancel(context.Background())
	server := &ctl.Server{Path: filepath.Join(f.sb.RunDir, "ctl.sock"), ShutdownHandler: func(req ctl.ShutdownRequest) (ctl.ShutdownOperation, error) {
		if req.Target.SandboxID != f.sb.ID || req.Target.RunID != f.sb.RunID {
			t.Errorf("wrong owner: %+v", req.Target)
			return ctl.ShutdownOperation{}, ctl.ErrShutdownTargetMismatch
		}
		if req.DeadlineMonotonicNS != 0 {
			t.Error("physical cleanup timeout truncated normal shutdown")
		}
		return ctl.ShutdownOperation{
			Begin: func() (int64, error) { close(entered); return ctl.MonotonicDeadline(time.Now().Add(time.Minute)) },
			Wait:  func() error { <-release; return nil },
		}, nil
	}}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverCtx) }()
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); cancelServer(); <-serverDone })
	// Use the real ctl.Shutdown client and its accepted/done framing. An
	// accepted request cannot release the runner before actual completion.
	done := make(chan error, 1)
	go func() { done <- f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("shutdown not called")
	}
	state, stops := f.lc.stopSnapshot()
	if state != "active" || stops != 0 {
		t.Fatalf("runner fenced before shutdown: %s/%d", state, stops)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, stops = f.lc.stopSnapshot()
	if stops == 0 {
		t.Fatal("normal shutdown bypassed mandatory runner fence")
	}
}

func TestDeleteShutdownFallbackStillFencesExactRun(t *testing.T) {
	for _, failure := range []error{ctl.ErrShutdownUnsupported, ctl.ErrShutdownTargetMismatch, ctl.ErrShutdownUnavailable, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "shutdown-fallback")
			beginDeletingForTest(t, f)
			calls := 0
			f.o.shutdownSandbox = func(context.Context, string, ctl.ShutdownTarget) error { calls++; return failure }
			if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
				t.Fatal(err)
			}
			_, stops := f.lc.stopSnapshot()
			if calls != 1 || stops == 0 {
				t.Fatalf("shutdown=%d, fences=%d", calls, stops)
			}
		})
	}
}

func TestDeleteNeverThawsFrozenOrLegacyOwnerForShutdown(t *testing.T) {
	for _, state := range []types.State{types.StatePausing, types.StatePaused, types.StateStarting, types.StateDeleting} {
		t.Run(string(state), func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "skip-shutdown")
			f.sb.State, f.sb.LaunchMode = state, ""
			if state == types.StatePaused {
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://" + stringsRepeatA64}
			}
			if state == types.StateStarting {
				f.sb.LaunchMode = types.LaunchImage
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			beginDeletingForTest(t, f)
			f.o.shutdownSandbox = func(context.Context, string, ctl.ShutdownTarget) error {
				t.Error("shutdown called for non-active owner")
				return errors.New("unexpected")
			}
			if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

const stringsRepeatA64 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
