package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type exitedRunnerLauncher struct {
	reconcileLauncher
	empty      bool
	proofErr   error
	afterProof func()
}

func (l *exitedRunnerLauncher) UnitEmpty(context.Context, string) (bool, error) {
	if l.afterProof != nil {
		l.afterProof()
	}
	return l.empty, l.proofErr
}

func TestReapExitedRunner(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		unitState                    string
		empty                        bool
		wantDead                     bool
		proofErr                     error
		cancelProof                  bool
		cancelLifecycleDuringCleanup bool
		staleRun                     bool
	}{
		{name: "CH exits then runner exits", unitState: "failed", empty: true, wantDead: true},
		{name: "sandbox ctl exits first", unitState: "inactive", empty: true, wantDead: true},
		{name: "runner still active", unitState: "active", empty: true},
		{name: "runner cgroup still populated", unitState: "failed"},
		{name: "proof query fails", unitState: "failed", proofErr: errors.New("proof unavailable")},
		{name: "proof budget expires before slow cleanup", unitState: "failed", empty: true, wantDead: true, cancelProof: true},
		{name: "shutdown drains accepted cleanup", unitState: "failed", empty: true, wantDead: true, cancelLifecycleDuringCleanup: true},
		{name: "old candidate after new run", unitState: "failed", empty: true, staleRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(filepath.Join(t.TempDir(), "node.db"), box)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			root := t.TempDir()
			cfg := &config.Config{}
			cfg.Paths.RunRoot = filepath.Join(root, "run")
			cfg.Paths.BaseRoot = filepath.Join(root, "base")
			cfg.Units.Runner = "sandbox-runner@.service"
			runID := "sr-00000000-0000-7000-8000-000000000001"
			unit := instanceUnit(cfg.Units.Runner, runID)
			lc := &exitedRunnerLauncher{empty: tc.empty, proofErr: tc.proofErr}
			lc.units = []launcher.Unit{{Name: unit, ActiveState: tc.unitState}}
			vs := &reconcileVS{}
			o := &Orchestrator{cfg: cfg, st: st, lc: lc, vs: vs, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			o.runs.restore(runID, unit)
			sb := &types.Sandbox{
				ID: "reap-test", Profile: types.ProfileBare, State: types.StateRunning,
				RunID: runID, CreatedUnix: 1, VswitchPort: "port-1",
				RunDir:      nodepath.SandboxRunDir(cfg.Paths.RunRoot, "reap-test"),
				BaseDir:     nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, "reap-test"),
				ManifestKey: strings.Repeat("a", 64),
			}
			sb.APISecret = deriveTestAPISecret(t, sb.ManifestKey)
			materializeTestSandboxCredentials(t, sb)
			if err := st.Put(ctx, sb); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{sb.RunDir, sb.BaseDir} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if tc.cancelLifecycleDuringCleanup {
				lifecycleCtx, cancelLifecycle := context.WithCancel(ctx)
				o.SetLifecycleContext(lifecycleCtx)
				cleanupStarted := make(chan struct{})
				releaseCleanup := make(chan struct{})
				o.removeSandboxBaseDir = func(path string) error {
					close(cleanupStarted)
					<-releaseCleanup
					return os.RemoveAll(path)
				}
				cleanupDone := make(chan error, 1)
				go func() { cleanupDone <- o.reapExitedRunners(ctx) }()
				<-cleanupStarted
				cancelLifecycle()
				drainDone := make(chan error, 1)
				go func() { drainDone <- o.DrainRunnerReaps(ctx) }()
				deadline := time.Now().Add(2 * time.Second)
				for {
					o.runnerReapOps.mu.Lock()
					stopped, active := o.runnerReapOps.stopped, o.runnerReapOps.active
					o.runnerReapOps.mu.Unlock()
					if stopped {
						if active != 1 {
							t.Fatalf("drain lost accepted cleanup: active=%d", active)
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("drain did not close admission")
					}
					runtime.Gosched()
				}
				select {
				case err := <-drainDone:
					t.Fatalf("drain returned before cleanup: %v", err)
				default:
				}
				close(releaseCleanup)
				if err := <-cleanupDone; err != nil {
					t.Fatal(err)
				}
				if err := <-drainDone; err != nil {
					t.Fatal(err)
				}
			} else if tc.staleRun {
				next := *sb
				next.RunID = "sr-00000000-0000-7000-8000-000000000002"
				if err := st.Put(ctx, &next); err != nil {
					t.Fatal(err)
				}
				if err := o.reapExitedRunner(ctx, sb); err != nil {
					t.Fatal(err)
				}
			} else if tc.cancelProof {
				proofCtx, cancel := context.WithCancel(ctx)
				lc.afterProof = cancel
				if err := o.reapExitedRunner(proofCtx, sb); err != nil {
					t.Fatal(err)
				}
			} else if err := o.reapExitedRunners(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := st.Get(ctx, sb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantDead {
				if got.State != types.StateDead || len(vs.detached) != 1 {
					t.Fatalf("state=%s detached=%v", got.State, vs.detached)
				}
				for _, dir := range []string{sb.RunDir, sb.BaseDir} {
					if _, err := os.Stat(dir); !os.IsNotExist(err) {
						t.Fatalf("dir %s still exists: %v", dir, err)
					}
				}
			} else if got.State != types.StateRunning || len(vs.detached) != 0 {
				t.Fatalf("state=%s detached=%v", got.State, vs.detached)
			} else if tc.staleRun && got.RunID == sb.RunID {
				t.Fatalf("stale candidate changed new run: %s", got.RunID)
			}
		})
	}
}
