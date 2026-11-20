package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sys/unix"
)

func TestUncommittedCaptureDeathRecoversThroughOrdinaryRunEnd(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "directory-busy"}[busy], func(t *testing.T) {
			f, owner := unstartedCaptureFixture(t)
			capture := owner
			capture.MemoryID, capture.MemoryEpoch = "partial-memory", 1
			p := startCheckpointHolder(t, f, capture, false, true)
			if err := f.o.reconcileRetiredRun(context.Background(), owner); !retirementIsPending(err) {
				t.Fatalf("active preparation changed: %v", err)
			}
			assertUnstartedCapturePending(t, f, owner)
			record := p.record(t)
			p.kill(t)
			var directory *os.File
			if busy {
				var err error
				directory, err = os.Open(record.Holder.Directory.Path)
				if err != nil {
					t.Fatal(err)
				}
				defer directory.Close()
				if err := unix.Flock(int(directory.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
				if err := f.o.reconcileRetiredRun(context.Background(), owner); !errors.Is(err, errRetirementPending) {
					t.Fatalf("busy private directory lost cleanup owner: %v", err)
				}
				assertUnstartedCapturePending(t, f, owner)
				if !p.record(t).Aborted {
					t.Fatal("private directory cleanup blocked physical abort")
				}
				if err := directory.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Recover durable host state after the actual producer SIGKILL.
			restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
			restarted.lc, restarted.vs = f.lc, f.vs
			ctx, cancel := context.WithCancel(context.Background())
			restarted.SetLifecycleContext(ctx)
			t.Cleanup(func() { cancel(); drainRunEnds(t, restarted) })
			f.o = restarted
			// Parent's ordinary exit is independent from the frozen-copy fact.
			f.lc.mu.Lock()
			f.lc.state = "inactive"
			f.lc.mu.Unlock()
			if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
				t.Fatal(err)
			}
			drainRunEnds(t, restarted)
			assertUnstartedCaptureDead(t, f, owner)
			record = p.record(t)
			if !record.Aborted || record.Committed || record.RuntimeRetired || record.RAMReleased || record.FileReady {
				t.Fatalf("recovery invented retirement facts: %+v", record)
			}
			if _, err := os.Lstat(record.Holder.Directory.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("abandoned private holder directory remains: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.sb.BaseDir, "checkpoint", "current-memory.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCommittedUnretiredCaptureDeathKeepsFencedCleanupBoundary(t *testing.T) {
	f, owner := unstartedCaptureFixture(t)
	capture := owner
	capture.MemoryID, capture.MemoryEpoch = "committed-memory", 1
	p := startCheckpointHolder(t, f, capture, false)
	before := p.record(t)
	p.kill(t)
	if err := f.o.reconcileRetiredRun(context.Background(), owner); !errors.Is(err, errCaptureOutcomePending) {
		t.Fatalf("unconfirmed retirement changed: %v", err)
	}
	assertUnstartedCapturePending(t, f, owner)
	if after := p.record(t); after != before {
		t.Fatalf("committed record was automatically aborted/discarded: %+v", after)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePausing || current.ResumeSource != f.sb.ResumeSource {
		t.Fatalf("unconfirmed runtime changed original source: %+v %v", current, err)
	}
}
