package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"golang.org/x/sys/unix"
)

func checkpointGCLifecycle(t *testing.T, o *Orchestrator) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.SetLifecycleContext(ctx)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if err := o.DrainPauses(ctx); err != nil {
				t.Errorf("drain checkpoint GC: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitCheckpointGC(t *testing.T, o *Orchestrator, sid string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		o.pausedCleanupMu.Lock()
		_, active := o.pausedCleanupActive[sid]
		o.pausedCleanupMu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("checkpoint GC worker did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func clearCheckpointRuntime(t *testing.T, f sandboxFinalizerFixture) {
	t.Helper()
	if f.sb.RunDir != "" {
		if err := os.RemoveAll(f.sb.RunDir); err != nil {
			t.Fatal(err)
		}
	}
	f.sb.State = types.StatePaused
	f.sb.RunID, f.sb.RunDir, f.sb.EnvdUDS, f.sb.CiUDS = "", "", "", ""
	f.sb.VswitchPort, f.sb.FloatingIP, f.sb.InnerIP, f.sb.PortMAC = "", "", "", ""
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
}

func TestReleasedHolderRetriesCheckpointGCWithoutRunOwnership(t *testing.T) {
	f := pendingPauseFixture(t)
	m := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
		r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
	})
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	clearCheckpointRuntime(t, f)
	dir := filepath.Join(f.sb.BaseDir, "checkpoint")
	junk, attempted := filepath.Join(dir, "obsolete.sandbox"), filepath.Join(t.TempDir(), "attempted")
	if err := os.WriteFile(junk, []byte("garbage"), 0600); err != nil {
		t.Fatal(err)
	}
	pin, err := os.OpenFile(filepath.Join(dir, ".memory-readers.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	if err := unix.Flock(int(pin.Fd()), unix.LOCK_SH); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(pin.Fd()), unix.LOCK_UN)
	checkpointCleanupTool(t, f.o, fmt.Sprintf("touch '%s'\nexec 9<>'%s'\nif ! flock -n -x 9; then exit %d; fi\nrm '%s'\n", attempted, pin.Name(), CheckpointMemoryReadersBusyExitCode, junk))
	checkpointGCLifecycle(t, f.o)
	if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	waitForCheckpointFile(t, attempted)
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
		t.Fatalf("GC retained released run: %v %v", found, err)
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || pausedCleanupPending(row) {
		t.Fatalf("GC restored runtime ownership: %+v %v", row, err)
	}
	if _, err := os.Stat(junk); err != nil {
		t.Fatalf("busy reader failed to protect garbage: %v", err)
	}
	if err := unix.Flock(int(pin.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	waitCheckpointGC(t, f.o, f.sb.ID)
	if _, err := os.Stat(junk); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unblocked GC was not retried automatically: %v", err)
	}
}

func TestCheckpointGCRestartWithoutRuntimeMarkers(t *testing.T) {
	f := localPausedCheckpoint(t, "gc-restart")
	clearCheckpointRuntime(t, f)
	dir := t.TempDir()
	failed, attempted, junk := filepath.Join(dir, "fail"), filepath.Join(dir, "attempted"), filepath.Join(f.sb.BaseDir, "checkpoint", "obsolete.sandbox")
	for _, path := range []string{failed, junk} {
		if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	checkpointCleanupTool(t, f.o, "touch '"+attempted+"'\nif [ -f '"+failed+"' ]; then exit 1; fi\nrm '"+junk+"'\n")
	stop := checkpointGCLifecycle(t, f.o)
	f.o.cleanupCheckpointAfterMemorySave(context.Background(), store.RunPoolOwnership{TaskID: f.sb.ID, RetirementInstanceID: f.sb.InstanceID, RetirementBaseDir: f.sb.BaseDir})
	waitForCheckpointFile(t, attempted)
	stop()
	if _, err := os.Stat(junk); err != nil {
		t.Fatal(err)
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || pausedCleanupPending(row) {
		t.Fatalf("failed GC recreated runtime marker: %+v %v", row, err)
	}
	if err := os.Remove(failed); err != nil {
		t.Fatal(err)
	}
	restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
	restarted.lc, restarted.vs, restarted.executables = f.lc, f.vs, f.o.executables
	checkpointGCLifecycle(t, restarted)
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCheckpointGC(t, restarted, f.sb.ID)
	if _, err := os.Stat(junk); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused BaseDir did not recover GC after restart: %v", err)
	}
}

func TestCheckpointGCWorkerRetirementReloadsSameIDSuccessor(t *testing.T) {
	for _, state := range []types.State{types.StateRunning, types.StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			f := localPausedCheckpoint(t, "gc-successor")
			clearCheckpointRuntime(t, f)
			old := f.sb.ResumeSource.Ref
			f.o.pausedCleanupActive = map[string]struct{}{f.sb.ID: {}}
			if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err != nil {
				t.Fatal(err)
			}
			// A same-ID successor wins the lifecycle lock between finalization
			// and retirement. No old artifact selection may cross this gap.
			successor := cloneSandbox(f.sb)
			successor.InstanceID = "new-instance"
			successor.State = state
			successor.ResumeSource.Ref = "file://" + strings.Repeat("b", 64) + ".sandbox@digest:" + strings.Repeat("b", 64)
			if state == types.StateRunning {
				successor.RunID = "new-run"
			}
			if err := f.o.st.Delete(context.Background(), f.sb.ID); err != nil {
				t.Fatal(err)
			}
			if err := f.o.st.InsertSandbox(context.Background(), successor); err != nil {
				t.Fatal(err)
			}
			current, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || current.InstanceID != successor.InstanceID || current.State != state {
				t.Fatalf("successor fixture did not replace old row: %+v %v", current, err)
			}
			proof := filepath.Join(t.TempDir(), "selected")
			checkpointCleanupTool(t, f.o, "printf '%s\n' \"$@\" > '"+proof+"'\n")
			retired, err := f.o.retirePausedCleanupWorker(f.sb.ID)
			if err != nil || !retired {
				t.Fatalf("worker retirement: %v %v", retired, err)
			}
			selected, err := os.ReadFile(proof)
			if state == types.StateRunning {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("GC touched running successor: %s %v", selected, err)
				}
			} else if err != nil || !strings.Contains(string(selected), successor.ResumeSource.Ref) || strings.Contains(string(selected), old) {
				t.Fatalf("GC used old source selection: %s %v", selected, err)
			}
		})
	}
}
