package orch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"golang.org/x/sys/unix"
)

// Model the natural-exit part of systemd Stop: SIGTERM cannot terminate a
// detached holder whose executable is waiting for its save. The production
// unit eventually escalates to SIGKILL; this test fails instead of killing Go.
type holderExitLauncher struct {
	launcher.Launcher
	holders map[string]*memorycheckpoint.Holder
}

func (l *holderExitLauncher) Stop(ctx context.Context, unit string) error {
	h := l.holders[unit]
	if h == nil {
		return l.Launcher.Stop(ctx, unit)
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := h.Wait(wait); err != nil {
		return fmt.Errorf("holder still saving at unit stop: %w", err)
	}
	return nil
}

func (l *holderExitLauncher) List(_ context.Context, pattern string) ([]launcher.Unit, error) {
	var units []launcher.Unit
	for unit, h := range l.holders {
		match, _ := filepath.Match(pattern, unit)
		if !match {
			continue
		}
		select {
		case <-h.Done():
		default:
			units = append(units, launcher.Unit{Name: unit, ActiveState: "active"})
		}
	}
	return units, nil
}

func TestTerminalDeleteCancelsDependentHoldersBeforeWaitingForUnits(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	first := retiredTestOwnership(t, f)
	dir := filepath.Join(f.sb.BaseDir, "checkpoint")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var holders []*memorycheckpoint.Holder
	lc := &holderExitLauncher{Launcher: f.lc, holders: make(map[string]*memorycheckpoint.Holder)}
	f.o.lc = lc
	var parentPin *os.File
	var closeParent sync.Once
	closePin := func() {
		closeParent.Do(func() {
			if parentPin != nil {
				_ = parentPin.Close()
			}
		})
	}
	t.Cleanup(closePin)
	for i := 0; i < 2; i++ {
		m := first
		m.RunID = fmt.Sprintf("sr-00000000-0000-7000-8000-%012d", 287+i)
		m.Unit = "sandbox-runner@" + m.RunID + ".service"
		m.RetirementCheckpointID, m.RetirementOperationID = fmt.Sprintf("capture-%d", i+1), fmt.Sprintf("capture-%d", i+1)
		m.MemoryID, m.ParentRetired = fmt.Sprintf("memory-%d", i+1), true
		if err := f.o.st.SaveRunPoolOwnership(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		f.o.runs.mu.Lock()
		f.o.runs.metadata[m.RunID], f.o.runs.owners[m.RunID] = m, f.o.runnerPool.pools[0]
		f.o.runs.mu.Unlock()
		f.o.runs.restore(m.RunID, m.Unit)
		mem, err := memory.Create("terminal-cancel", 4096)
		if err != nil {
			t.Fatal(err)
		}
		fd, err := memorycheckpoint.ReadOnlyMemfd(mem.FD())
		if err != nil {
			mem.Close()
			t.Fatal(err)
		}
		if err := mem.Close(); err != nil {
			fd.Close()
			t.Fatal(err)
		}
		h, err := memorycheckpoint.PrepareHolder(dir, f.sb.RunDir, memorycheckpoint.CaptureRecord{
			Version:      memorycheckpoint.RecordVersion,
			Producer:     memorycheckpoint.RunIdentity{SandboxID: f.sb.ID, InstanceID: f.sb.InstanceID, RunID: m.RunID},
			CheckpointID: m.RetirementCheckpointID, HostOperationID: m.RetirementOperationID,
			MemoryID: m.MemoryID, Epoch: m.MemoryEpoch,
		}, fd, 4096, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Record().Update(func(r *memorycheckpoint.CaptureRecord) {
			r.Committed, r.RuntimeRetired = true, true
			r.SnapshotRef, r.SandboxRef = checkpointSnapshotRef, checkpointSandboxRef
		}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// The successor's save retains a real shared RAM pin on its parent.
			// Revoking/waiting for only the oldest holder cannot cancel it.
			parentPin, err = os.OpenFile(h.Record().Path()+".ram.lock", os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Flock(int(parentPin.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
				t.Fatal(err)
			}
		}
		successor := i == 1
		if err := h.Start(func(ctx context.Context, _ io.ReaderAt) (string, error) {
			<-ctx.Done()
			if successor {
				closePin()
			}
			return "", ctx.Err()
		}, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			select {
			case <-h.Done():
				return
			default:
			}
			closePin()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = memorycheckpoint.DeleteCheckpoint(ctx, dir, memorycheckpoint.Reference{CheckpointID: m.RetirementCheckpointID, MemoryID: m.MemoryID})
			if err := h.Wait(ctx); err != nil {
				t.Error(err)
			}
		})
		holders = append(holders, h)
		lc.holders[m.Unit] = h
	}
	// Runtime/network/RunDir ownership has retired; the two holders remain.
	f.sb.State = types.StatePaused
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	f.sb.RunID, f.sb.RunDir, f.sb.VswitchPort, f.sb.FloatingIP = "", "", "", ""
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	beginDeletingForTest(t, f)
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
		t.Fatal(err)
	}
	for _, h := range holders {
		r := h.Record().Read()
		if !r.Deleted || !r.Discarded || !r.RAMReleased || r.FileReady {
			t.Fatalf("invalid terminal capture facts: deleted=%t discarded=%t released=%t ready=%t", r.Deleted, r.Discarded, r.RAMReleased, r.FileReady)
		}
	}
	if current, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || current != nil {
		t.Fatalf("terminal owner remains: %v", err)
	}
	if rows, err := f.o.st.RunPoolOwnerships(context.Background()); err != nil || len(rows) != 0 {
		t.Fatalf("retired units remain: %d %v", len(rows), err)
	}
	if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
		t.Fatalf("terminal BaseDir remains: %v", err)
	}
}

// Terminal source ownership withdraws external roots, but SDK publishers also
// use the local source fence. Revocation must not pass that publication boundary.
func TestTerminalDeleteHonorsCheckpointPublicationFence(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	w := testCaptureRecord(t, f, m)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	beginDeletingForTest(t, f)
	sb, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(sb.BaseDir, "checkpoint")
	fence, err := artifact.LockCheckpointSources(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ctx, release, err := f.o.trySourceOperation(ctx, sb)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := f.o.discardRetiredMemoryForDelete(ctx, sb); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("terminal cancellation passed publisher fence: %v", err)
	}
	record, err := f.o.readRunCapture(context.Background(), m)
	if err != nil || record == nil || record.Deleted {
		t.Fatalf("fenced source was revoked: %v", err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("fenced publisher's unit was stopped")
	}
}
