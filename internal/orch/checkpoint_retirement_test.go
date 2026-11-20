package orch

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"golang.org/x/sys/unix"
)

func retiredTestOwnership(t *testing.T, f sandboxFinalizerFixture) store.RunPoolOwnership {
	t.Helper()
	pool := f.o.runnerPool.pools[0]
	m := store.RunPoolOwnership{RunID: f.sb.RunID, Kind: runKindSandbox, Pool: pool.name, ResourcePool: pool.resourcePool,
		Unit: f.lc.unit, TaskID: f.sb.ID, RetirementOperationID: "capture-1", RetirementCheckpointID: "capture-1",
		RetirementInstanceID: f.sb.InstanceID, RetirementBaseDir: f.sb.BaseDir,
		Role: store.RunRoleMemoryHolder, MemoryID: "memory-1", MemoryEpoch: 1}
	if err := f.o.st.SaveRunPoolOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	f.o.runs.mu.Lock()
	f.o.runs.metadata = map[string]store.RunPoolOwnership{m.RunID: m}
	f.o.runs.owners = map[string]*runPool{m.RunID: pool}
	f.o.runs.mu.Unlock()
	f.o.runs.restore(m.RunID, m.Unit)
	return m
}

func testCaptureRecord(t *testing.T, f sandboxFinalizerFixture, m store.RunPoolOwnership, adjust ...func(*memorycheckpoint.CaptureRecord)) *memorycheckpoint.RecordWriter {
	t.Helper()
	// These tests control durable facts directly. Give an already-removed
	// service directory a real identity; physical holder tests use PrepareHolder.
	dir, err := os.MkdirTemp(filepath.Dir(f.sb.RunDir), ".fixture-holder-")
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(dir, &stat); err != nil {
		t.Fatal(err)
	}
	identity := memorycheckpoint.HolderDirectoryIdentity{Path: dir, Device: uint64(stat.Dev), Inode: stat.Ino}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	r := memorycheckpoint.CaptureRecord{Version: 1, Producer: memorycheckpoint.RunIdentity{SandboxID: f.sb.ID, InstanceID: f.sb.InstanceID, RunID: f.sb.RunID},
		CheckpointID: m.RetirementCheckpointID, HostOperationID: m.RetirementOperationID, MemoryID: "memory-1", Epoch: 1,
		SnapshotRef: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef, Committed: true, RuntimeRetired: true,
		Holder: memorycheckpoint.HolderIdentity{PID: os.Getpid(), StartTime: 1, SocketPath: filepath.Join(dir, "m.sock"), Directory: identity}}
	for _, change := range adjust {
		change(&r)
	}
	path, err := memorycheckpoint.RecordPath(filepath.Join(f.sb.BaseDir, "checkpoint"), r.CheckpointID, r.MemoryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".ram.lock", nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(path+".ram.lock", &stat); err != nil {
		t.Fatal(err)
	}
	r.Holder.RAMLock = memorycheckpoint.FileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
	w, err := memorycheckpoint.CreateRecord(filepath.Join(f.sb.BaseDir, "checkpoint"), r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestAbortedCaptureSupportsSynchronousFallbackAndDefiniteRollback(t *testing.T) {
	for _, captureErr := range []error{nil, io.EOF} {
		name := "synchronous fallback"
		if captureErr != nil {
			name = "aborted failure"
		}
		t.Run(name, func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			m := retiredTestOwnership(t, f)
			m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
			if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
				r.Committed, r.RuntimeRetired, r.Aborted = false, false, true
			})
			source := types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
			got, err := f.o.recoverCaptureOutcome(context.Background(), f.sb, &ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}, source, captureErr)
			if !errors.Is(err, captureErr) || got != source {
				t.Fatalf("aborted outcome = %+v %v", got, err)
			}
			owner, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID)
			if err != nil || !found || owner.RetirementOperationID != "" || owner.Role != "" {
				t.Fatalf("abort discarded ordinary owner or retained intent: %+v %v", owner, err)
			}
		})
	}
}

func stopRetirementWorkers(t *testing.T, o *Orchestrator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o.SetLifecycleContext(ctx)
}

func TestHostPermitRequiresActualParentCapability(t *testing.T) {
	f := pendingPauseFixture(t)
	f.o.allowLegacyAssignmentWithoutRunSession = false
	if permit, err := f.o.prepareCheckpointRetirement(context.Background(), f.sb, "capture-1"); permit != nil || !errors.Is(err, errRetirementPending) {
		t.Fatalf("missing parent = %+v %v", permit, err)
	}
	old, ok, err := f.o.RegisterRunSession(context.Background(), runKindSandbox, f.sb.RunID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if permit, err := f.o.prepareCheckpointRetirement(context.Background(), f.sb, "capture-1"); err != nil || permit != nil {
		t.Fatalf("old parent = %+v %v", permit, err)
	}
	old.Close(true)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch, m.RetirementOperationID = "", "", 0, ""
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	current, ok, err := f.o.RegisterRunSessionFeatures(context.Background(), runKindSandbox, f.sb.RunID, []string{configsock.FeatureCheckpointHolderRetirementV1})
	if err != nil || !ok {
		t.Fatal(err)
	}
	defer current.Close(true)
	permit, err := f.o.prepareCheckpointRetirement(context.Background(), f.sb, "capture-1")
	if err != nil || permit == nil || permit.RunID != f.sb.RunID {
		t.Fatalf("supported parent = %+v %v", permit, err)
	}
	durable, found, err := f.o.st.RunPoolOwnership(context.Background(), f.sb.RunID)
	if err != nil || !found || durable.Role != "" || durable.ParentRetired || durable.RetirementOperationID != permit.OperationID {
		t.Fatalf("prepare prematurely retired runtime: %+v %v", durable, err)
	}
}

func TestParentAckDoesNotNeedSIDLockOrFenceHolder(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, m)
	session, ok, err := f.o.RegisterRunSessionFeatures(context.Background(), runKindSandbox, f.sb.RunID, []string{configsock.FeatureCheckpointHolderRetirementV1})
	if err != nil || !ok {
		t.Fatal(err)
	}
	defer session.Close(true)
	event := <-session.(configsock.RunRetirementSession).Retirements()
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.o.cleanupRetiredRunner(ctx, f.sb); done <- err }()
	stale := event
	stale.Epoch++
	if _, err := f.o.RunRetirementRole(ctx, runKindSandbox, f.sb.RunID, &stale); err == nil {
		t.Fatal("stale epoch acknowledged")
	}
	if _, err := f.o.RunRetirementRole(ctx, runKindSandbox, f.sb.RunID, &event); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ACK deadlocked behind SID lock")
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("runtime retirement stopped holder unit")
	}
	if unit := f.o.runs.unit(m.RunID); unit != m.Unit {
		t.Fatalf("holder unit was forgotten: %q", unit)
	}
	f.o.runs.mu.Lock()
	_, charged := f.o.runs.owners[m.RunID]
	f.o.runs.mu.Unlock()
	if charged {
		t.Fatal("holder retained active runner slot")
	}
}

func TestCaptureEOFAfterCommitRecoversSourceAndAtomicRole(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	if err := f.o.st.SaveRunPoolOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	testCaptureRecord(t, f, m)
	f.sb.State = types.StatePausing
	if changed, err := f.o.st.SetPausing(context.Background(), f.sb.ID, f.sb.RunID, true); err != nil || !changed {
		t.Fatalf("prepare pause = %v %v", changed, err)
	}
	permit := &ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}
	source, err := f.o.recoverCaptureOutcome(context.Background(), f.sb, permit, types.ResumeSource{}, io.EOF)
	if err != nil || source.Ref != checkpointSnapshotRef {
		t.Fatalf("committed EOF = %+v %v", source, err)
	}
	if changed, err := f.o.commitPausedCapture(context.Background(), f.sb, source, ""); err != nil || !changed {
		t.Fatalf("commit recovered capture = %v %v", changed, err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	role, _, roleErr := f.o.st.RunPoolOwnership(context.Background(), f.sb.RunID)
	if err != nil || roleErr != nil || current.State != types.StatePaused || role.Role != store.RunRoleMemoryHolder || role.MemoryID != "memory-1" {
		t.Fatalf("recovered state = %+v %+v %v %v", current, role, err, roleErr)
	}
}

func TestCaptureUnknownEOFAndDeleteDoNotDiscardFrozenOwner(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	permit := &ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}
	if _, err := f.o.recoverCaptureOutcome(context.Background(), f.sb, permit, types.ResumeSource{}, io.EOF); !errors.Is(err, errCaptureOutcomePending) {
		t.Fatalf("unknown EOF = %v", err)
	}
	testCaptureRecord(t, f, m)
	if err := f.o.cleanupSandboxTerminalOwnership(context.Background(), f.sb); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("ordinary terminal cleanup while RAM pending = %v", err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("pending delete killed frozen owner")
	}
	if _, err := os.Stat(f.sb.BaseDir); err != nil {
		t.Fatal("pending delete removed checkpoint source")
	}
}

func TestPausedCleanupRemovesRunDirectoryAndPreservesSiblingHolder(t *testing.T) {
	f := localPausedCheckpoint(t, "sibling-holder")
	stopRetirementWorkers(t, f.o)
	checkpointCleanupTool(t, f.o, "exit 0\n")
	holder := filepath.Join(filepath.Dir(f.sb.RunDir), ".fixture-sibling-holder")
	if err := os.MkdirAll(holder, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(holder, "m.sock"), []byte("holder"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.sb.RunDir, "ctl.sock"), []byte("old runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(holder, "m.sock")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(f.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("ordinary RunDir inode remained")
	}
}

func TestRetirementRecoveryFinishesPausedTransactionWithoutStoppingHolder(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	m.RetainCheckpoint = true
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	testCaptureRecord(t, f, m)
	if changed, err := f.o.st.SetPausing(context.Background(), f.sb.ID, f.sb.RunID, true); err != nil || !changed {
		t.Fatalf("prepare = %v %v", changed, err)
	}
	if err := f.o.reconcileRetiredRun(context.Background(), m); !errors.Is(err, errRetirementPending) {
		t.Fatalf("recover = %v", err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePaused || current.ResumeSource.Ref != checkpointSnapshotRef {
		t.Fatalf("source = %+v %v", current, err)
	}
	retained, err := f.o.st.Checkpoint(context.Background(), f.sb.ID)
	if err != nil || retained == nil || retained.ID != m.RetirementCheckpointID {
		t.Fatalf("retained checkpoint = %+v %v", retained, err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("recovery killed the pending holder")
	}
}

func TestOrdinaryFenceCannotKillPendingHolder(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	testCaptureRecord(t, f, m)
	if err := f.o.fenceSandboxRunner(context.Background(), m.RunID); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("ordinary fence = %v", err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("ordinary fence stopped holder")
	}
}

func TestReleasedHolderAbsentServicePreservesSuccessorRunDirectory(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	w := testCaptureRecord(t, f, m)
	if err := w.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
	}); err != nil {
		t.Fatal(err)
	}
	record := w.Read()
	successor := filepath.Join(f.sb.RunDir, "ctl.sock")
	if err := os.WriteFile(successor, []byte("successor"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.o.releaseRetiredRun(context.Background(), m, &record); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
		t.Fatalf("released owner = %v %v", found, err)
	}
	if b, err := os.ReadFile(successor); err != nil || string(b) != "successor" {
		t.Fatalf("successor path = %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Dir(record.Holder.SocketPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released generation remained: %v", err)
	}
}

func TestHardDeleteRequiresReaderProofBeforeDiscardingCapture(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	w := testCaptureRecord(t, f, m)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	beginDeletingForTest(t, f)
	pin, err := memorycheckpoint.PinDirectory(filepath.Join(f.sb.BaseDir, "checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err == nil {
		t.Fatal("delete discarded a pinned reader")
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current == nil || current.State != types.StateDeleting {
		t.Fatalf("failed delete lost durable owner: %+v %v", current, err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
		t.Fatalf("reader prevented delete but ownership was discarded: %v %v", found, err)
	}
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if current, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || current != nil {
		t.Fatalf("delete did not converge: %+v %v", current, err)
	}
}

func TestDeletePreparedHostIntentBeforeCommandDispatch(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.sb.BaseDir, "checkpoint"), 0700); err != nil {
		t.Fatal(err)
	}
	beginDeletingForTest(t, f)
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
		t.Fatalf("unaccepted capture intent remained: %v %v", found, err)
	}
}

func TestCommittedCaptureRetirementTimeoutRetainsForwardRecovery(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	w := testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) { r.RuntimeRetired = false })
	if changed, err := f.o.st.SetPausing(context.Background(), f.sb.ID, f.sb.RunID, true); err != nil || !changed {
		t.Fatalf("prepare = %v %v", changed, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	guard, unlock, err := f.o.lockLifecycleMutation(ctx, f.sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.o.recoverCaptureOutcome(guard, f.sb, &ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}, types.ResumeSource{}, io.EOF)
	unlock()
	if !errors.Is(err, errCaptureOutcomePending) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retirement timeout = %v", err)
	}
	owner, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID)
	if err != nil || !found || owner.RetirementOperationID != m.RetirementOperationID {
		t.Fatalf("unknown retirement lost prepared owner: %+v %v", owner, err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePausing {
		t.Fatalf("unknown retirement rolled back runtime: %+v %v", current, err)
	}
	// A bounded failed request must leave the ordinary admission fences usable.
	nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
	defer nextCancel()
	_, release, err := f.o.lockLifecycleMutation(nextCtx, f.sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err := w.Update(func(r *memorycheckpoint.CaptureRecord) { r.RuntimeRetired = true }); err != nil {
		t.Fatal(err)
	}
	if err := f.o.reconcileRetiredRun(context.Background(), owner); !errors.Is(err, errRetirementPending) {
		t.Fatalf("continued retirement recovery = %v", err)
	}
	current, err = f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePaused || current.ResumeSource.Ref != checkpointSnapshotRef {
		t.Fatalf("committed source was lost after timeout: %+v %v", current, err)
	}
}

func TestCommittedLostCaptureRetainsForwardRecoveryInsteadOfRollback(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) { r.RAMReleased = true })
	if changed, err := f.o.st.SetPausing(context.Background(), f.sb.ID, f.sb.RunID, true); err != nil || !changed {
		t.Fatalf("prepare pause: %v %v", changed, err)
	}
	_, err := f.o.recoverCaptureOutcome(context.Background(), f.sb,
		&ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}, types.ResumeSource{}, io.EOF)
	if !errors.Is(err, memorycheckpoint.ErrMemoryLost) || !errors.Is(err, errCaptureOutcomePending) {
		t.Fatalf("lost committed capture became rollback-safe: %v", err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StatePausing {
		t.Fatalf("committed loss revived the runtime: %v", err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
		t.Fatalf("committed loss discarded forward recovery: %v %v", found, err)
	}
	if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	current, err = f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StateDead {
		t.Fatalf("lost capture did not converge through terminal cleanup: %v", err)
	}
}

func TestReleasedHolderCollectsPausedCheckpointAfterRunDirCleared(t *testing.T) {
	for _, paused := range []bool{true, false} {
		t.Run(map[bool]string{true: "paused", false: "running successor"}[paused], func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			m := retiredTestOwnership(t, f)
			testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
				r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
			})
			f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
			f.sb.RunID, f.sb.RunDir, f.sb.EnvdUDS, f.sb.CiUDS = "", "", "", ""
			f.sb.State = types.StatePaused
			if !paused {
				f.sb.State, f.sb.RunID = types.StateRunning, "successor-run"
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			proof := filepath.Join(t.TempDir(), "collected")
			checkpointCleanupTool(t, f.o, "touch '"+proof+"'\n")
			if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
				t.Fatalf("completed holder reconciliation: %v", err)
			}
			_, proofErr := os.Stat(proof)
			if paused && proofErr != nil || !paused && !errors.Is(proofErr, os.ErrNotExist) {
				t.Fatalf("post-save collection paused=%v: %v", paused, proofErr)
			}
			if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
				t.Fatalf("finished holder ownership remained: %v %v", found, err)
			}
			row, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || row.State != f.sb.State || row.RunID != f.sb.RunID || row.ResumeSource != f.sb.ResumeSource {
				t.Fatalf("post-save collection changed current owner: %+v %v", row, err)
			}
		})
	}
}
