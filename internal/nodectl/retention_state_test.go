package nodectl

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func prepareRetentionStateTest(t *testing.T, s *State, sid string) *retainedReceipt {
	t.Helper()
	active, ok := s.retentionReservation(sid)
	if !ok {
		t.Fatal("missing active reservation")
	}
	r := &retainedReceipt{Version: 1, SandboxID: sid, Stage: retentionPrepared, Source: active,
		Capture:    resource.CaptureRetention{CapturePath: filepath.Join(t.TempDir(), "capture.json"), MemoryID: "memory-one", Epoch: 1, OperationID: "checkpoint-one"},
		Memory:     resource.RetainedMemory{MemoryID: "memory-one", Epoch: 1, SourcePool: s.SandboxPool(sid), Bytes: active.ReservationMemory},
		OwnerLease: resource.Lease{PID: active.PeerPID}, OwnerToken: active.Token}
	if err := s.prepareRetained(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRetainedMemoryProtectsPreparedAndCountsOneSource(t *testing.T) {
	s := makeState(8<<30, 0)
	spec := AdmitSpec{SandboxID: "sid", Token: "old", PeerPID: 101, Capacity: Resources{MemoryBytes: 1 << 30, CPUMilli: 1000},
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500}, InitialBudget: 256 << 20, InitialCache: 4 << 20,
		CacheMax: 32 << 20, ClientFeatures: []string{resource.FeatureCacheBudgetV1, resource.FeatureRetainedMemoryV1}}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	r := prepareRetentionStateTest(t, s, "sid")
	if _, _, err := s.Release("old"); err == nil {
		t.Fatal("ordinary Release deleted prepared B")
	}
	if _, _, err := s.RemoveIfIdentity("sid", "token:old"); err == nil {
		t.Fatal("sweeper deleted prepared B")
	}
	if _, _, err := s.ReconcileAndGrant("old", 128<<20, 0, resource.UrgencyLow, NewAllocator(AllocatorPolicy{})); err == nil {
		t.Fatal("prepared frozen Budget was shrunk")
	}
	if got := s.ResourceSnapshot(); got.Reserved.MemoryBytes != 256<<20 || got.CacheAllocated != 4<<20 {
		t.Fatalf("prepared charge: %+v", got)
	}
	r.Stage = retentionHolding
	if _, err := s.commitRetained(r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.commitRetained(r); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot(); got.Reserved.MemoryBytes != 256<<20 || got.Reserved.CPUMilli != 0 || got.CacheAllocated != 0 || got.StartupInFlight != 0 || got.ReservationCount != 0 {
		t.Fatalf("B -> R must preserve only old memory: %+v", got)
	}
	if _, _, err := s.Sync(SyncSpec{SandboxID: "sid", Token: "old-runtime", PeerPID: 202, Capacity: spec.Capacity, ReservationMemory: 256 << 20}); err == nil {
		t.Fatal("ordinary Sync bypassed pending R")
	}
	if _, _, err := s.Release("old"); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.RetainedMemory("sid"); !ok || got.Bytes != 256<<20 {
		t.Fatalf("old Release affected R: %+v %v", got, ok)
	}
	r.Stage = retentionAdopting
	r.Handoff = &retainedHandoff{ID: "adopt-new", Lease: resource.Lease{PID: 202}}
	if err := s.setRetainedReceipt(r); err != nil {
		t.Fatal(err)
	}
	spec.PeerPID, spec.Token, spec.RetainedAdoption = 202, "new", "adopt-new"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 512<<20 {
		t.Fatalf("R + new B=%d", got)
	}
	r.Stage, r.OwnerLease.PID = retentionActive, 202
	if err := s.setRetainedReceipt(r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Release("new"); err != nil {
		t.Fatal(err)
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
		t.Fatalf("new Release erased R: %d", got)
	}
	if err := s.releaseRetained("sid", r.Capture); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.RetainedMemory("sid"); ok {
		t.Fatal("settled tombstone counted as pending")
	}
	if got := s.ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
		t.Fatalf("settled R=%d", got)
	}
}

func TestRetainedMemorySourcePoolAndStaleCleanup(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 500 * poolMiB, CPUMilli: 1000}
	placePoolTest(t, s, "numa0", "sid", capacity)
	spec := AdmitSpec{SandboxID: "sid", Token: "old", PeerPID: 101, Capacity: capacity, InitialBudget: 200 * poolMiB}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	r := prepareRetentionStateTest(t, s, "sid")
	r.Stage = retentionHolding
	if _, err := s.commitRetained(r); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution("sid", "run-sid"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReservePlacement("sid", "new-run", capacity, OperationRecovery, "", func(name string) bool { return name == "numa1" }); err != nil {
		t.Fatal(err)
	}
	r.Stage, r.Handoff = retentionAdopting, &retainedHandoff{ID: "new-owner", Lease: resource.Lease{PID: 202}}
	if err := s.setRetainedReceipt(r); err != nil {
		t.Fatal(err)
	}
	spec.PeerPID, spec.Token, spec.RetainedAdoption = 202, "new", "new-owner"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	r.Stage, r.OwnerLease.PID = retentionActive, 202
	if err := s.setRetainedReceipt(r); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution("sid", "run-sid"); err != nil {
		t.Fatal(err)
	}
	if got := s.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes; got != 200*poolMiB {
		t.Fatalf("R moved source pool: %d", got)
	}
	if got := s.PoolStates()["numa1"].ResourceSnapshot().Reserved.MemoryBytes; got != 200*poolMiB {
		t.Fatalf("new B missing: %d", got)
	}
	// Concurrent stale Sync calls cannot recreate a second active SID in the
	// old routing destination while the new owner continues operating.
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if _, _, err := s.Sync(SyncSpec{SandboxID: "sid", PeerPID: 101, Token: "stale", Capacity: capacity, ReservationMemory: 200 * poolMiB}); err == nil {
				t.Error("stale old owner Sync succeeded")
			}
		})
	}
	wg.Wait()
	if err := s.releaseRetained("sid", r.Capture); err != nil {
		t.Fatal(err)
	}
	if got := s.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
		t.Fatalf("source R not released: %d", got)
	}
	newCapture := r.Capture
	newCapture.MemoryID, newCapture.Epoch = "memory-two", 2
	active, _ := s.retentionReservation("sid")
	next := &retainedReceipt{SandboxID: "sid", Stage: retentionPrepared, Source: active, Capture: newCapture,
		Memory: resource.RetainedMemory{MemoryID: "memory-two", Epoch: 2, SourcePool: "numa1", Bytes: active.ReservationMemory}}
	if err := s.prepareRetained(next); err != nil {
		t.Fatal(err)
	}
	if err := s.releaseRetained("sid", r.Capture); err == nil {
		t.Fatal("late old generation release accepted")
	}
	if _, ok := s.RetainedMemory("sid"); !ok {
		t.Fatal("late release cleared next capture")
	}
}
