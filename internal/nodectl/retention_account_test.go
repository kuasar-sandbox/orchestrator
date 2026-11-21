package nodectl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestRetainedAccountMultipleCapturesAndReplay(t *testing.T) {
	srv, first, captureA := committedRetentionFixture(t)
	adopt := func(id string) (*resource.LeaseHandle, *Message, string) {
		t.Helper()
		candidate, req := adoptionCandidate(t, srv, captureA, id)
		req.Retention = nil // Restore choice is not the authority for R.
		var token string
		response, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &token)
		if err != nil || response.Status != StatusAdmitted {
			t.Fatalf("adopt: %+v %v", response, err)
		}
		if _, err := candidate.ConfirmPublished(); err != nil {
			t.Fatal(err)
		}
		return candidate, req, token
	}
	_, _, tokenB := adopt("run-b")
	start, err := retentionProcessStartTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	second, err := newTestRetentionRecord(t, filepath.Join(filepath.Dir(srv.Identity), "checkpoint"), memorycheckpoint.CaptureRecord{
		Version: memorycheckpoint.RecordVersion, Producer: memorycheckpoint.RunIdentity{SandboxID: "sid", InstanceID: "instance-b", RunID: "run-b"},
		CheckpointID: "checkpoint-b", MemoryID: "memory-b", Epoch: 1, Holder: memorycheckpoint.HolderIdentity{PID: os.Getpid(), StartTime: start}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	captureB := resource.CaptureRetention{CapturePath: second.Path(), MemoryID: "memory-b", Epoch: 1, OperationID: "checkpoint-b"}
	call := func(typ string, c resource.CaptureRetention, token string) {
		t.Helper()
		if response := srv.handleRetained(os.Getpid(), &Message{Type: typ, SandboxID: "sid", Retention: &c}, token); response.Type != TypeAck {
			t.Fatal(response.Msg)
		}
	}
	call(resource.TypePrepareRetained, captureB, tokenB)
	account := srv.State.retainedSnapshot("sid")
	if len(account.Captures) != 2 || account.Stage != retentionActive || account.capture(captureB).Stage != retentionPrepared ||
		account.capture(captureB).Memory.Bytes != 256<<20 || srv.State.ResourceSnapshot().Reserved.MemoryBytes != 512<<20 {
		t.Fatalf("B included inherited R: %+v", account)
	}
	if err := second.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired = true, true
		r.SnapshotRef, r.SandboxRef = "snapshot-b", "sandbox-b"
	}); err != nil {
		t.Fatal(err)
	}
	call(resource.TypeCommitRetained, captureB, tokenB)
	if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 512<<20 || got.ReservationCount != 0 || got.StartupInFlight != 0 {
		t.Fatalf("R1 + R2: %+v", got)
	}
	srv.phaseHook = func(phase string) {
		if phase == "retention_intent" {
			if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 768<<20 || got.ReservationCount != 1 {
				t.Fatalf("accepted handoff had unaccounted B: %+v", got)
			}
		}
	}
	_, reqC, tokenC := adopt("run-c")
	srv.phaseHook = nil
	for _, item := range []struct {
		writer  *memorycheckpoint.RecordWriter
		capture resource.CaptureRetention
	}{{second, captureB}, {first, captureA}} {
		if err := item.writer.Update(func(r *memorycheckpoint.CaptureRecord) {
			r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "saved"
		}); err != nil {
			t.Fatal(err)
		}
		call(resource.TypeReleaseRetained, item.capture, "original-producer")
		before := srv.State.ResourceSnapshot().Reserved.MemoryBytes
		call(resource.TypeReleaseRetained, item.capture, "duplicate-ack")
		if after := srv.State.ResourceSnapshot().Reserved.MemoryBytes; after != before {
			t.Fatalf("duplicate release: %d -> %d", before, after)
		}
	}
	account = srv.State.retainedSnapshot("sid")
	if account.pending() || account.Handoff == nil || account.Handoff.ID != reqC.HandoffID || account.Revision == reqC.RetainedAccount.Revision {
		t.Fatal("last R removed handoff or did not advance account")
	}
	var replayToken string
	response, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), reqC, &replayToken)
	if err != nil || response.Status != StatusAdmitted || replayToken != tokenC {
		t.Fatalf("old account version failed accepted replay: %+v %v", response, err)
	}
	for _, change := range []func(*Message){func(r *Message) { r.AllocatableAtSnapshot += 16 << 20 }, func(r *Message) { r.CacheInitial += 4 << 20 }} {
		altered := *reqC
		change(&altered)
		if _, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), &altered, &replayToken); err == nil {
			t.Fatal("accepted handoff changed its B/cache contract")
		}
	}
	if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 256<<20 || got.ReservationCount != 1 || got.CacheAllocated != 4<<20 {
		t.Fatalf("R settlement changed active B/cache: %+v", got)
	}
}

func TestRetainedAccountInventoryExceedsWireFrame(t *testing.T) {
	srv, _, _ := committedRetentionFixture(t)
	r := srv.State.retainedSnapshot("sid")
	base := cloneCapture(r.Captures[0])
	for n := 1; n <= 128; n++ {
		e := cloneCapture(base)
		e.Capture.MemoryID = fmt.Sprintf("memory-%d", n)
		e.Memory.MemoryID = e.Capture.MemoryID
		e.CheckpointID, e.Capture.OperationID = fmt.Sprintf("checkpoint-%d", n), fmt.Sprintf("checkpoint-%d", n)
		e.Capture.CapturePath = filepath.Join(filepath.Dir(base.Capture.CapturePath), fmt.Sprintf("%d.json", n))
		r.Captures = append(r.Captures, e)
	}
	if err := writeRetainedReceipt(srv.Identity, r); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(retentionPath(srv.Identity, "sid"))
	if err != nil || stat.Size() <= resource.MaxMessageBytes {
		t.Fatalf("inventory fixture not larger than frame: %+v %v", stat, err)
	}
	recovered, err := readRetainedReceipt(retentionPath(srv.Identity, "sid"))
	if err != nil || len(recovered.Captures) != len(r.Captures) {
		t.Fatalf("large inventory: %+v %v", recovered, err)
	}
	var wire bytes.Buffer
	if err := resource.WriteMessage(&wire, &resource.Message{Type: resource.TypeAdoptRetained, SandboxID: "sid", RetainedAccount: recovered.reference(), HandoffID: "candidate"}); err != nil {
		t.Fatal(err)
	}
	if wire.Len() > 1024 {
		t.Fatalf("wire unexpectedly carries capture inventory: %d", wire.Len())
	}
}

func TestRetainedAccountUpgradesLegacyReceipt(t *testing.T) {
	srv, _, _ := committedRetentionFixture(t)
	original := srv.State.retainedSnapshot("sid")
	e := original.Captures[0]
	legacy := legacyRetainedReceipt{Version: 1, SandboxID: original.SandboxID, Capture: e.Capture, Producer: e.Producer,
		CheckpointID: e.CheckpointID, Holder: e.Holder, Memory: e.Memory, Stage: retentionHolding, Source: e.Source,
		SourceStartTime: e.SourceStartTime, SourceFile: e.SourceFile, OwnerLease: original.OwnerLease, OwnerFile: original.OwnerFile, OwnerToken: original.OwnerToken}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(retentionPath(srv.Identity, "sid"), data, 0600); err != nil {
		t.Fatal(err)
	}
	recovered := makeState(8<<30, 0)
	if err := srv.Inventory.Recover(recovered); err != nil {
		t.Fatal(err)
	}
	r := recovered.retainedSnapshot("sid")
	if r.Version != 2 || len(r.Captures) != 1 || r.Captures[0].Capture != e.Capture || r.Captures[0].Stage != retentionHeld || r.OwnerFile != original.OwnerFile {
		t.Fatalf("upgrade changed identity: %+v", r)
	}
	if got := recovered.ResourceSnapshot().Reserved.MemoryBytes; got != e.Memory.Bytes {
		t.Fatalf("upgrade charge=%d", got)
	}
	data, err = os.ReadFile(retentionPath(srv.Identity, "sid"))
	if err != nil {
		t.Fatal(err)
	}
	var disk retainedAccount
	if err := strictRetentionJSON(data, &disk); err != nil || disk.Version != 2 {
		t.Fatalf("migration was not persisted: %v", err)
	}
}

func TestRetainedAccountMultipleSourcePools(t *testing.T) {
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 500 * poolMiB, CPUMilli: 1000}
	placePoolTest(t, s, "numa0", "sid", capacity)
	spec := AdmitSpec{SandboxID: "sid", Token: "run-a", PeerPID: 101, Capacity: capacity, InitialBudget: 200 * poolMiB}
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	r := prepareRetentionStateTest(t, s, "sid")
	first := r.Captures[0].Capture
	r = heldRetainedCapture(r, first)
	if _, err := s.commitRetained(r, first); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution("sid", "run-sid"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReservePlacement("sid", "run-b", capacity, OperationRecovery, "", func(name string) bool { return name == "numa1" }); err != nil {
		t.Fatal(err)
	}
	r.Stage, r.Handoff = retentionAdopting, &retainedHandoff{ID: "handoff-b", InitialBudget: spec.InitialBudget, Lease: resource.Lease{PID: 202}}
	if err := s.setRetainedReceipt(r); err != nil {
		t.Fatal(err)
	}
	spec.PeerPID, spec.Token, spec.RetainedAdoption = 202, "run-b", "handoff-b"
	if _, _, err := s.Admit(spec); err != nil {
		t.Fatal(err)
	}
	r.Stage, r.OwnerLease.PID = retentionActive, 202
	active, _ := s.retentionReservation("sid")
	second := resource.CaptureRetention{CapturePath: filepath.Join(t.TempDir(), "second.json"), MemoryID: "second", Epoch: 1, OperationID: "second"}
	r.Captures = append(r.Captures, &retainedCapture{Capture: second, Stage: retentionPrepared, Source: active,
		Memory: resource.RetainedMemory{MemoryID: "second", Epoch: 1, SourcePool: "numa1", Bytes: active.ReservationMemory}})
	if err := s.prepareRetained(r, second); err != nil {
		t.Fatal(err)
	}
	r = heldRetainedCapture(r, second)
	if _, err := s.commitRetained(r, second); err != nil {
		t.Fatal(err)
	}
	recovered := twoPoolState(t)
	if err := recovered.restoreRetained(s.retainedSnapshot("sid")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"numa0", "numa1"} {
		if got := recovered.PoolStates()[name].ResourceSnapshot(); got.Reserved.MemoryBytes != 200*poolMiB || got.StartupInFlight != 0 || got.ReservationCount != 0 {
			t.Fatalf("pool %s restored B from holder: %+v", name, got)
		}
	}
	next := settledRetainedCapture(recovered.retainedSnapshot("sid"), second)
	if err := recovered.releaseRetained(next, second); err != nil {
		t.Fatal(err)
	}
	if got := recovered.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes; got != 200*poolMiB {
		t.Fatalf("settling R2 released R1: %d", got)
	}
	if got := recovered.PoolStates()["numa1"].ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
		t.Fatalf("R2 remains: %d", got)
	}
}

func TestRetainedAccountSettledCgroupPathIsNotConsumerIdentity(t *testing.T) {
	srv, _, capture := committedRetentionFixture(t)
	old := settledRetainedCapture(srv.State.retainedSnapshot("sid"), capture)
	e := old.Captures[0]
	e.SourceStartTime = ^uint64(0) // this historical process identity is gone
	e.Holder.StartTime = e.SourceStartTime
	writeFakeCgroup(t, e.Source.CgroupPath, "536870912", os.Getpid())
	recovered := makeState(8<<30, 0)
	if err := recovered.restoreRetained(old); err != nil {
		t.Fatal(err)
	}
	if err := srv.Inventory.scanCgroups(recovered, map[int]bool{}); err != nil {
		t.Fatal(err)
	}
	if got := recovered.ResourceSnapshot(); got.Reserved.MemoryBytes == 0 || got.ReservationCount == 0 {
		t.Fatalf("historical cgroup hid a new consumer: %+v", got)
	}
	known := makeState(8<<30, 0)
	if err := known.restoreRetained(old); err != nil {
		t.Fatal(err)
	}
	if err := srv.Inventory.scanCgroups(known, map[int]bool{os.Getpid(): true}); err != nil {
		t.Fatal(err)
	}
	if got := known.ResourceSnapshot(); got.Reserved.MemoryBytes != 0 || got.ReservationCount != 0 {
		t.Fatalf("known retired holder was charged B again: %+v", got)
	}
}

func TestRetainedAccountRetiresOnlyAtSIDTerminalBoundary(t *testing.T) {
	srv, writer, capture := committedRetentionFixture(t)
	if err := srv.State.RetireRetainedMemory("sid"); err == nil {
		t.Fatal("retired an unsettled account")
	}
	candidate, request := adoptionCandidate(t, srv, capture, "successor")
	var token string
	if response, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), request, &token); err != nil || response.Status != StatusAdmitted {
		t.Fatalf("adopt: %+v %v", response, err)
	}
	if _, err := candidate.ConfirmPublished(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "saved"
	}); err != nil {
		t.Fatal(err)
	}
	if response := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypeReleaseRetained, SandboxID: "sid", Retention: &capture}, "original-token"); response.Type != TypeAck {
		t.Fatal(response.Msg)
	}
	if err := srv.State.RetireRetainedMemory("sid"); err == nil {
		t.Fatal("last R erased active B")
	}
	if _, _, err := srv.State.Release(token); err != nil {
		t.Fatal(err)
	}
	if err := srv.State.RetireRetainedMemory("sid"); err == nil {
		t.Fatal("live owner lease lost identity")
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := srv.State.RetireRetainedMemory("sid"); err != nil {
			t.Fatal(err)
		}
	}
	if srv.State.retainedSnapshot("sid") != nil {
		t.Fatal("terminal SID retained account")
	}
	if _, err := os.Stat(retentionPath(srv.Identity, "sid")); !os.IsNotExist(err) {
		t.Fatalf("terminal receipt persisted: %v", err)
	}
}

func TestRetainedCaptureOperationSeparatesProducerAndSettledEntries(t *testing.T) {
	srv, writer, capture := committedRetentionFixture(t)
	a := srv.State.retainedSnapshot("sid").Captures[0]
	if !srv.State.RetainedCaptureOperation(a.Producer, a.CheckpointID, a.Capture.OperationID) {
		t.Fatal("exact operation missing")
	}
	for _, change := range []func(*memorycheckpoint.RunIdentity){
		func(p *memorycheckpoint.RunIdentity) { p.RunID += "-successor" },
		func(p *memorycheckpoint.RunIdentity) { p.InstanceID += "-other" },
		func(p *memorycheckpoint.RunIdentity) { p.SandboxID += "-other" },
	} {
		producer := a.Producer
		change(&producer)
		if srv.State.RetainedCaptureOperation(producer, a.CheckpointID, a.Capture.OperationID) {
			t.Fatal("unrelated producer inherited rollback barrier")
		}
	}
	if srv.State.RetainedCaptureOperation(a.Producer, a.CheckpointID, "different-operation") || srv.State.RetainedCaptureOperation(a.Producer, "different-checkpoint", a.Capture.OperationID) {
		t.Fatal("operation identity was partial")
	}
	if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.FileReady = true
		r.RAMReleased = true
		r.PayloadRef = "saved"
	}); err != nil {
		t.Fatal(err)
	}
	if response := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypeReleaseRetained, SandboxID: "sid", Retention: &capture}, "old-producer"); response.Type != TypeAck {
		t.Fatal(response.Msg)
	}
	if srv.State.RetainedCaptureOperation(a.Producer, a.CheckpointID, a.Capture.OperationID) {
		t.Fatal("settled identity still blocked exact retirement")
	}
	if len(srv.State.retainedSnapshot("sid").Captures) != 1 {
		t.Fatal("settlement erased discoverable zero-charge identity")
	}
}
