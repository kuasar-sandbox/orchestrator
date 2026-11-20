package nodectl

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
	"golang.org/x/sys/unix"
)

// Metadata-driven ledger tests do not keep a physical holder alive. Preserve
// real, stable proof identities and model its service directory as already gone.
func newTestRetentionRecord(t *testing.T, directory string, record memorycheckpoint.CaptureRecord) (*memorycheckpoint.RecordWriter, error) {
	t.Helper()
	path, err := memorycheckpoint.RecordPath(directory, record.CheckpointID, record.MemoryID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(filepath.Dir(directory), ".retention-holder-")
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Stat(dir, &stat); err != nil {
		return nil, err
	}
	record.Holder.Directory = memorycheckpoint.HolderDirectoryIdentity{Path: dir, Device: uint64(stat.Dev), Inode: stat.Ino}
	record.Holder.SocketPath = filepath.Join(dir, "m.sock")
	if err := os.Remove(dir); err != nil {
		return nil, err
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path+".ram.lock", nonce, 0600); err != nil {
		return nil, err
	}
	if err := unix.Stat(path+".ram.lock", &stat); err != nil {
		return nil, err
	}
	record.Holder.RAMLock = memorycheckpoint.FileIdentity{Device: uint64(stat.Dev), Inode: stat.Ino, Nonce: hex.EncodeToString(nonce)}
	return memorycheckpoint.CreateRecord(directory, record)
}

func directRetentionFixture(t *testing.T) (*Server, *memorycheckpoint.RecordWriter, resource.CaptureRetention) {
	t.Helper()
	dir := t.TempDir()
	socket := filepath.Join(dir, "ctl.sock")
	lease, err := resource.CreateLease(resource.Lease{Version: resource.LeaseVersion, SandboxID: "sid", PID: os.Getpid(),
		ControllerSocket: socket, CgroupPath: filepath.Join(dir, "cgroups", "old"), CapacityMemory: 1 << 30, CapacityCPUMilli: 1000,
		FloorMemory: 128 << 20, FloorCPUMilli: 500, StartupMemory: 256 << 20,
		ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	l, err := resource.ReadLease(lease.Path())
	if err != nil {
		t.Fatal(err)
	}
	state := makeState(8<<30, 0)
	_, _, err = state.Admit(AdmitSpec{SandboxID: "sid", PeerPID: os.Getpid(), Token: "original-token", CgroupPath: l.CgroupPath,
		Capacity: Resources{MemoryBytes: l.CapacityMemory, CPUMilli: l.CapacityCPUMilli}, InitialBudget: 256 << 20,
		ConfiguredAllocatable: Resources{MemoryBytes: l.FloorMemory, CPUMilli: l.FloorCPUMilli}, ClientFeatures: l.ClientFeatures, LeasePath: lease.Path()})
	if err != nil {
		t.Fatal(err)
	}
	start, err := retentionProcessStartTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	w, err := newTestRetentionRecord(t, filepath.Join(dir, "checkpoint"), memorycheckpoint.CaptureRecord{
		Version: memorycheckpoint.Version, Producer: memorycheckpoint.RunIdentity{SandboxID: "sid", InstanceID: "instance", RunID: "run"},
		CheckpointID: "checkpoint", MemoryID: "memory", Epoch: 1,
		Holder: memorycheckpoint.HolderIdentity{PID: os.Getpid(), StartTime: start, SocketPath: filepath.Join(dir, "holder.sock")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	capture := resource.CaptureRetention{CapturePath: w.Path(), MemoryID: "memory", OperationID: "checkpoint", Epoch: 1}
	srv := &Server{Identity: socket, State: state, Inventory: &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{filepath.Join(dir, "cgroups")}, Pool: state.AllocatablePool}}
	return srv, w, capture
}

func TestRetainedMemoryAbortedProofReconcilesWithoutReleasingB(t *testing.T) {
	for _, recoverInventory := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "restart"}[recoverInventory], func(t *testing.T) {
			srv, w, capture := directRetentionFixture(t)
			resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "original-token")
			if resp.Type != TypeAck {
				t.Fatal(resp.Msg)
			}
			if err := w.Update(func(r *memorycheckpoint.CaptureRecord) { r.Aborted = true }); err != nil {
				t.Fatal(err)
			}
			if recoverInventory {
				recovered := makeState(8<<30, 0)
				if err := srv.Inventory.Recover(recovered); err != nil {
					t.Fatal(err)
				}
				if _, pending := recovered.RetainedMemory("sid"); pending {
					t.Fatal("aborted capture recovered as R")
				}
				if got := recovered.ResourceSnapshot(); got.ReservationCount != 1 || got.Reserved.MemoryBytes != 1<<30 {
					t.Fatalf("ordinary lease was not conservatively recovered: %+v", got)
				}
			} else {
				if err := srv.State.ReconcileRetainedMemory("sid"); err != nil {
					t.Fatal(err)
				}
				if _, pending := srv.State.RetainedMemory("sid"); pending {
					t.Fatal("aborted intent remained pending")
				}
				if got := srv.State.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
					t.Fatalf("aborting capture released active B: %d", got)
				}
				if _, found, err := srv.State.Release("original-token"); err != nil || !found {
					t.Fatalf("ordinary cleanup remained blocked: %v %v", found, err)
				}
			}
			if account, err := readRetainedReceipt(retentionPath(srv.Identity, "sid")); err != nil || len(account.Captures) != 0 {
				t.Fatalf("abort must preserve the empty SID account: %+v %v", account, err)
			}
		})
	}
}

func TestRetainedMemoryUnknownProofPreservesCharge(t *testing.T) {
	srv, w, capture := directRetentionFixture(t)
	if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
		t.Fatal(resp.Msg)
	}
	if err := os.WriteFile(w.Path(), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := srv.State.ReconcileRetainedMemory("sid"); err != nil {
		t.Fatal(err)
	}
	if _, pending := srv.State.RetainedMemory("sid"); !pending {
		t.Fatal("unknown proof cleared capture protection")
	}
	if got := srv.State.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
		t.Fatalf("unknown proof released charge: %d", got)
	}
}

func TestRetainedMemoryAbsentReceiptNeedsReleasedProof(t *testing.T) {
	srv, w, capture := directRetentionFixture(t)
	req := &Message{Type: resource.TypeAdoptRetained, SandboxID: "sid", Retention: &capture,
		ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}}
	token := ""
	resp := srv.handleAdmit(nil, os.Getpid(), req, &token)
	if resp.Status != StatusRejected || resp.Reason == resource.ReasonRetentionSettledMissing {
		t.Fatalf("unaccounted pending memory allowed fallback: %+v", resp)
	}
	if err := w.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired, r.FileReady, r.RAMReleased = true, true, true, true
		r.SnapshotRef, r.SandboxRef, r.PayloadRef = "snapshot", "sandbox", "payload"
	}); err != nil {
		t.Fatal(err)
	}
	resp = srv.handleAdmit(nil, os.Getpid(), req, &token)
	if resp.Status != StatusRejected || resp.Reason != resource.ReasonRetentionSettledMissing || !resource.HasFeature(resp.ClientFeatures, resource.FeatureRetainedMemoryV2) {
		t.Fatalf("released static capture cannot use ordinary lease: %+v", resp)
	}
}

func TestRetainedMemorySupersededReleaseIsExactNoOp(t *testing.T) {
	for _, reuseMemoryID := range []bool{false, true} {
		t.Run(map[bool]string{false: "different-memory-same-epoch", true: "same-memory-different-epoch"}[reuseMemoryID], func(t *testing.T) {
			srv, w, capture := directRetentionFixture(t)
			call := func(typ string, peer int, c resource.CaptureRetention, token string) *Message {
				return srv.handleRetained(peer, &Message{Type: typ, SandboxID: "sid", Retention: &c}, token)
			}
			if resp := call(resource.TypePrepareRetained, os.Getpid(), capture, "original-token"); resp.Type != TypeAck {
				t.Fatal(resp.Msg)
			}
			if err := w.Update(func(r *memorycheckpoint.CaptureRecord) {
				r.Committed, r.RuntimeRetired, r.FileReady, r.RAMReleased = true, true, true, true
				r.SnapshotRef, r.SandboxRef, r.PayloadRef = "snapshot", "sandbox", "payload"
			}); err != nil {
				t.Fatal(err)
			}
			for _, typ := range []string{resource.TypeCommitRetained, resource.TypeReleaseRetained} {
				if resp := call(typ, os.Getpid(), capture, "original-token"); resp.Type != TypeAck {
					t.Fatal(resp.Msg)
				}
			}
			srv.State.Wm.StartupFactor = .75
			srv.Admission = NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100, StartupTTL: time.Minute})
			srv.Admission.SetWiring(srv.State, nil, srv.BuildAdmitOKFromQueue)
			candidate, req := adoptionCandidate(t, srv, capture, "next-owner")
			var newToken string
			if resp, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &newToken); err != nil || resp.Status != StatusAdmitted {
				t.Fatalf("adopt: %+v %v", resp, err)
			}
			if _, err := candidate.ConfirmPublished(); err != nil {
				t.Fatal(err)
			}

			proof, err := memorycheckpoint.ReadCaptureRecord(w.Path())
			if err != nil {
				t.Fatal(err)
			}
			memory, epoch := "memory-two", uint64(1)
			if reuseMemoryID {
				memory, epoch = capture.MemoryID, 2
			}
			nextWriter, err := newTestRetentionRecord(t, filepath.Join(filepath.Dir(srv.Identity), "checkpoint"), memorycheckpoint.CaptureRecord{
				Version: memorycheckpoint.Version, Producer: memorycheckpoint.RunIdentity{SandboxID: "sid", InstanceID: "instance-two", RunID: "run-two"},
				CheckpointID: "checkpoint-two", MemoryID: memory, Epoch: epoch, Holder: proof.Holder})
			if err != nil {
				t.Fatal(err)
			}
			defer nextWriter.Close()
			next := resource.CaptureRetention{CapturePath: nextWriter.Path(), MemoryID: memory, Epoch: epoch, OperationID: "checkpoint-two"}
			if resp := call(resource.TypePrepareRetained, os.Getpid(), next, newToken); resp.Type != TypeAck {
				t.Fatal(resp.Msg)
			}
			if resp := call(resource.TypeReleaseRetained, os.Getppid(), capture, "original-token"); resp.Type != TypeError {
				t.Fatalf("nonproducer received release ACK: %+v", resp)
			}
			resp := call(resource.TypeReleaseRetained, os.Getpid(), capture, "original-token")
			if resp.Type != TypeAck {
				t.Fatalf("superseded release: %+v", resp)
			}
			if got, pending := srv.State.RetainedMemory("sid"); !pending || got.MemoryID != memory || got.Epoch != epoch || srv.State.ResourceSnapshot().Reserved.MemoryBytes != 256<<20 {
				t.Fatalf("superseded release changed successor: %+v pending=%v", got, pending)
			}
		})
	}
}
