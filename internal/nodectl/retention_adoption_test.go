package nodectl

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func committedRetentionFixture(t *testing.T) (*Server, *memorycheckpoint.RecordWriter, resource.CaptureRetention) {
	t.Helper()
	srv, writer, capture := directRetentionFixture(t)
	srv.State.Wm.StartupFactor = .75
	srv.Admission = NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100, StartupTTL: time.Minute})
	srv.Admission.SetWiring(srv.State, nil, srv.BuildAdmitOKFromQueue)
	if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
		t.Fatal(resp.Msg)
	}
	if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired = true, true
		r.SnapshotRef, r.SandboxRef = "snapshot", "sandbox"
	}); err != nil {
		t.Fatal(err)
	}
	if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypeCommitRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
		t.Fatal(resp.Msg)
	}
	return srv, writer, capture
}

func adoptionCandidate(t *testing.T, srv *Server, capture resource.CaptureRetention, id string) (*resource.LeaseHandle, *Message) {
	t.Helper()
	lease := srv.State.retainedSnapshot("sid").Source
	features := []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV1, resource.FeatureCacheBudgetV1, resource.CacheMaximumFeature(32 << 20)}
	h, err := resource.CreateLeaseCandidate(resource.Lease{Version: resource.LeaseVersion, SandboxID: "sid", PID: os.Getpid(),
		ControllerSocket: srv.Identity, CgroupPath: filepath.Join(filepath.Dir(lease.CgroupPath), id),
		CapacityMemory: 1 << 30, CapacityCPUMilli: 1000, FloorMemory: 128 << 20, FloorCPUMilli: 500,
		StartupMemory: 256 << 20, ClientFeatures: features})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	l, err := resource.ReadLease(h.Path())
	if err != nil {
		t.Fatal(err)
	}
	return h, &Message{Type: resource.TypeAdoptRetained, SandboxID: "sid", Retention: &capture, HandoffID: id, LeaseCandidate: h.Path(),
		CapacityMemoryBytes: l.CapacityMemory, CapacityCPU: 1, FloorMemoryBytes: l.FloorMemory, FloorCPU: .5,
		StartupBudgetMemory: l.StartupMemory, AllocatableAtSnapshot: 256 << 20, CgroupPath: l.CgroupPath,
		ClientFeatures: features, CacheMaximum: 32 << 20, CacheInitial: 4 << 20}
}

func TestRetainedMemoryAdoptionClosedCandidateSamePIDRetries(t *testing.T) {
	for _, settled := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending-R", true: "settled-R"}[settled], func(t *testing.T) {
			srv, writer, capture := committedRetentionFixture(t)
			h, req := adoptionCandidate(t, srv, capture, "failed-attempt")
			srv.phaseHook = func(phase string) {
				if phase == "retention_admitted" {
					if err := h.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			token := ""
			_, admitErr := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &token)
			if admitErr == nil {
				t.Fatal("closed candidate was published")
			}
			if token == "" {
				t.Fatalf("failed publication did not bind admitted connection token: %v", admitErr)
			}
			before := srv.State.retainedSnapshot("sid")
			if before.Stage != retentionAdopting || before.Handoff.Token != token {
				t.Fatal("failure did not retain the exact adoption intent")
			}
			if settled {
				if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
					r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload"
				}); err != nil {
					t.Fatal(err)
				}
				if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypeReleaseRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
					t.Fatal(resp.Msg)
				}
			}
			if err := srv.State.ReconcileRetainedMemory("sid"); err != nil {
				t.Fatal(err)
			}
			after := srv.State.retainedSnapshot("sid")
			if after.Stage != retentionHolding || after.Handoff != nil || after.Settled != settled {
				t.Fatalf("ended candidate did not recover to holding: stage=%s settled=%v", after.Stage, after.Settled)
			}
			wantR := uint64(256 << 20)
			if settled {
				wantR = 0
			}
			if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != wantR || got.CacheAllocated != 0 || got.ReservationCount != 0 {
				t.Fatalf("cleanup changed source R or retained failed B/cache: %+v", got)
			}
			srv.phaseHook = nil
			_, retry := adoptionCandidate(t, srv, capture, "next-attempt")
			resp := srv.handleAdmit(nil, os.Getpid(), retry, &token)
			if resp == nil || resp.Status != StatusAdmitted {
				t.Fatalf("same PID next attempt failed: %+v", resp)
			}
			if token == before.Handoff.Token {
				t.Fatal("next attempt inherited the failed token")
			}
			if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != wantR+256<<20 || got.CacheAllocated != 4<<20 || got.ReservationCount != 1 {
				t.Fatalf("retry budget: %+v", got)
			}
		})
	}
}

func TestRetainedMemoryAdoptionPublishedBeforeACKRollsForward(t *testing.T) {
	srv, _, capture := committedRetentionFixture(t)
	_, req := adoptionCandidate(t, srv, capture, "published-attempt")
	srv.phaseHook = func(phase string) {
		if phase == "retention_published" {
			panic("stop before receipt/ACK")
		}
	}
	token := ""
	func() {
		defer func() {
			if got := recover(); got != "stop before receipt/ACK" {
				t.Fatalf("publication interruption: %v", got)
			}
		}()
		if _, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &token); err != nil {
			t.Errorf("before publication: %v", err)
		}
	}()
	if err := srv.Inventory.ReconcileRetained(srv.State); err != nil {
		t.Fatal(err)
	}
	r := srv.State.retainedSnapshot("sid")
	if r.Stage != retentionActive || r.OwnerFile != r.Handoff.File || r.OwnerToken != token {
		t.Fatal("exact published lease was not rolled forward")
	}
	if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 512<<20 || got.CacheAllocated != 4<<20 || got.ReservationCount != 1 {
		t.Fatalf("lost ACK recovery erased active B or source R: %+v", got)
	}
	srv.phaseHook = nil
	resp, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &token)
	if err != nil || resp.Token != r.OwnerToken {
		t.Fatalf("published replay did not preserve exact token: %v", err)
	}
	if pid, locked, err := resource.LeaseLockOwner(resource.LeasePath(srv.Identity, "sid")); err != nil || !locked || pid != os.Getpid() {
		t.Fatalf("same PID inspection dropped the POSIX lock: %d %v %v", pid, locked, err)
	}
}

func TestRetainedMemoryAdoptionUnknownOrLiveCandidateKeepsIntent(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "unknown-inode"}[replace], func(t *testing.T) {
			srv, _, capture := committedRetentionFixture(t)
			h, req := adoptionCandidate(t, srv, capture, "unpublished-attempt")
			srv.phaseHook = func(phase string) {
				if phase == "retention_intent" {
					panic("stop after intent")
				}
			}
			func() {
				defer func() {
					if got := recover(); got != "stop after intent" {
						t.Fatalf("intent interruption: %v", got)
					}
				}()
				_, _ = srv.buildRetainedAdmitOK(nil, os.Getpid(), req, nil)
			}()
			if replace {
				if err := os.Rename(h.Path(), h.Path()+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(h.Path(), []byte("unrelated inode"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := srv.Inventory.ReconcileRetained(srv.State); err != nil {
				t.Fatal(err)
			}
			r := srv.State.retainedSnapshot("sid")
			if r.Stage != retentionAdopting || r.Handoff.ID != req.HandoffID {
				t.Fatal("live or unknown candidate was rolled back")
			}
			srv.phaseHook = nil
			_, other := adoptionCandidate(t, srv, capture, "other-attempt")
			if _, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), other, nil); err == nil {
				t.Fatal("new attempt stole a live/unknown handoff")
			}
		})
	}
}
