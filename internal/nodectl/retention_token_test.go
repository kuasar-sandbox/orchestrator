package nodectl

import (
	"os"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestPreparedCaptureSurvivesStateSyncTokenRotation(t *testing.T) {
	for _, action := range []string{resource.TypeCommitRetained, resource.TypeAbortRetained, resource.TypeReleaseRetained} {
		t.Run(action, func(t *testing.T) {
			srv, writer, capture := directRetentionFixture(t)
			srv.Logf = func(string, ...any) {}
			call := func(kind, token string) {
				t.Helper()
				response := srv.handleRetained(os.Getpid(), &Message{Type: kind, SandboxID: "sid", Retention: &capture}, token)
				if response.Type != TypeAck {
					t.Fatal(kind, response.Msg)
				}
			}
			call(resource.TypePrepareRetained, "original-token")
			token := ""
			response := srv.handleStateSync(nil, os.Getpid(), &Message{SandboxID: "sid", AppliedAllocatableMemory: 256 << 20, Settled: true}, &token)
			if response.Type != TypeAck || token == "" || token == "original-token" {
				t.Fatalf("StateSync did not rotate session: %+v", response)
			}
			if active, ok := srv.State.retentionReservation("sid"); !ok || !active.RetentionPrepared || active.Token != token {
				t.Fatal("StateSync lost capture protection", active)
			}
			if srv.State.retainedSnapshot("sid").Captures[0].Source.Token != "original-token" {
				t.Fatal("session rotation rewrote immutable source history")
			}
			if action != resource.TypeAbortRetained {
				if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
					r.Committed, r.RuntimeRetired = true, true
					r.SnapshotRef, r.SandboxRef = "snapshot", "sandbox"
					if action == resource.TypeReleaseRetained {
						r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload"
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			call(action, token)
			call(action, token)
			switch action {
			case resource.TypeCommitRetained:
				if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 256<<20 || got.ReservationCount != 0 {
					t.Fatalf("rotated commit did not transfer exactly B to R: %+v", got)
				}
			case resource.TypeAbortRetained:
				if active, ok := srv.State.retentionReservation("sid"); !ok || active.RetentionPrepared {
					t.Fatal("rotated abort left capture protection", active)
				}
				if _, found, err := srv.State.Release(token); err != nil || !found {
					t.Fatal("abort did not restore ordinary B lifecycle", found, err)
				}
				if got := srv.State.ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
					t.Fatal("aborted B leaked", got)
				}
			case resource.TypeReleaseRetained:
				if got := srv.State.ResourceSnapshot(); got.Reserved.MemoryBytes != 0 || got.ReservationCount != 0 {
					t.Fatalf("rotated prepared release leaked B: %+v", got)
				}
			}
		})
	}
}

func TestPreparedCaptureDoesNotDetachOtherLeaseOrCgroup(t *testing.T) {
	for _, mismatch := range []string{"pid", "lease", "cgroup"} {
		t.Run(mismatch, func(t *testing.T) {
			srv, _, capture := directRetentionFixture(t)
			if response := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "original-token"); response.Type != TypeAck {
				t.Fatal(response.Msg)
			}
			state := srv.State
			state.mu.Lock()
			active := state.bySID["sid"]
			switch mismatch {
			case "pid":
				active.PeerPID++
			case "lease":
				active.LeasePath += ".replacement"
			case "cgroup":
				active.CgroupPath += ".replacement"
			}
			state.mu.Unlock()
			account := state.retainedSnapshot("sid")
			if _, err := state.commitRetained(heldRetainedCapture(account, capture), capture); err == nil {
				t.Fatal("different producer identity committed the capture")
			}
			if err := state.abortRetained(withoutRetainedCapture(account, capture), capture); err != nil {
				t.Fatal(err)
			}
			if active, ok := state.retentionReservation("sid"); !ok || !active.RetentionPrepared {
				t.Fatal("old capture modified the replacement reservation", active)
			}
			if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
				t.Fatal("replacement charge changed", got)
			}
		})
	}
}
