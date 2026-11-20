package nodectl

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func recoveredPoolServer(srv *Server, state *State) *Server {
	recovered := &Server{Identity: srv.Identity, State: state, Inventory: srv.Inventory}
	recovered.Admission = NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100, StartupTTL: time.Minute})
	recovered.Admission.SetWiring(state, nil, recovered.BuildAdmitOKFromQueue)
	return recovered
}

func TestRetainedMemorySettledInventorySurvivesRemovedSourcePool(t *testing.T) {
	for _, mode := range []string{"settled-receipt", "released-proof", "collected-proof", "active-successor"} {
		t.Run(mode, func(t *testing.T) {
			srv, writer, capture := committedRetentionFixture(t)
			if err := writer.Update(func(r *memorycheckpoint.CaptureRecord) {
				r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload"
			}); err != nil {
				t.Fatal(err)
			}
			if mode != "released-proof" {
				if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypeReleaseRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
					t.Fatal(resp.Msg)
				}
			}
			var replay *Message
			if mode == "active-successor" {
				// The old pool has drained. The successor now uses a different
				// configured pool while the tombstone retains the source name.
				srv.State.PoolName = "replacement"
				_, replay = adoptionCandidate(t, srv, capture, "active-owner")
				if _, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), replay, nil); err != nil {
					t.Fatal(err)
				}
			}
			before := srv.State.retainedSnapshot("sid")
			if mode == "collected-proof" {
				if err := os.Remove(writer.Path()); err != nil {
					t.Fatal(err)
				}
			}
			state := makeState(8<<30, 0)
			state.PoolName, state.Wm.StartupFactor = "replacement", .75
			if err := srv.Inventory.Recover(state); err != nil {
				t.Fatalf("settled historical pool prevented inventory startup: %v", err)
			}
			r := state.retainedSnapshot("sid")
			if r == nil || !r.Settled || r.Capture != capture || r.Memory != before.Memory || r.OwnerFile != before.OwnerFile || r.OwnerToken != before.OwnerToken {
				t.Fatal("restart lost settled replay or owner identity")
			}
			if _, pending := state.RetainedMemory("sid"); pending {
				t.Fatal("settled tombstone was restored as R")
			}
			want := uint64(0)
			if mode == "active-successor" {
				want = 1 << 30 // Ordinary lease recovery reserves the live B's Capacity.
			}
			if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != want {
				t.Fatalf("historical pool recreated a charge: got %d want %d", got, want)
			}
			if mode == "collected-proof" {
				return // GC need not retain old data to recover an already settled receipt.
			}
			recovered := recoveredPoolServer(srv, state)
			if replay == nil {
				_, replay = adoptionCandidate(t, recovered, capture, "new-owner")
			}
			resp, err := recovered.buildRetainedAdmitOK(nil, os.Getpid(), replay, nil)
			if err != nil || resp.Status != StatusAdmitted {
				t.Fatalf("successor admission/replay after source pool removal: %v", err)
			}
			if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
				t.Fatalf("successor was not charged exactly once in its current pool: %d", got)
			}
			if mode == "active-successor" && resp.Token != before.OwnerToken {
				t.Fatal("active successor replay changed the persisted handoff token")
			}
		})
	}
}

func TestRetainedMemoryUnsettledInventoryRequiresSourcePool(t *testing.T) {
	for _, stage := range []string{retentionPrepared, retentionHolding} {
		t.Run(stage, func(t *testing.T) {
			var srv *Server
			if stage == retentionHolding {
				srv, _, _ = committedRetentionFixture(t)
			} else {
				var capture resource.CaptureRetention
				srv, _, capture = directRetentionFixture(t)
				if resp := srv.handleRetained(os.Getpid(), &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "original-token"); resp.Type != TypeAck {
					t.Fatal(resp.Msg)
				}
			}
			state := makeState(8<<30, 0)
			state.PoolName = "replacement"
			if err := srv.Inventory.Recover(state); err == nil || !strings.Contains(err.Error(), "unknown retained memory pool") {
				t.Fatalf("unsettled source pool disappearance was accepted: %v", err)
			}
			r, err := readRetainedReceipt(retentionPath(srv.Identity, "sid"))
			if err != nil || r.Settled || r.Stage != stage || r.Memory.SourcePool != "default" {
				t.Fatalf("failed recovery rewrote source responsibility: %v", err)
			}
		})
	}
}
