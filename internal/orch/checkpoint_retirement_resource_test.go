package orch

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// Exercise the actual node ledger and retention protocol. In particular, no
// fake ready response may hide a placement still owned by the frozen producer.
func TestPendingHolderRetirementReleasesPlacementBeforeMemorySave(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	f.o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	m := retiredTestOwnership(t, f)
	state := nodectl.NewState(8<<30, 8000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	f.o.memoryPressure.Store(&nodePressureController{state: state})
	capacity := nodectl.Resources{MemoryBytes: 1 << 30, CPUMilli: 1000}
	if _, err := state.ReservePlacement(f.sb.ID, m.RunID, capacity, nodectl.OperationCreate, "", nil); err != nil {
		t.Fatal(err)
	}

	// Keep the UDS independent of the longer sandbox fixture path.
	dir := t.TempDir()
	socket, cgroups := filepath.Join(dir, "r.sock"), filepath.Join(dir, "cgroups")
	srv := &nodectl.Server{Path: socket, State: state,
		Inventory: &nodectl.Inventory{ControllerSocket: socket, CgroupScanPaths: []string{cgroups}, Pool: state.AllocatablePool}}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = srv.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	lease, err := resource.CreateLease(resource.Lease{Version: resource.LeaseVersion, SandboxID: f.sb.ID, PID: os.Getpid(),
		ControllerSocket: socket, CgroupPath: filepath.Join(cgroups, "source"), CapacityMemory: capacity.MemoryBytes, CapacityCPUMilli: capacity.CPUMilli,
		FloorMemory: 128 << 20, FloorCPUMilli: 500, StartupMemory: 256 << 20,
		ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	if _, _, err := state.Admit(nodectl.AdmitSpec{SandboxID: f.sb.ID, PeerPID: os.Getpid(), Token: "source-token", CgroupPath: filepath.Join(cgroups, "source"),
		Capacity: capacity, InitialBudget: 256 << 20, ConfiguredAllocatable: nodectl.Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}, LeasePath: lease.Path()}); err != nil {
		t.Fatal(err)
	}
	start, err := memorycheckpoint.ProcessStartTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	w := testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired = false, false
		r.Holder.StartTime = start
	})
	capture := resource.CaptureRetention{CapturePath: w.Path(), MemoryID: m.MemoryID, Epoch: m.MemoryEpoch, OperationID: m.RetirementOperationID}
	conn, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rpc := func(kind string) {
		t.Helper()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := resource.WriteMessage(conn, &resource.Message{Type: kind, SandboxID: f.sb.ID, Token: "source-token", Retention: &capture}); err != nil {
			t.Fatal(err)
		}
		response, err := resource.ReadMessage(conn)
		if err != nil || response.Type != resource.TypeAck {
			t.Fatalf("%s = %+v %v", kind, response, err)
		}
	}
	rpc(resource.TypePrepareRetained)
	if err := w.Update(func(r *memorycheckpoint.CaptureRecord) { r.Committed, r.RuntimeRetired = true, true }); err != nil {
		t.Fatal(err)
	}
	event := runRetirementEvent(m)
	if _, err := f.o.RunRetirementRole(context.Background(), runKindSandbox, m.RunID, &event); !errors.Is(err, errRetirementPending) {
		t.Fatalf("prepared B must block placement retirement: %v", err)
	}
	if got := state.ResourceSnapshot(); got.ReservationCount != 1 || got.Reserved.MemoryBytes != 256<<20 {
		t.Fatalf("parent ACK released protected B: %+v", got)
	}
	if _, err := state.ReservePlacement(f.sb.ID, "successor", capacity, nodectl.OperationRecovery, "", nil); err == nil {
		t.Fatal("prepared B allowed a successor before resource commit")
	}
	rpc(resource.TypeCommitRetained)
	// A lost parent reply can recover through the existing run-end worker.
	if err := f.o.reconcileRetiredRun(context.Background(), m); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("pending holder reconciliation = %v", err)
	}
	if got := w.Read(); got.FileReady || got.RAMReleased {
		t.Fatal("test accidentally waited for memory saving")
	}
	if r, ok := state.RetainedMemory(f.sb.ID); !ok || r.Bytes != 256<<20 || r.MemoryID != m.MemoryID {
		t.Fatalf("retirement lost frozen memory: %+v %v", r, ok)
	}
	if got := state.ResourceSnapshot(); got.ReservationCount != 0 || got.Reserved.MemoryBytes != 256<<20 {
		t.Fatalf("holding charge must be B=0, R=256MiB: %+v", got)
	}
	if _, err := state.ReservePlacement(f.sb.ID, "successor", capacity, nodectl.OperationRecovery, "", nil); err != nil {
		t.Fatalf("resume cannot reserve while predecessor RAM is pending: %v", err)
	}
	if _, err := f.o.RunRetirementRole(context.Background(), runKindSandbox, m.RunID, &event); err != nil {
		t.Fatalf("old parent ACK replay: %v", err)
	}
	if _, err := state.ReservePlacement(f.sb.ID, "unexpected-third-run", capacity, nodectl.OperationRecovery, "", nil); err == nil {
		t.Fatal("old parent replay removed successor placement")
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found || f.o.runs.unit(m.RunID) != m.Unit {
		t.Fatalf("pending holder lost durable unit identity: %v %v", found, err)
	}
	if _, stops := f.lc.stopSnapshot(); stops != 0 {
		t.Fatal("placement retirement stopped the frozen holder")
	}
}

func TestDeletingCheckpointWaitsForResourceControllerDuringStartup(t *testing.T) {
	for _, hasRecord := range []bool{false, true} {
		name := "unaccepted capture intent"
		if hasRecord {
			name = "released capture"
		}
		t.Run(name, func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			f.o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
			m := retiredTestOwnership(t, f)
			if hasRecord {
				w := testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
					r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
				})
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
				if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(f.sb.BaseDir, "checkpoint"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			beginDeletingForTest(t, f)
			// Conductor calls this before StartResourceController. Unavailable
			// accounting must leave a retry owner, not prevent conductor startup.
			if err := f.o.ReconcileSandboxes(context.Background()); err != nil {
				t.Fatalf("startup was blocked before controller could start: %v", err)
			}
			if row, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || row == nil || row.State != types.StateDeleting {
				t.Fatalf("controller-unavailable cleanup lost deleting owner: %+v %v", row, err)
			}
			if owner, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found || owner.RetirementOperationID != m.RetirementOperationID {
				t.Fatalf("controller-unavailable cleanup lost capture intent: %+v %v", owner, err)
			}
			if _, err := os.Stat(f.sb.BaseDir); err != nil {
				t.Fatalf("controller-unavailable cleanup removed source: %v", err)
			}
			attachPressure(t, f.o)
			if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
				t.Fatalf("ready-controller retry failed: %v", err)
			}
			if row, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || row != nil {
				t.Fatalf("ready-controller retry retained deleting row: %+v %v", row, err)
			}
			if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
				t.Fatalf("ready-controller retry retained holder: %v %v", found, err)
			}
		})
	}
}
