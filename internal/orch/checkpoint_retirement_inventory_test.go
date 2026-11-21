package orch

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestDeletingCheckpointWaitsForPublishedControllerInventory(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	f.o.cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true}
	m := retiredTestOwnership(t, f)
	dir := shortOrchestratorTestDir(t)
	socket, cgroups := filepath.Join(dir, "r.sock"), filepath.Join(dir, "cgroups")
	newState := func() *nodectl.State {
		return nodectl.NewState(8<<30, 8000, 0, 0, nodectl.Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	}
	inventory := &nodectl.Inventory{ControllerSocket: socket, CgroupScanPaths: []string{cgroups}}
	oldState := newState()
	oldServer := &nodectl.Server{Path: socket, State: oldState, Inventory: inventory}
	if err := oldServer.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- oldServer.Serve(ctx) }()
	var conn net.Conn
	stop := func() {
		if done == nil {
			return
		}
		if conn != nil {
			_ = conn.Close()
		}
		cancel()
		_ = oldServer.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
		done = nil
	}
	t.Cleanup(stop)
	lease, err := resource.CreateLease(resource.Lease{Version: resource.LeaseVersion, SandboxID: f.sb.ID, PID: os.Getpid(),
		ControllerSocket: socket, CgroupPath: filepath.Join(cgroups, "source"), CapacityMemory: 1 << 30, CapacityCPUMilli: 1000,
		FloorMemory: 128 << 20, FloorCPUMilli: 500, StartupMemory: 256 << 20,
		ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	if _, _, err := oldState.Admit(nodectl.AdmitSpec{SandboxID: f.sb.ID, PeerPID: os.Getpid(), Token: "source-token", CgroupPath: filepath.Join(cgroups, "source"),
		Capacity: nodectl.Resources{MemoryBytes: 1 << 30, CPUMilli: 1000}, InitialBudget: 256 << 20,
		ConfiguredAllocatable: nodectl.Resources{MemoryBytes: 128 << 20, CPUMilli: 500},
		ClientFeatures:        []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}, LeasePath: lease.Path()}); err != nil {
		t.Fatal(err)
	}
	start, err := memorycheckpoint.ProcessStartTime(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	w := testCaptureRecord(t, f, m, func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired, r.Holder.StartTime = false, false, start
	})
	capture := resource.CaptureRetention{CapturePath: w.Path(), MemoryID: m.MemoryID, Epoch: m.MemoryEpoch, OperationID: m.RetirementOperationID}
	conn, err = net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
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
	rpc(resource.TypeCommitRetained)
	if retained, pending := oldState.RetainedMemory(f.sb.ID); !pending || retained.Bytes != 256<<20 {
		t.Fatalf("test did not create a durable retained obligation: %+v %v", retained, pending)
	}
	stop()
	// The holder finishes while the controller is down, before the durable R
	// receipt can be settled. Its capture record is the only release proof.
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Update(func(r *memorycheckpoint.CaptureRecord) {
		r.FileReady, r.RAMReleased, r.PayloadRef = true, true, "payload.memory"
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(socket+".retained", resource.LeaseFilename(f.sb.ID))
	readSettled := func() bool {
		t.Helper()
		data, err := os.ReadFile(receiptPath)
		if err != nil {
			t.Fatal(err)
		}
		var receipt struct {
			Captures []struct {
				Stage string `json:"stage"`
			} `json:"captures"`
		}
		if err := json.Unmarshal(data, &receipt); err != nil {
			t.Fatal(err)
		}
		return len(receipt.Captures) == 1 && receipt.Captures[0].Stage == "settled"
	}
	if readSettled() {
		t.Fatal("test accidentally persisted memory settlement before restart")
	}
	beginDeletingForTest(t, f)
	state := newState()
	admission := nodectl.NewAdmissionController(nodectl.AdmissionPolicy{Rate: 100, Burst: 100})
	if err := f.o.ConfigureMemoryPressure(context.Background(), state, admission, nodectl.DefaultPressurePolicy()); err != nil {
		t.Fatal(err)
	}
	if f.o.memoryPressure.Load() == nil || state.InventoryReady() {
		t.Fatal("test did not stop between configuration publication and inventory recovery")
	}
	if _, pending := state.RetainedMemory(f.sb.ID); pending {
		t.Fatal("unrecovered ledger unexpectedly contained the persisted receipt")
	}
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); !errors.Is(err, errRetirementPending) {
		t.Fatalf("deleting retry accepted unrecovered inventory: %v", err)
	}
	if err := f.o.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatalf("inventory-pending cleanup blocked conductor startup: %v", err)
	}
	if pending, err := f.o.retainedMemoryPending(context.Background(), f.sb.ID); !pending || !errors.Is(err, errRetirementPending) {
		t.Fatalf("admission treated unrecovered inventory as ready: %v %v", pending, err)
	}
	m.ParentRetired = true
	if err := f.o.retireHolderExecution(m); !errors.Is(err, errRetirementPending) {
		t.Fatalf("execution retirement accepted unrecovered inventory: %v", err)
	}
	if err := f.o.abortCheckpointRetirement(context.Background(), f.sb, ctl.HostRetirementPermit{OperationID: m.RetirementOperationID}); !errors.Is(err, errRetirementPending) {
		t.Fatalf("rollback accepted unrecovered inventory: %v", err)
	}
	if err := f.o.runs.forget(m.RunID); !errors.Is(err, errRetirementPending) {
		t.Fatalf("queued ownership cleanup accepted unrecovered inventory: %v", err)
	}
	if row, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || row == nil || row.State != types.StateDeleting {
		t.Fatalf("unrecovered controller lost deleting ownership: %+v %v", row, err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
		t.Fatalf("unrecovered controller lost holder identity: %v %v", found, err)
	}
	if _, err := os.Stat(w.Path()); err != nil || readSettled() {
		t.Fatalf("unrecovered controller removed or fabricated release proof: %v", err)
	}
	server := &nodectl.Server{Path: socket, State: state, Inventory: inventory}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(); _ = server.Owner.Close() })
	if !state.InventoryReady() || !readSettled() {
		t.Fatal("successful recovery did not persist settlement before publishing readiness")
	}
	if _, pending := state.RetainedMemory(f.sb.ID); pending || state.ResourceSnapshot().Reserved.MemoryBytes != 0 {
		t.Fatal("settled source was restored as a retained memory charge")
	}
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
		t.Fatalf("ready-controller cleanup retry failed: %v", err)
	}
	if row, err := f.o.st.Get(context.Background(), f.sb.ID); err != nil || row != nil {
		t.Fatalf("ready-controller cleanup retained deleting row: %+v %v", row, err)
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
		t.Fatalf("ready-controller cleanup retained holder identity: %v %v", found, err)
	}
	if _, err := os.Stat(w.Path()); !os.IsNotExist(err) {
		t.Fatalf("terminal source remains: %v", err)
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatalf("terminal SID retained empty account header: %v", err)
	}
}
