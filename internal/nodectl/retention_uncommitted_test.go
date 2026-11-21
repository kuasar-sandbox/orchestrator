package nodectl

import (
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestRetainedMemoryUncommittedProducerDeathAbortsPreparedReceipt(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "periodic", true: "inventory"}[restart], func(t *testing.T) {
			root := shortTestDir(t)
			producer := startRetentionHolderProcess(t, root)
			socket := filepath.Join(root, "ctl.sock")
			lease, _, _, err := resource.InspectLeaseIdentity(resource.LeasePath(socket, "sid"))
			if err != nil {
				t.Fatal(err)
			}
			state := makeState(8<<30, 0)
			_, _, err = state.Admit(AdmitSpec{SandboxID: "sid", Token: "source-token", PeerPID: producer.cmd.Process.Pid,
				CgroupPath: lease.CgroupPath, LeasePath: resource.LeasePath(socket, "sid"),
				Capacity:              Resources{MemoryBytes: lease.CapacityMemory, CPUMilli: lease.CapacityCPUMilli},
				ConfiguredAllocatable: Resources{MemoryBytes: lease.FloorMemory, CPUMilli: lease.FloorCPUMilli},
				InitialBudget:         256 << 20, ClientFeatures: lease.ClientFeatures})
			if err != nil {
				t.Fatal(err)
			}
			inventory := &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{filepath.Join(root, "cgroups")}, Pool: state.AllocatablePool, Logf: t.Logf}
			srv := &Server{Identity: socket, State: state, Inventory: inventory}
			capture := resource.CaptureRetention{CapturePath: producer.path, MemoryID: producer.record.MemoryID, Epoch: producer.record.Epoch, OperationID: producer.record.HostOperationID}
			response := srv.handleRetained(producer.cmd.Process.Pid, &Message{Type: resource.TypePrepareRetained, SandboxID: "sid", Retention: &capture}, "source-token")
			if response.Type != TypeAck {
				t.Fatal(response.Msg)
			}
			if err := state.ReconcileRetainedMemory("sid"); err != nil {
				t.Fatal(err)
			}
			if _, found := state.RetainedMemory("sid"); !found {
				t.Fatal("live writer lost prepared responsibility")
			}
			producer.kill(t)
			if restart {
				state = makeState(8<<30, 0)
				if err := inventory.Recover(state); err != nil {
					t.Fatal(err)
				}
				if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
					t.Fatalf("dead source reservation restored after abort: %d", got)
				}
			} else {
				if err := state.ReconcileRetainedMemory("sid"); err != nil {
					t.Fatal(err)
				}
				if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != 256<<20 {
					t.Fatalf("capture abort released ordinary B: %d", got)
				}
				if _, found, err := state.Release("source-token"); err != nil || !found {
					t.Fatalf("ordinary lease release: %t %v", found, err)
				}
				if got := state.ResourceSnapshot().Reserved.MemoryBytes; got != 0 {
					t.Fatalf("ordinary source reservation remained: %d", got)
				}
			}
			if _, found := state.RetainedMemory("sid"); found {
				t.Fatal("abandoned prepared capture remained charged")
			}
			if account, err := readRetainedReceipt(retentionPath(socket, "sid")); err != nil || len(account.Captures) != 0 {
				t.Fatalf("abandoned capture left entries or lost SID header: %+v %v", account, err)
			}
			record, err := memorycheckpoint.ReadCaptureRecord(producer.path)
			if err != nil || !record.Aborted || record.RuntimeRetired || record.RAMReleased || record.Committed {
				t.Fatalf("wrong recovered preparation facts: %+v %v", record, err)
			}
			// Recovery is idempotent after durable intent removal.
			if err := state.ReconcileRetainedMemory("sid"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
