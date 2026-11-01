package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestResetTerminalManagementAndCreateFences(t *testing.T) {
	for _, state := range []SandboxState{StateDead, StateDeleting} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			r := testReg(t)
			r.SetNodeOwner(&recordingNodeOwner{runtime: map[string]*NodeRecord{"n1": {NodeID: "n1", APIEndpoint: "http://api:8080", DataEndpoint: "http://proxy:8081"}}})
			record := testE2BSandboxRecord("/g", "rk", "retained", "n1", state)
			record.InstanceID, record.StateVersion, record.AutoReset = "parent-1", 5, true
			rev, err := r.stores.PutSandbox(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", APIEndpoint: "http://api:8080", DataEndpoint: "http://proxy:8081", LastHeartbeatUnix: 1}); err != nil {
				t.Fatal(err)
			}
			ref := testNodeSandboxRef("/g", "rk", "retained", "e2b", testAPIFingerprint)
			if err := r.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
				t.Fatal(err)
			}
			resolved, found, err := r.ResolveSID(ctx, "/g", "rk", "retained")
			if err != nil || !found || resolved.APIEndpoint != "http://api:8080" || resolved.DataEndpoint != "" {
				t.Fatalf("terminal resolve=%+v %v %v", resolved, found, err)
			}
			if _, err := r.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil)); !errors.Is(err, ErrReserveConflict) {
				t.Fatalf("create overwrote terminal owner: %v", err)
			}
			if err := r.startReserve(ctx, "/g", "rk", record, rev, true, nil, nil, true, false); !errors.Is(err, ErrReserveConflict) {
				t.Fatalf("reserve bypassed terminal owner: %v", err)
			}
			if err := r.placeAndCreate(ctx, "/g", "rk", nil, nil, nil, true, false); !errors.Is(err, ErrReserveConflict) {
				t.Fatalf("placement bypassed terminal owner: %v", err)
			}
			// Node disappearance does not turn retained local data into a new owner.
			r.sweepNode(ctx, "n1", time.Second)
			if got, _, found, err := r.stores.GetSandbox(ctx, "/g", "rk"); err != nil || !found || got.InstanceID != record.InstanceID || got.State != state {
				t.Fatalf("reaped terminal management=%+v %v %v", got, found, err)
			}
			if _, found, err := r.stores.GetNodeSandboxRef(ctx, "n1", ref.NodeSandboxID); err != nil || !found {
				t.Fatalf("reaped retained ownership: %v %v", found, err)
			}
			// A reconnect snapshot rebuilds the same terminal projection.
			route := testRouteFromRecord(record, record.NodeSandboxID, string(state))
			route.InstanceID, route.PressureVersion = record.InstanceID, record.StateVersion
			r.applyRoute(ctx, "n1", &route)
			got, _, _, err := r.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || got.State != state || !got.AutoReset {
				t.Fatalf("reconnect changed owner policy: %+v %v", got, err)
			}
		})
	}
}

func TestResetProjectionRejectsOldRunAndAcceptsCommittedPaused(t *testing.T) {
	ctx := context.Background()
	r := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "s1", "n1", StateDead)
	record.InstanceID, record.StateVersion = "instance", 7
	if _, err := r.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := r.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", "s1", "e2b", testAPIFingerprint)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		instance string
		version  uint64
		state    string
		want     SandboxState
	}{
		{"old-parent", 99, routesync.StateRunning, StateDead},
		{"instance", 6, routesync.StateRunning, StateDead},
		{"instance", 7, routesync.StateRunning, StateDead},
		{"instance", 8, routesync.StatePaused, StatePaused},
		{"instance", 7, routesync.StateDead, StatePaused},
		{"instance", 9, routesync.StateRunning, StateReady},
		{"instance", 8, routesync.StatePaused, StateReady},
	} {
		route := testRouteFromRecord(record, record.NodeSandboxID, tc.state)
		route.InstanceID, route.PressureVersion = tc.instance, tc.version
		r.applyRoute(ctx, "n1", &route)
		got, _, found, err := r.stores.GetSandbox(ctx, "/g", "rk")
		if err != nil || !found || got.State != tc.want {
			t.Fatalf("projection %+v => %+v %v %v", tc, got, found, err)
		}
	}
}
