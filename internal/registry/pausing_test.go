package registry

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestReservePausingPreservesOwner(t *testing.T) {
	for _, placementEntry := range []bool{false, true} {
		for _, accepted := range []bool{false, true} {
			t.Run(fmt.Sprintf("placement=%v/accepted=%v", placementEntry, accepted), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				reg := testReg(t)
				original := testE2BSandboxRecord("/g", "rk", "pausing-owner", "n1", StatePausing)
				if _, err := reg.stores.PutSandbox(ctx, original); err != nil {
					t.Fatal(err)
				}
				if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:12345"}); err != nil {
					t.Fatal(err)
				}
				if err := reg.stores.AddNodeSandboxRef(ctx, "n1", testNodeSandboxRef("/g", "rk", original.SandboxID, original.Profile, original.APISecretFingerprint)); err != nil {
					t.Fatal(err)
				}
				var placements, commands atomic.Int32
				reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
					placements.Add(1)
					return nil, ErrNoNode
				}))
				reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
					commands.Add(1)
					if cmd.Kind != routesync.CmdConnect || cmd.SID != original.NodeSandboxID {
						t.Errorf("capture owner replaced: %+v", cmd)
					}
					before, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
					if err != nil || !found || before.State != StatePausing || before.SandboxGeneration != original.SandboxGeneration {
						t.Errorf("pausing overwritten before Connect: %+v, %v", before, err)
					}
					// A node may publish capture completion while Connect is queued.
					// Rejection must not roll this progress back to pausing.
					paused := testRouteFromRecord(original, original.NodeSandboxID, routesync.StatePaused)
					reg.applyRoute(ctx, "n1", &paused)
					status := routesync.AckRejected
					if accepted {
						status = routesync.AckAccepted
					}
					go reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: status})
					if accepted {
						running := testRouteFromRecord(original, original.NodeSandboxID, routesync.StateRunning)
						reg.applyRoute(ctx, "n1", &running)
					}
				}})
				var err error
				if placementEntry {
					// The initial Reserve read can become stale before the placement
					// loop rereads the route; this entry must also preserve pausing.
					err = reg.placeAndCreate(ctx, "/g", "rk", nil, nil, nil, true)
				} else {
					var result *ReserveResult
					result, err = reg.ReserveSandbox(ctx, testCreateReserve("/g", "rk", nil))
					if err == nil && (result.Route.SandboxID != original.SandboxID || result.Route.NodeSandboxID != original.NodeSandboxID) {
						t.Fatalf("changed result identity: %+v", result)
					}
				}
				if (err == nil) != accepted || placements.Load() != 0 || commands.Load() != 1 {
					t.Fatalf("err=%v placements=%d commands=%d", err, placements.Load(), commands.Load())
				}
				stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
				want := StatePaused
				if accepted {
					want = StateReady
				}
				if err != nil || !found || stored.State != want || !sameSandboxGeneration(stored, original) || stored.NextSandboxGeneration != original.NextSandboxGeneration {
					t.Fatalf("changed capture owner/progress: %+v, %v", stored, err)
				}
			})
		}
	}
}
