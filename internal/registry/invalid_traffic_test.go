package registry

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestInvalidTrafficUpdateDoesNotDeleteRegistryOwnership(t *testing.T) {
	for _, state := range []string{routesync.StateStarting, routesync.StateRunning, routesync.StatePaused} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			reg := testReg(t)
			sid := "sb-invalid"
			record := testE2BSandboxRecord("/g", "rk", sid, "n1", StatePaused)
			if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
				t.Fatal(err)
			}
			ref := testNodeSandboxRef("/g", "rk", sid, "e2b", testAPIFingerprint)
			if err := reg.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
				t.Fatal(err)
			}
			route := testE2BRoute(sid, state)
			route.TrafficPolicyInvalid = true
			reg.applyRoute(ctx, "n1", &route)
			stored, _, found, err := reg.stores.GetSandbox(ctx, "/g", "rk")
			if err != nil || !found || stored.NodeSandboxID != record.NodeSandboxID {
				t.Fatalf("lost sandbox=%+v found=%v err=%v", stored, found, err)
			}
			storedRef, found, err := reg.stores.GetNodeSandboxRef(ctx, "n1", record.NodeSandboxID)
			if err != nil || !found || storedRef != ref {
				t.Fatalf("lost ownership=%+v found=%v err=%v", storedRef, found, err)
			}
		})
	}
}
