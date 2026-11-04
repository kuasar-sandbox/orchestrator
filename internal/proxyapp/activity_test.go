package proxyapp

import (
	"strconv"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestDisabledActivitySnapshotDoesNotAllocatePerRoute(t *testing.T) {
	i := NewInstance("activity-test", "boot", "", 0)
	i.activityRoutes = make(map[string]routesync.RouteEntry)
	for n := 0; n < 10000; n++ {
		sid := strconv.Itoa(n)
		i.activityRoutes[sid] = routesync.RouteEntry{SandboxID: sid, RunID: "run", State: routesync.StateRunning}
	}
	s := &readinessSink{instance: i}
	if allocs := testing.AllocsPerRun(10, func() {
		if got := s.ActivitySnapshot(); len(got) != 0 {
			t.Fatalf("disabled routes reported activity: %d", len(got))
		}
	}); allocs != 0 {
		t.Fatalf("disabled policy snapshot allocated %g objects", allocs)
	}
	zero := int64(0)
	i.activityRoutes["enabled"] = routesync.RouteEntry{SandboxID: "enabled", RunID: "run", IdleTimeout: &zero}
	got := s.ActivitySnapshot()
	if len(got) != 1 || got[0].SandboxID != "enabled" || got[0].State != "unknown" {
		t.Fatalf("enabled route omitted: %+v", got)
	}
}
