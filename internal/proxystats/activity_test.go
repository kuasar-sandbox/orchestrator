package proxystats

import (
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestActivityInitialIdleAndWorkerRecovery(t *testing.T) {
	m := NewMasterStats(nil, []string{"w"})
	n := timePoint{wall: time.Now(), bootNS: 100}
	m.now = func() timePoint { return n }
	m.trustSince = n
	route := routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning, Hold: &routesync.HoldActivity{}}
	m.ObserveRoute(route)
	if m.Activity(route).State != "unknown" {
		t.Fatal("incomplete initial observation certified")
	}
	readyWorker(t, m, "w", 1)
	first := m.Activity(route)
	if first.State != "idle" || first.SinceBootNS != 100 {
		t.Fatal(first)
	}
	n.bootNS = 200
	n.wall = n.wall.Add(100)
	m.ObserveRoute(route)
	if m.Activity(route) != first {
		t.Fatal("metadata replay reset idle")
	}
	m.StreamFault("w", 1)
	if m.Activity(route).State != "unknown" {
		t.Fatal("fault retained candidate")
	}
	m.WorkerExited("w", 1)
	readyWorker(t, m, "w", 2)
	second := m.Activity(route)
	if second.State != "idle" || second.Generation <= first.Generation || second.SinceBootNS != 200 {
		t.Fatal(second)
	}
	m.InplaceRestart = func() bool { return true }
	if m.Activity(route).State != "unknown" {
		t.Fatal("hot restart certified")
	}
	m.InplaceRestart = nil
	m.ResetObservation()
	if m.Activity(route).State != "unknown" {
		t.Fatal("resync retained observation")
	}
	n.bootNS = 300
	n.wall = n.wall.Add(100)
	m.ObserveRoute(route)
	if m.Activity(route).SinceBootNS != 300 {
		t.Fatal("fresh observation not rebuilt")
	}
}
