package proxystats

import (
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestPauseLastZeroSurvivesBlockedAsyncStats(t *testing.T) {
	w := NewWorkerStats()
	n := timePoint{wall: time.Now(), bootNS: 100}
	w.now = func() timePoint { return n }
	first, err := w.PauseActivity("s", func() bool { return true })
	if err != nil || !first["forward"].NeverActive || !first["forward"].Sampled {
		t.Fatal(first, err)
	}
	flow := w.BeginParking("s", proxy.ConnectServiceForward)
	n.bootNS = 200
	n.wall = n.wall.Add(time.Second)
	flow.Close()
	// No sender runs: the Master cannot possibly have this new idle edge.
	sample, err := w.PauseActivity("s", func() bool { return true })
	if err != nil || sample["forward"].IdleSinceBootNS != 200 || sample["forward"].NeverActive {
		t.Fatal(sample, err)
	}
	if sample["forward"].IdleSinceBootNS <= 150 {
		t.Fatal("old candidate would pass")
	}
	if sample["forward"].IdleSinceBootNS > 250 {
		t.Fatal("traffic ending before t0 would fail")
	}
}
func TestPauseGatePreservesRunBaseline(t *testing.T) {
	m := NewMasterStats(nil, []string{"w"})
	n := timePoint{wall: time.Now(), bootNS: 100}
	m.now = func() timePoint { return n }
	m.trustSince = n
	readyWorker(t, m, "w", 1)
	route := routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning}
	m.ObserveRoute(route)
	n.bootNS = 200
	n.wall = n.wall.Add(100)
	route.State = routesync.StatePausing
	m.ObserveRoute(route)
	if e := m.PauseEvidence("s", "r"); e.SinceBootNS != 100 {
		t.Fatal(e)
	}
	if e := m.PauseEvidence("s", "old"); e.SinceBootNS != 0 {
		t.Fatal(e)
	}
	m.ResetObservation()
	if e := m.PauseEvidence("s", "r"); e.SinceBootNS != 0 {
		t.Fatal(e)
	}
}
