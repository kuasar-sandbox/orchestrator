package proxystats

import (
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type fakeClockWatch struct {
	changed bool
	err     error
}

func (w *fakeClockWatch) Poll() (bool, error) { c := w.changed; w.changed = false; return c, w.err }
func (w *fakeClockWatch) Close()              {}
func TestClockWatchDetectsUnobservedStepsAndFailure(t *testing.T) {
	m := NewMasterStats(nil, []string{"w"})
	if m.clockWatch != nil {
		m.clockWatch.Close()
	}
	watch := &fakeClockWatch{}
	m.clockWatch = watch
	n := timePoint{wall: time.Now(), bootNS: int64(time.Second)}
	m.now = func() timePoint { return n }
	m.trustSince = n
	readyWorker(t, m, "w", 1)
	m.ObserveRoute(routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning})
	// Both wall and BOOTTIME advanced normally, but a realtime cancellation
	// records a forward/back step between these two samples.
	n.wall = n.wall.Add(time.Second)
	n.bootNS += int64(time.Second)
	watch.changed = true
	e := m.PauseEvidence("s", "r")
	if !e.Clock.Discontinuous || e.Clock.UncertainThrough.IsZero() {
		t.Fatal("lost timerfd cancellation")
	}
	watch.err = errors.New("rearm failed")
	e = m.PauseEvidence("s", "r")
	if e.Clock.Start.BootNS != 0 {
		t.Fatal("watch failure certified public time")
	}
	watch.err = nil
	n.wall = n.wall.Add(time.Second)
	n.bootNS += int64(time.Second)
	e = m.PauseEvidence("s", "r")
	if e.Clock.Start.BootNS == 0 {
		t.Fatal("watch recovery unavailable")
	}
}

func TestClockUncertaintySurvivesObservationRebuild(t *testing.T) {
	m := NewMasterStats(nil, []string{"w"})
	if m.clockWatch != nil {
		m.clockWatch.Close()
	}
	m.clockWatch = &fakeClockWatch{changed: true}
	n := timePoint{wall: time.Now(), bootNS: int64(time.Second)}
	m.now = func() timePoint { return n }
	m.trustSince = n
	readyWorker(t, m, "w", 1)
	m.ObserveRoute(routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning})
	m.ResetObservation()
	n.wall = n.wall.Add(time.Hour)
	n.bootNS += int64(time.Hour)
	m.ObserveRoute(routesync.RouteEntry{SandboxID: "s", RunID: "next", State: routesync.StateRunning})
	if !m.PauseEvidence("s", "next").Clock.Unbounded {
		t.Fatal("run rebuild erased clock uncertainty")
	}
}
