package proxystats

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func (m *MasterStats) clockLocked() {
	if m.clockWatch != nil {
		changed, err := m.clockWatch.Poll()
		if changed || err != nil {
			m.clock.Broken = true
			m.clock.Unbounded = true
		}
		m.clockUnavailable = err != nil
	}
	n := m.now()
	// Fence a clock set between the first Poll and the paired time read too.
	if m.clockWatch != nil {
		changed, err := m.clockWatch.Poll()
		if changed || err != nil {
			m.clock.Broken = true
			m.clock.Unbounded = true
		}
		m.clockUnavailable = m.clockUnavailable || err != nil
	}
	m.clock.Observe(idleclock.Point{Wall: n.wall, BootNS: n.bootNS})
}
func (m *MasterStats) RunClock(ctx context.Context) {
	m.mu.Lock()
	watch := m.clockWatch
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if watch != nil {
			watch.Close()
		}
		m.clockWatch = nil
		m.clockUnavailable = true
		m.mu.Unlock()
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		m.clockLocked()
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ObserveRoute establishes the RunID baseline before installing a later gate.
// Repeated metadata and temporary pausing publications preserve that baseline.
func (m *MasterStats) ObserveRoute(r routesync.RouteEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clockLocked()
	if r.State == routesync.StateDead || r.State == routesync.StateDeleting {
		delete(m.routes, r.SandboxID)
		return
	}
	observed := routeObservation{runID: r.RunID, state: r.State}
	if h := r.Hold; h.Valid() {
		observed.holdKnown = true
		observed.hold.egress = uint64(h.Value)
		if h.IdleSince != nil {
			observed.hold.idle = timePoint{wall: h.IdleSince.UTC(), bootNS: h.IdleSinceBootNS}
		}
	}
	old := m.routes[r.SandboxID]
	if old.runID == r.RunID && (r.State == routesync.StateRunning || r.State == routesync.StatePausing) {
		observed.since = old.since
	}
	if r.State == routesync.StateRunning && observed.since.bootNS == 0 {
		observed.since = m.now()
	}
	m.routes[r.SandboxID] = observed
}
func (m *MasterStats) ResetObservation() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.routes)
	m.trustSince = m.now()
}

// PauseEvidence returns only the run and worker-set baselines. The barrier
// samples physical flows synchronously in each worker; Conductor checks hold
// once at its authority. Neither may use the master's async stats cache.
func (m *MasterStats) PauseEvidence(sid, run string) routesync.IdleEvidence {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clockLocked()
	marker := m.routes[sid]
	if !m.availableLocked() || marker.runID != run || marker.since.bootNS <= 0 {
		return routesync.IdleEvidence{}
	}
	proof := m.clock
	if m.clockUnavailable {
		proof = idleclock.Proof{}
	}
	baseline := laterPoint(marker.since, m.trustSince)
	return routesync.IdleEvidence{Points: []idleclock.Point{{Wall: baseline.wall, BootNS: baseline.bootNS}}, SinceBootNS: baseline.bootNS, Clock: proof}
}

type clockWatcher interface {
	Poll() (bool, error)
	Close()
}
