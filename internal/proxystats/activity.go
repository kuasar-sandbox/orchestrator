package proxystats

import "github.com/kuasar-sandbox/orchestrator/internal/routesync"

func (m *MasterStats) Activity(r routesync.RouteEntry) routesync.Activity {
	a := routesync.Activity{SandboxID: r.SandboxID, RunID: r.RunID, State: "unknown"}
	if m.InplaceRestart != nil && m.InplaceRestart() {
		return a
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	marker := m.routes[r.SandboxID]
	if !marker.holdKnown || marker.state != routesync.StateRunning || r.State != routesync.StateRunning || r.TrafficPolicyInvalid || !m.availableLocked() || marker.runID != r.RunID || marker.since.bootNS <= 0 {
		return a
	}
	a.Generation = m.trustSince.bootNS
	if marker.hold.egress != 0 {
		a.State = "busy"
		return a
	}
	a.SinceBootNS = max(laterPoint(marker.since, m.trustSince).bootNS, marker.hold.idle.bootNS)
	a.State = "idle"
	for _, v := range m.traffic[r.SandboxID].services {
		if v.parking != 0 || v.egress != 0 {
			a.State = "busy"
			a.SinceBootNS = 0
			return a
		}
		a.SinceBootNS = max(a.SinceBootNS, v.idle.bootNS)
	}
	return a
}
