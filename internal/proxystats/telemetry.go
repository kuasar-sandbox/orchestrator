package proxystats

// TelemetrySnapshot is a trusted, read-only projection of the same master-owned
// worker contributions used for traffic diagnostics. No extra worker polling.
type TelemetrySnapshot struct {
	SandboxID string
	RunID     string
	State     string
	Hold      uint64
	HoldKnown bool
}

func (m *MasterStats) MetricsSnapshot() ([]TelemetrySnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.availableLocked() {
		return nil, false
	}
	result := make([]TelemetrySnapshot, 0, len(m.routes))
	for sid, route := range m.routes {
		if route.state != "starting" && route.state != "running" && route.state != "pausing" && route.state != "paused" {
			continue
		}
		item := TelemetrySnapshot{
			SandboxID: sid, RunID: route.runID, State: route.state,
			HoldKnown: route.holdKnown && !m.physicalOnly, Hold: route.hold.egress,
		}
		result = append(result, item)
	}
	return result, true
}
