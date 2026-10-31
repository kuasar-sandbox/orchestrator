package proxystats

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// PauseActivity samples under the same per-SID mutex as parking acquisition and
// backend attachment. The gate must already be visible to activation readers.
// Checking it under this mutex orders the sample with even unlimited traffic.
func (w *WorkerStats) PauseActivity(sid string, gated func() bool) (map[string]routesync.PauseActivity, error) {
	if w == nil || !validSandboxID(sid) {
		return nil, fmt.Errorf("invalid pause statistics target")
	}
	entry := w.lockEntry(sid, true)
	defer entry.mu.Unlock()
	out := make(map[string]routesync.PauseActivity, len(allServices))
	for _, service := range allServices {
		state := entry.services[service]
		out[service] = routesync.PauseActivity{Parking: state.parking, Connected: state.egress}
	}
	if gated == nil || !gated() {
		return out, fmt.Errorf("pause gate is not current")
	}
	return out, nil
}
