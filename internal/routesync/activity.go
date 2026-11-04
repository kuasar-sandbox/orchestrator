package routesync

import (
	"context"
	"time"
)

const TypeActivity = "activity"

// Activity is an absolute observation. Sequence is scoped to the registered
// control stream; Generation identifies the trusted Worker set, not an idle token.
type Activity struct {
	Sequence    uint64 `json:"sequence"`
	SandboxID   string `json:"sid"`
	RunID       string `json:"run_id"`
	Generation  int64  `json:"generation"`
	State       string `json:"state"` // idle, busy, unknown
	SinceBootNS int64  `json:"since_boot_ns"`
}
type ActivitySource interface{ ActivitySnapshot() []Activity }
type ActivityReceiver interface {
	OnProxyActivity(context.Context, string, *Activity)
}

func (s *Subscriber) forwardActivity(ctx context.Context, up chan<- *Msg) {
	source, ok := s.sink.(ActivitySource)
	if !ok {
		return
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var seq uint64
	previous := make(map[string]Activity)
	refreshed := make(map[string]time.Time)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := source.ActivitySnapshot()
			seen := make(map[string]bool, len(snapshot))
			for _, a := range snapshot {
				seen[a.SandboxID] = true
			}
			for sid := range previous {
				if !seen[sid] {
					delete(previous, sid)
					delete(refreshed, sid)
				}
			}
			for _, a := range snapshot {
				old, exists := previous[a.SandboxID]
				if exists && old == a && time.Since(refreshed[a.SandboxID]) < time.Second {
					continue
				}
				previous[a.SandboxID] = a
				refreshed[a.SandboxID] = time.Now()
				seq++
				a.Sequence = seq
				if !enqueueUp(ctx, up, &Msg{Type: TypeActivity, Activity: &a}) {
					return
				}
			}
		}
	}
}
