package nodectl

import (
	"context"
	"time"
)

// ActiveReclaimer is the controller-side counterpart to the
// per-sandbox pressure sensor. It periodically scans settled
// reservations and shrinks allocatable_now toward
// max(floor, last_reported_rss * SafetyMargin). The shrink is
// communicated to sandbox-ctl through the next Heartbeat ack
// (see Server.handleHeartbeat).
//
// In yellow / red zones the safety margin is tightened so headroom
// is recovered faster.
type ActiveReclaimer struct {
	State        *State
	Interval     time.Duration // 10s default
	SafetyMargin float64       // 1.25 default (working set + 25%)
	Logf         func(string, ...any)
	Auditor      *Auditor // optional
}

// Run is the periodic sweep loop. Returns when ctx is cancelled.
func (r *ActiveReclaimer) Run(ctx context.Context) {
	if r.Interval == 0 {
		r.Interval = 10 * time.Second
	}
	if r.SafetyMargin < 1.0 {
		r.SafetyMargin = 1.25
	}
	if r.Logf == nil {
		r.Logf = func(string, ...any) {}
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sweep()
		}
	}
}

func (r *ActiveReclaimer) sweep() {
	zone := r.State.ResourceSnapshot().Zone
	events := r.State.ReclaimSettled(r.SafetyMargin)
	for _, event := range events {
		res := event.After
		delta := event.Before.AllocatableNowMem - res.AllocatableNowMem
		r.Logf("reclaim sid=%s zone=%s rss=%d alloc=%d → %d (-%d)",
			res.SandboxID, zone, res.LastReportedRSS, event.Before.AllocatableNowMem, res.AllocatableNowMem, delta)
		if r.Auditor != nil {
			r.Auditor.Logf("reclaim sid=%s zone=%s rss=%d alloc=%d→%d delta=%d",
				res.SandboxID, zone, res.LastReportedRSS, event.Before.AllocatableNowMem, res.AllocatableNowMem, delta)
		}
	}
}
