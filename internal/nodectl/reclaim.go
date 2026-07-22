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
	State             *State
	Persister         *Persister
	PreparedAdmission *PreparedAdmissionController
	Interval          time.Duration // 10s default
	SafetyMargin      float64       // 1.25 default (working set + 25%)
	Logf              func(string, ...any)
	Auditor           *Auditor // optional
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
	r.State.Lock()

	zone := r.State.MemoryZone()
	margin := r.SafetyMargin
	switch zone {
	case ZoneYellow:
		margin = 1.10
	case ZoneRed:
		margin = 1.05
	case ZoneCritical:
		margin = 1.00
	}

	previous := make(map[string]uint64)
	for token, res := range r.State.Reservations {
		if res.Stage != StageSettled {
			continue
		}
		ws := res.LastReportedRSS
		if ws == 0 {
			ws = res.Floor.MemoryBytes
		}
		target := uint64(float64(ws) * margin)
		if target < res.Floor.MemoryBytes {
			target = res.Floor.MemoryBytes
		}
		if target >= res.AllocatableNowMem {
			continue
		}
		delta := res.AllocatableNowMem - target
		r.Logf("reclaim sid=%s zone=%s rss=%d alloc=%d → %d (-%d)",
			res.SandboxID, zone, ws, res.AllocatableNowMem, target, delta)
		if r.Auditor != nil {
			r.Auditor.Logf("reclaim sid=%s zone=%s rss=%d alloc=%d→%d delta=%d",
				res.SandboxID, zone, ws, res.AllocatableNowMem, target, delta)
		}
		previous[token] = res.AllocatableNowMem
		res.AllocatableNowMem = target
	}
	if len(previous) == 0 {
		r.State.Unlock()
		return
	}
	flushErr := r.Persister.Flush(r.State)
	if flushErr != nil && !FlushPublished(flushErr) {
		for token, allocatable := range previous {
			r.State.Reservations[token].AllocatableNowMem = allocatable
		}
		r.State.Unlock()
		r.Logf("reclaim persist: %v", flushErr)
		return
	}
	r.State.Unlock()
	if flushErr != nil {
		r.Logf("reclaim persist: %v", flushErr)
	}
	if r.PreparedAdmission != nil {
		r.PreparedAdmission.SignalCapacityChange()
	}
}
