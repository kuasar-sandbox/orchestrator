package configsock

import (
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Only registrations online at this cut participate. There is deliberately no
// durable proxy inventory. A disconnect/replacement after this cut fails the
// attempt through the same exact-epoch fences as ordinary route barriers.
func (r *Registry) BeginProxyPauseBarrier() (routesync.PauseBarrier, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var participants []*Plugin
	for _, p := range r.m {
		if proxyRouteParticipantReady(p) {
			participants = append(participants, p)
		}
	}
	b, err := r.beginRouteBarrierLocked(participants, true)
	if err != nil {
		return nil, err
	}
	b.pause = true
	b.activity.Activity = make(map[string]routesync.PauseActivity)
	return b, nil
}

func (r *Registry) pauseAck(p *Plugin, id string, ack routesync.PauseAck) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.barriers[id]
	if b == nil || !b.pause || b.err != nil || r.m[p.ID] != p || !p.ready || b.required[p.epoch] != p {
		return
	}
	if _, ok := b.acked[p.epoch]; ok {
		return
	}
	for service, sample := range ack.Activity {
		total := b.activity.Activity[service]
		total.Parking += sample.Parking
		total.Connected += sample.Connected
		b.activity.Activity[service] = total
	}
	if ack.Error != "" || ack.Activity == nil {
		b.activity.Error = ack.Error
		b.failLocked(ErrProxyRouteUnavailable)
		return
	}
	b.acked[p.epoch] = struct{}{}
	if len(b.acked) == len(b.required) {
		b.signalLocked()
	}
}

func (b *proxyRouteBarrier) Activity() routesync.PauseAck {
	b.registry.mu.Lock()
	defer b.registry.mu.Unlock()
	out := routesync.PauseAck{Error: b.activity.Error, Activity: make(map[string]routesync.PauseActivity)}
	for service, sample := range b.activity.Activity {
		out.Activity[service] = sample
	}
	return out
}
