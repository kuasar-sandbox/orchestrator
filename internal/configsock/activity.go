package configsock

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func activitySource(p *Plugin) string { return fmt.Sprintf("%s/%d", p.ID, p.epoch) }
func (r *Registry) ValidActivitySource(source string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validActivitySourceLocked(source)
}
func (r *Registry) validActivitySourceLocked(source string) bool {
	var only *Plugin
	for _, p := range r.m {
		if p.Caps.SubscribeKind() == routesync.KindRouteWake && p.Caps.Proxy != nil {
			if only != nil {
				return false
			}
			only = p
		}
	}
	return only != nil && proxyRouteParticipantReady(only) && activitySource(only) == source
}

// Deliver while the registry lease lock is held. Receiver is memory-only and
// never calls back into Registry, so an old checked lease cannot race takeover.
func (r *Registry) deliverActivity(p *Plugin, receive func(bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[p.ID] != p {
		return
	}
	receive(r.validActivitySourceLocked(activitySource(p)))
}
