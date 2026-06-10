package orch

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

// This file makes the orchestrator the routesync.Source: it snapshots the route
// set, publishes route changes to subscribed route-sync clients (one per external
// proxy), and resumes a sandbox when a proxy wakes it. The orchestrator is the
// single route authority — proxies are caches.

// routeEntry projects a sandbox into the wire route entry pushed to proxies.
func routeEntry(sb *types.Sandbox) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID:   sb.ID,
		Profile:     string(sb.Profile()),
		TemplateID:  sb.TemplateID,
		State:       string(sb.State),
		EnvdUDS:     sb.EnvdUDS,
		CiUDS:       sb.CiUDS,
		FloatingIP:  sb.FloatingIP,
		AccessToken: sb.EnvdAccessToken,
	}
}

// --- routesync.Source ---

// Snapshot returns the full current route set (running + paused).
func (o *Orchestrator) Snapshot(ctx context.Context) ([]routesync.RouteEntry, error) {
	var out []routesync.RouteEntry
	for _, st := range []types.State{types.StateRunning, types.StatePaused} {
		list, err := o.st.ListByState(ctx, st)
		if err != nil {
			return nil, err
		}
		for _, sb := range list {
			out = append(out, routeEntry(sb))
		}
	}
	return out, nil
}

// Subscribe registers a route-change listener. The orchestrator drops + closes the
// channel if a listener falls behind, prompting the routesync client to reconnect
// and re-snapshot (bounded memory, eventual consistency).
func (o *Orchestrator) Subscribe() (<-chan routesync.Event, func()) {
	ch := make(chan routesync.Event, 1024)
	o.subsMu.Lock()
	id := o.subSeq
	o.subSeq++
	o.subs[id] = ch
	o.subsMu.Unlock()
	cancel := func() {
		o.subsMu.Lock()
		if c, ok := o.subs[id]; ok {
			delete(o.subs, id)
			close(c)
		}
		o.subsMu.Unlock()
	}
	return ch, cancel
}

// OnWake handles a proxy's Wake: resume a paused sandbox (single-flight) so the
// resulting Upsert unparks the proxy's held request; for an unknown/dead sandbox,
// push a Delete so the proxy stops waiting and returns 404 instead of timing out.
func (o *Orchestrator) OnWake(ctx context.Context, sid string) {
	sb := o.lookup(sid)
	if sb == nil {
		sb, _ = o.st.Get(ctx, sid)
	}
	if sb == nil {
		o.publishDelete(sid)
		return
	}
	switch sb.State {
	case types.StateRunning:
		o.publishUpsert(sb) // already up; re-announce so the proxy unparks
	case types.StatePaused:
		if err := o.sf.Do(sid, func() error { return o.resumeIfPaused(ctx, sid) }); err != nil {
			o.log.Warn("wake resume failed", "sid", sid, "err", err)
			// stays paused; the proxy's park times out -> 503 + Retry-After.
		}
		// resume() publishes the running upsert on success.
	default: // dead
		o.publishDelete(sid)
	}
}

// Policy is the operational policy pushed to proxies at handshake.
func (o *Orchestrator) Policy() routesync.Policy {
	return routesync.Policy{
		Domain:        o.cfg.API.Domain,
		AuthMode:      o.cfg.Proxy.Auth,
		ParkTimeoutMS: int(o.cfg.ParkTimeoutDur() / time.Millisecond),
	}
}

// --- publish ---

func (o *Orchestrator) publishUpsert(sb *types.Sandbox) {
	o.publish(routesync.Event{Kind: routesync.TypeUpsert, Route: routeEntry(sb)})
}

func (o *Orchestrator) publishDelete(sid string) {
	o.publish(routesync.Event{Kind: routesync.TypeDelete, SID: sid})
}

// publish fans an event out to every subscriber. A full subscriber is dropped +
// closed (it reconnects and re-snapshots) rather than blocking the caller.
func (o *Orchestrator) publish(ev routesync.Event) {
	o.subsMu.Lock()
	defer o.subsMu.Unlock()
	for id, ch := range o.subs {
		select {
		case ch <- ev:
		default:
			delete(o.subs, id)
			close(ch)
			o.log.Warn("routesync subscriber lagged; dropped (will reconnect)", "sub", id)
		}
	}
}
