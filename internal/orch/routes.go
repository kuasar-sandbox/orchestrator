package orch

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const routeLogLimit = 4096

type routeLogEntry struct {
	seq int64
	ev  routesync.Event
}

// This file makes the orchestrator the routesync.Source: it streams the route set,
// publishes route changes to subscribed route-sync clients (one per external
// proxy), and resumes a sandbox when a proxy wakes it. The orchestrator is the
// single route authority — proxies are caches.

// routeEntry projects a sandbox into the wire route entry pushed to proxies. The
// MMDS secret is derived deterministically (so every proxy worker agrees) and is
// carried on every entry — subscribers that don't serve MMDS simply ignore it.
// MMDSRoutes goes through admittedMMDSMetadata rather than reading
// sb.Metadata directly: this fires both on ordinary publish and on Range's
// full snapshot to a (re)connecting external proxy, which reads straight from
// the store, so a specification that predates a restart/config change (mmds
// routes now disabled, reserved prefixes since tightened) must not reach a
// proxy just because it was admitted under an older policy.
func (o *Orchestrator) routeEntry(sb *types.Sandbox) routesync.RouteEntry {
	return o.routeEntryWithMMDS(sb, o.admittedMMDSMetadata(sb))
}

// routeEntryWithMMDS is routeEntry with the MMDSRoutes field supplied by the
// caller instead of re-derived via admittedMMDSMetadata. For a caller that
// already holds an admission-checked canonical form from earlier in the same
// call -- Create/bootCluster/runBuildUnit, immediately after their own
// ExtractMMDS call on metadata nothing has touched since; or migrate.go's
// import, which deliberately bypasses admittedMMDSMetadata's
// MMDSRuntimeAvailable() gate to keep a carried-forward value dormant rather
// than admitted -- going through routeEntry would pay a second full
// ExtractMMDS pass (JSON decode plus policy validation) for an identical
// result.
func (o *Orchestrator) routeEntryWithMMDS(sb *types.Sandbox, mmdsRoutes string) routesync.RouteEntry {
	apiSecretFingerprint, _ := store.APISecretHash(sb.APISecret)
	manifestKeyFingerprint, _ := store.ManifestKeyHash(sb.ManifestKey)
	e := routesync.RouteEntry{
		SandboxID:              sb.ID,
		Profile:                string(sb.Profile),
		TemplateID:             sb.TemplateID,
		State:                  string(sb.State),
		EnvdUDS:                sb.EnvdUDS,
		CiUDS:                  sb.CiUDS,
		FloatingIP:             sb.FloatingIP,
		AuthSandboxID:          sb.AuthSandboxID(),
		APISecret:              sb.APISecret,
		APISecretFingerprint:   apiSecretFingerprint,
		ManifestKeyFingerprint: manifestKeyFingerprint,
		ServiceSecret:          sb.ServiceSecret,
		EnvdAccessToken:        sb.EnvdAccessToken,
		TrafficAccessToken:     sb.TrafficAccessToken,
		ForwardAccessToken:     sb.ForwardAccessToken,
		SnapshotLocation:       snapshotLocation(sb.SnapshotRef),
		MmdsSecret:             hex.EncodeToString(keys.MmdsSecret(sb.ManifestKey, sb.ID)),
		MMDSRoutes:             mmdsRoutes,
	}
	return e
}

// snapshotLocation classifies a sandbox's persisted state so a subscriber can
// decide migration: "" when never paused (starting/running/dead), "remote" for an uploaded
// portable canonical ref, else "local" (a node-bound checkpoint bundle).
func snapshotLocation(ref string) string {
	switch {
	case ref == "":
		return ""
	case types.IsPortableRef(ref):
		return "remote"
	default:
		return "local"
	}
}

// --- routesync.Source ---

// Range streams the full current route set (starting + running + paused) one entry
// at a time, in id order within each state. Starting entries carry the identity
// needed by MMDS but remain non-routable until a running update. Streaming (vs
// materializing a slice) keeps send-side memory bounded at high sandbox density.
func (o *Orchestrator) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	for _, st := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused} {
		if err := o.st.RangeByState(ctx, st, func(sb *types.Sandbox) error {
			return fn(o.routeEntry(sb))
		}); err != nil {
			return err
		}
	}
	return nil
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
		if err := o.resumeSandbox(ctx, sid); err != nil {
			o.log.Warn("wake resume failed", "sid", sid, "err", err)
			// stays paused; the proxy's park times out -> 404.
		}
		// resume() publishes the running upsert on success.
	case types.StateStarting:
		// The launch already in progress will publish running or its rollback
		// state. A second Wake must not start another runner.
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

func (o *Orchestrator) SourceFingerprint() string {
	o.routeLogMu.Lock()
	defer o.routeLogMu.Unlock()
	return o.routeFP
}

func (o *Orchestrator) CurrentRevToken() string {
	o.routeLogMu.Lock()
	defer o.routeLogMu.Unlock()
	return routesync.MakeRevToken(o.routeFP, o.routeSeq)
}

func (o *Orchestrator) Replay(ctx context.Context, afterSeq int64, fn func(routesync.Event) error) error {
	o.routeLogMu.Lock()
	if len(o.routeLog) == 0 {
		if afterSeq == o.routeSeq {
			o.routeLogMu.Unlock()
			return nil
		}
		o.routeLogMu.Unlock()
		return routesync.ErrResumeUnavailable
	}
	first := o.routeLog[0].seq
	if afterSeq < first-1 || afterSeq > o.routeSeq {
		o.routeLogMu.Unlock()
		return routesync.ErrResumeUnavailable
	}
	events := make([]routesync.Event, 0, len(o.routeLog))
	for _, item := range o.routeLog {
		if item.seq > afterSeq {
			events = append(events, item.ev)
		}
	}
	o.routeLogMu.Unlock()
	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

// --- publish ---

func (o *Orchestrator) publishUpsert(sb *types.Sandbox) {
	o.publish(routesync.Event{Kind: routesync.TypeUpsert, Route: o.routeEntry(sb)})
}

func (o *Orchestrator) publishDelete(sid string) {
	o.publish(routesync.Event{Kind: routesync.TypeDelete, SID: sid})
}

// publish fans an event out to every subscriber. A full subscriber is dropped +
// closed (it reconnects and re-snapshots) rather than blocking the caller.
func (o *Orchestrator) publish(ev routesync.Event) {
	o.appendRouteLog(ev)
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

func (o *Orchestrator) appendRouteLog(ev routesync.Event) {
	o.routeLogMu.Lock()
	defer o.routeLogMu.Unlock()
	o.routeSeq++
	o.routeLog = append(o.routeLog, routeLogEntry{seq: o.routeSeq, ev: ev})
	if len(o.routeLog) > routeLogLimit {
		copy(o.routeLog, o.routeLog[len(o.routeLog)-routeLogLimit:])
		o.routeLog = o.routeLog[:routeLogLimit]
	}
}
