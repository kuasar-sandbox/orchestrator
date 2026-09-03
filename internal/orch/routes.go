package orch

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const routeLogLimit = 4096

type routeLogEntry struct {
	seq int64
	ev  routesync.Event
}

// This file makes the orchestrator the routesync.Source: it streams the route set,
// publishes route changes to subscribed route-sync clients (including the independent
// Proxy), and resumes a sandbox when that Proxy wakes it. The orchestrator is the
// single route authority — proxies are caches.

// routeEntry projects a sandbox into the wire route entry pushed to subscribers.
// ServeAuthority removes MMDS routes/values unless the registration is the trusted
// MMDS proxy shape enforced by config-socket.
func (o *Orchestrator) routeEntry(sb *types.Sandbox) routesync.RouteEntry {
	entry, err := o.routeEntryContext(context.Background(), sb)
	if err != nil && o.log != nil {
		o.log.Warn("route projection unavailable", "sid", sb.ID, "err", err)
	}
	return entry
}

func (o *Orchestrator) routeEntryContext(ctx context.Context, sb *types.Sandbox) (routesync.RouteEntry, error) {
	e, trafficErr := routeEntryBase(sb)
	routes, values, err := o.mmdsRouteProjection(ctx, sb)
	e.MMDSRoutes = routes
	if values != nil {
		projected := routesync.MMDSRouteSecretValues(values)
		e.MMDSRouteSecretValues = &projected
	}
	return e, errors.Join(trafficErr, err)
}

func routeEntryBase(sb *types.Sandbox) (routesync.RouteEntry, error) {
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
		StableID:               sb.StableID(),
		APISecret:              sb.APISecret,
		APISecretFingerprint:   apiSecretFingerprint,
		ManifestKeyFingerprint: manifestKeyFingerprint,
		ServiceSecret:          sb.ServiceSecret,
		EnvdAccessToken:        sb.EnvdAccessToken,
		TrafficAccessToken:     sb.TrafficAccessToken,
		ForwardAccessToken:     sb.ForwardAccessToken,
		ArtifactLocation:       artifactLocation(sb.ResumeSource.Ref),
		MmdsSecret:             hex.EncodeToString(keys.MmdsSecret(sb.ManifestKey, sb.ID)),
		RunID:                  sb.RunID,
	}
	if raw, present := sb.Metadata[sandboxcfg.NsTraffic]; present {
		patch, err := sandboxcfg.ParseTrafficPatch(raw)
		if err != nil {
			// Persisted metadata is validated before insertion. A corrupted row
			// must nevertheless fail closed instead of becoming unlimited.
			e.State = routesync.StateDead
			return e, fmt.Errorf("project traffic metadata for %s: %w", sb.ID, err)
		}
		e.MaxInflightPatch = patch.MaxInflight
	}
	return e, nil
}

// artifactLocation classifies a retained ResumeSource independently of state
// and source kind: "" means no owned artifact, "remote" is a portable
// canonical ref, and "local" is a node-bound capture artifact.
func artifactLocation(ref string) string {
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

// Range streams the full current sandbox route set (starting + running + paused)
// and active synthetic builder routes one entry at a time. Durable rows remain
// in id order within each state. Starting entries carry the identity needed by
// MMDS but remain non-routable until a running update. Streaming the durable
// majority (rather than materializing it) keeps send-side memory bounded at high
// sandbox density.
func (o *Orchestrator) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	for _, st := range []types.State{types.StateStarting, types.StateRunning, types.StatePaused} {
		if err := o.st.RangeByState(ctx, st, func(sb *types.Sandbox) error {
			entry, err := o.routeEntryContext(ctx, sb)
			if err != nil {
				// Preserve the route lifecycle while making secret routes fail
				// closed through a nil values projection.
				if o.log != nil {
					o.log.Warn("MMDS route projection unavailable", "sid", sb.ID, "err", err)
				}
			}
			return fn(entry)
		}); err != nil {
			return err
		}
	}
	// Builder sandboxes are deliberately synthetic rather than durable sandbox
	// rows, but they are live MMDS principals for the duration of a build. Include
	// them in a full snapshot so a Proxy restart converges exactly like
	// the live Upsert path instead of losing builder MMDS until the build exits.
	o.mu.Lock()
	buildRows := make([]*types.Sandbox, 0, len(o.mmdsBuildOwners))
	for sandboxID := range o.mmdsBuildOwners {
		if sb := o.reg[sandboxID]; sb != nil {
			buildRows = append(buildRows, cloneSandbox(sb))
		}
	}
	o.mu.Unlock()
	sort.Slice(buildRows, func(i, j int) bool { return buildRows[i].ID < buildRows[j].ID })
	for _, sb := range buildRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := o.routeEntryContext(ctx, sb)
		if err != nil && o.log != nil {
			o.log.Warn("MMDS builder route projection unavailable", "sid", sb.ID, "err", err)
		}
		if err := fn(entry); err != nil {
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

// OnWake handles a proxy's Wake: a paused sandbox accepts the common launch
// attempt so the resulting Upsert unparks the held request; for unknown/dead,
// push Delete so the proxy returns 404 instead of timing out.
func (o *Orchestrator) OnWake(ctx context.Context, sid string) {
	// Read and publish stable states under the same per-SID lifecycle fence used
	// by Kill and launch terminal commits. Otherwise a stale cached starting
	// entry could be re-announced after a concurrent Delete and strand a worker
	// until its full park timeout.
	unlock := o.lifecycle.Lock(sid)
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		unlock()
		o.log.Warn("wake lookup failed", "sid", sid, "err", err)
		return
	}
	if sb == nil {
		o.uncache(sid)
		o.publishDelete(sid)
		unlock()
		return
	}
	switch sb.State {
	case types.StateRunning:
		o.cache(sb)
		o.publishUpsert(sb) // already up; re-announce so the proxy unparks
		unlock()
	case types.StatePaused:
		// ensureResumeAccepted performs its own authoritative re-read under this
		// lifecycle lock, so release it before entering the common admission.
		unlock()
		request := types.ResumeRequest{Trigger: types.ResumeTriggerWake, Mode: types.ResumeAuto}
		if _, _, err := o.ensureResumeAcceptedFrom(ctx, sid, nil, request, conductorextension.SandboxOriginProxy, nil); err != nil {
			o.log.Warn("wake resume failed", "sid", sid, "err", err)
		}
	case types.StateStarting:
		// Never claim or launch here. Re-announcing the authoritative starting
		// entry closes a route-propagation race for a worker that woke from a
		// previously missing view.
		o.cache(sb)
		o.publishUpsert(sb)
		unlock()
	case types.StateDeleting:
		// Delete acceptance already removed the local cache. Do not publish the
		// terminal route event until the durable finalizer has fenced and removed
		// every exact local owner and hard-deleted the row.
		o.uncache(sid)
		unlock()
	default: // dead
		o.uncache(sid)
		o.publishDelete(sid)
		unlock()
	}
}

// Policy is the operational policy pushed to proxies at handshake.
func (o *Orchestrator) Policy() routesync.Policy {
	services := o.cfg.MMDS.ServiceEndpoints()
	return routesync.Policy{
		Domain:        o.cfg.API.Domain,
		AuthMode:      o.cfg.Proxy.Auth,
		ParkTimeoutMS: int(o.cfg.ParkTimeoutDur() / time.Millisecond),
		MMDS: &routesync.MMDSProxyPolicy{
			Enabled:  o.cfg.MMDS.Enabled,
			Listen:   o.cfg.MMDS.Listen,
			Services: services,
		},
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

// publishRouteBarrier fans out an ephemeral ordered fence without adding it to
// the durable route changelog. It shares the exact subscriber channels used by
// publishUpsert, so each stream observes Upsert before its barrier.
func (o *Orchestrator) publishRouteBarrier(barrierID string) {
	o.fanout(routesync.Event{Kind: routesync.TypeRouteBarrier, BarrierID: barrierID})
}

// publish fans an event out to every subscriber. A full subscriber is dropped +
// closed (it reconnects and re-snapshots) rather than blocking the caller.
func (o *Orchestrator) publish(ev routesync.Event) {
	o.appendRouteLog(ev)
	o.fanout(ev)
}

func (o *Orchestrator) fanout(ev routesync.Event) {
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
