package orch

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
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
func (o *Orchestrator) routeEntry(sb *types.Sandbox) routesync.RouteEntry {
	e := routesync.RouteEntry{
		SandboxID:          sb.ID,
		Profile:            string(sb.Profile()),
		TemplateID:         sb.TemplateID,
		State:              string(sb.State),
		EnvdUDS:            sb.EnvdUDS,
		CiUDS:              sb.CiUDS,
		FloatingIP:         sb.FloatingIP,
		AccessToken:        sb.EnvdAccessToken,
		TrafficAccessToken: sb.TrafficAccessToken,
		SnapshotLocation:   snapshotLocation(sb.SnapshotRef),
		MmdsSecret:         hex.EncodeToString(keys.MmdsSecret(sb.ManifestKey, sb.ID)),
	}
	if managed, nodeID, nodeEpoch, registryGeneration, digest, err := sandboxRouteFence(sb); managed {
		if err != nil {
			// A malformed system-owned Binding must fail closed at every proxy.
			e.BindingDigest = "invalid"
		} else {
			e.NodeID = nodeID
			e.NodeEpoch = nodeEpoch
			e.RegistryGeneration = registryGeneration
			e.BindingDigest = digest
		}
	}
	return e
}

// routeEntryForSync attaches the durable event watermark to a cluster-managed
// route. The object supplies node-local forwarding details, while the workflow
// event supplies the state and capabilities that were persisted atomically with
// event_seq. Standalone routes have no workflow and keep their original shape.
func (o *Orchestrator) routeEntryForSync(ctx context.Context, sb *types.Sandbox) (routesync.RouteEntry, error) {
	entry := o.routeEntry(sb)
	managed, _, _, _, _, bindingErr := sandboxRouteFence(sb)
	if bindingErr != nil {
		return routesync.RouteEntry{}, bindingErr
	}
	if !managed {
		return entry, nil
	}
	if o.st == nil {
		return routesync.RouteEntry{}, fmt.Errorf("orch: managed route %q has no durable store", sb.ID)
	}
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		return routesync.RouteEntry{}, err
	}
	if current == nil {
		return routesync.RouteEntry{}, fmt.Errorf("orch: managed route %q has no Sandbox object", sb.ID)
	}
	entry = o.routeEntry(current)
	workflow, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sb.ID)
	if err != nil {
		return routesync.RouteEntry{}, err
	}
	if workflow == nil || workflow.LatestEvent == nil || workflow.EventSeq == 0 {
		return routesync.RouteEntry{}, fmt.Errorf("orch: managed route %q has no durable execution event", sb.ID)
	}
	event := workflow.LatestEvent
	if err := event.Validate(); err != nil {
		return routesync.RouteEntry{}, err
	}
	if workflow.Kind != clusterstate.ExecutionKindSandbox || workflow.ObjectID != sb.ID ||
		workflow.EventSeq != event.EventSeq || workflow.BindingDigest != entry.BindingDigest ||
		event.ObjectID != sb.ID || event.NodeID != entry.NodeID || event.NodeEpoch != entry.NodeEpoch ||
		event.RegistryGeneration != entry.RegistryGeneration || event.Binding != workflow.OpaqueBinding ||
		event.BindingDigest != entry.BindingDigest {
		return routesync.RouteEntry{}, fmt.Errorf("orch: managed route %q disagrees with its durable execution event", sb.ID)
	}
	state, err := routeStateFromExecutionEvent(event.State)
	if err != nil {
		return routesync.RouteEntry{}, err
	}
	if string(current.State) != state || event.TemplateRef != current.TemplateID ||
		event.AccessToken != current.EnvdAccessToken || event.TrafficAccessToken != current.TrafficAccessToken {
		return routesync.RouteEntry{}, fmt.Errorf("orch: managed route %q object projection is not event-atomic", sb.ID)
	}
	entry.EventSeq = event.EventSeq
	entry.State = state
	entry.TemplateID = event.TemplateRef
	entry.AccessToken = event.AccessToken
	entry.TrafficAccessToken = event.TrafficAccessToken
	entry.SnapshotLocation = event.SnapshotLocation
	return entry, nil
}

func routeStateFromExecutionEvent(state string) (string, error) {
	switch state {
	case string(clusterstate.WorkflowRouteReady):
		return routesync.StateRunning, nil
	case string(clusterstate.WorkflowRoutePaused):
		return routesync.StatePaused, nil
	default:
		return "", fmt.Errorf("orch: terminal execution state %q cannot be projected as a route", state)
	}
}

func (o *Orchestrator) routeDeleteForSync(ctx context.Context, sid string) (routesync.RouteDelete, error) {
	delete := routesync.RouteDelete{SandboxID: sid}
	if o.st == nil {
		return delete, nil
	}
	workflow, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sid)
	if err != nil {
		return routesync.RouteDelete{}, err
	}
	if workflow == nil {
		return delete, nil
	}
	if workflow.LatestEvent == nil || workflow.EventSeq == 0 {
		return routesync.RouteDelete{}, fmt.Errorf("orch: managed route %q has no durable terminal event", sid)
	}
	event := workflow.LatestEvent
	if err := event.Validate(); err != nil {
		return routesync.RouteDelete{}, err
	}
	if event.State != "ERROR" && event.State != "DELETED" {
		return routesync.RouteDelete{}, fmt.Errorf("orch: managed route %q is not terminal", sid)
	}
	if workflow.ObjectID != sid || workflow.EventSeq != event.EventSeq ||
		workflow.BindingDigest != event.BindingDigest || workflow.OpaqueBinding != event.Binding {
		return routesync.RouteDelete{}, fmt.Errorf("orch: managed route %q terminal event is inconsistent", sid)
	}
	delete.NodeID = event.NodeID
	delete.NodeEpoch = event.NodeEpoch
	delete.RegistryGeneration = event.RegistryGeneration
	delete.BindingDigest = event.BindingDigest
	delete.EventSeq = event.EventSeq
	return delete, delete.Validate()
}

// snapshotLocation classifies a sandbox's persisted state so a subscriber can
// decide migration: "" when never paused (running/dead), "remote" for an uploaded
// (portable) manifest:// ref, else "local" (a node-bound checkpoint bundle).
func snapshotLocation(ref string) string {
	switch {
	case ref == "":
		return ""
	case strings.HasPrefix(ref, "manifest://"):
		return "remote"
	default:
		return "local"
	}
}

// --- routesync.Source ---

// Range streams the full current route set (running + paused) one entry at a time,
// in id order within each state. Streaming (vs materializing a slice) keeps
// send-side memory bounded at high sandbox density — see store.RangeByState.
func (o *Orchestrator) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	for _, st := range []types.State{types.StateRunning, types.StatePaused} {
		if err := o.st.RangeByState(ctx, st, func(sb *types.Sandbox) error {
			entry, err := o.routeEntryForSync(ctx, sb)
			if err != nil {
				return err
			}
			return fn(entry)
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
func (o *Orchestrator) OnWake(ctx context.Context, wake routesync.RouteWake) {
	sid := wake.SandboxID
	request := proxy.RouteRequest{
		SandboxID:                  sid,
		ExpectedNodeID:             wake.NodeID,
		ExpectedNodeEpoch:          wake.NodeEpoch,
		ExpectedRegistryGeneration: wake.RegistryGeneration,
		ExpectedBindingDigest:      wake.BindingDigest,
	}
	sb := o.lookup(sid)
	if sb == nil {
		sb, _ = o.st.Get(ctx, sid)
	}
	if sb == nil {
		o.publishDelete(sid)
		return
	}
	if _, failed := validateSandboxRouteFence(sb, request); failed {
		o.publishUpsert(sb)
		return
	}
	switch sb.State {
	case types.StateRunning:
		o.publishUpsert(sb) // already up; re-announce so the proxy unparks
	case types.StatePaused:
		if err := o.sf.Do(sid, func() error {
			return o.lifecycle.Do(sid, func() error { return o.resumeIfPaused(ctx, sid, request) })
		}); err != nil {
			o.log.Warn("wake resume failed", "sid", sid, "err", err)
			// stays paused; the proxy's park times out -> 404.
		}
		// resume() publishes the running upsert on success.
	default: // dead
		o.publishDelete(sid)
	}
}

// Policy is the operational policy pushed to proxies at handshake.
func (o *Orchestrator) Policy() routesync.Policy {
	return routesync.Policy{
		Domain:            o.cfg.API.Domain,
		AuthMode:          o.cfg.Proxy.Auth,
		ParkTimeoutMS:     int(o.cfg.ParkTimeoutDur() / time.Millisecond),
		RequireRouterMTLS: o.cfg.Cluster.NodeLink.Endpoint != "",
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
	if err := o.publishUpsertContext(context.Background(), sb); err != nil && o.log != nil {
		o.log.Error("route projection rejected", "sandbox", sb.ID, "err", err)
	}
}

func (o *Orchestrator) publishUpsertContext(ctx context.Context, sb *types.Sandbox) error {
	entry, err := o.routeEntryForSync(ctx, sb)
	if err != nil {
		return err
	}
	o.publish(routesync.Event{Kind: routesync.TypeUpsert, Route: entry})
	return nil
}

func (o *Orchestrator) publishDelete(sid string) {
	if err := o.publishDeleteContext(context.Background(), sid); err != nil && o.log != nil {
		o.log.Error("route delete projection rejected", "sandbox", sid, "err", err)
	}
}

func (o *Orchestrator) publishDeleteContext(ctx context.Context, sid string) error {
	delete, err := o.routeDeleteForSync(ctx, sid)
	if err != nil {
		return err
	}
	o.publish(routesync.Event{Kind: routesync.TypeDelete, Delete: delete})
	return nil
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
