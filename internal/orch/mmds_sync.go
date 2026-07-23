package orch

import (
	"context"
	"strconv"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// This file makes the orchestrator the routesync.MmdsSource: it streams the
// current MMDS endpoint set (with cleartext secret values, on this
// authenticated local sync stream only) and publishes live
// changes to subscribed external proxy masters — parallel to routes.go's
// implementation of routesync.Source for the route family, but on its own
// independent generation counter and subscriber set. The two sync streams
// share no state.

// mmdsWireEntry projects a store row into the wire entry pushed to
// subscribers.
func mmdsWireEntry(e store.MMDSEndpointFull) routesync.MmdsEndpointEntry {
	return routesync.MmdsEndpointEntry{
		SandboxID:        e.SandboxID,
		Name:             e.Name,
		Path:             e.Path,
		BackendType:      e.BackendType,
		PublicConfigJSON: e.PublicConfigJSON,
		Revision:         e.Revision,
		ValuePresent:     e.ValuePresent,
		ContentType:      e.ContentType,
		ExpiresUnix:      e.ExpiresUnix,
		SecretPlaintext:  e.SecretPlaintext,
	}
}

// --- routesync.MmdsSource ---

// MmdsGeneration returns a fresh generation stamp for the next RangeMmds
// full scan — a plain monotonically-increasing counter, independent of the
// route family's routeSeq/routeFP — the two sync streams must never conflate
// their generation counters.
func (o *Orchestrator) MmdsGeneration() string {
	o.mmdsGenMu.Lock()
	defer o.mmdsGenMu.Unlock()
	o.mmdsGenSeq++
	return strconv.FormatInt(o.mmdsGenSeq, 10)
}

// RangeMmds streams every currently-declared endpoint across every sandbox,
// each carrying its current secret plaintext (if configured), through fn.
// A disabled node policy (mmds.endpoints.enabled=false) streams nothing,
// regardless of what sandbox_mmds_endpoints still holds from before the
// feature was disabled — a subscriber's own local config (e.g. a proxy
// master inferring "enabled" only from whether mmds_listen is set) must
// never be the thing that decides whether historical ciphertext gets synced
// into external process memory; the conductor's own flag is authoritative
// and always wins.
func (o *Orchestrator) RangeMmds(ctx context.Context, fn func(routesync.MmdsEndpointEntry) error) error {
	if !o.cfg.MMDS.Endpoints.Enabled {
		return nil
	}
	return o.st.RangeMMDSEndpoints(ctx, func(e store.MMDSEndpointFull) error {
		return fn(mmdsWireEntry(e))
	})
}

// SubscribeMmds registers a live MMDS-endpoint-change listener, parallel to
// Subscribe() for the route family (same lagged-subscriber drop policy: a
// full channel is closed rather than blocking the publisher). Disabled
// (mmds.endpoints.enabled=false) returns a channel that is never written to
// — see RangeMmds; Create/admin mutations are already unreachable when
// disabled, so nothing would publish to it in practice, but the guard is
// kept here too rather than relying solely on those upstream gates.
func (o *Orchestrator) SubscribeMmds() (<-chan routesync.MmdsEvent, func()) {
	if !o.cfg.MMDS.Endpoints.Enabled {
		return make(chan routesync.MmdsEvent), func() {}
	}
	ch := make(chan routesync.MmdsEvent, 1024)
	o.mmdsSubsMu.Lock()
	id := o.mmdsSubSeq
	o.mmdsSubSeq++
	o.mmdsSubs[id] = ch
	o.mmdsSubsMu.Unlock()
	cancel := func() {
		o.mmdsSubsMu.Lock()
		if c, ok := o.mmdsSubs[id]; ok {
			delete(o.mmdsSubs, id)
			close(c)
		}
		o.mmdsSubsMu.Unlock()
	}
	return ch, cancel
}

// --- publish ---

// publishMmdsEntry re-reads (sid,name)'s complete current state and
// publishes it as a live upsert — called after every successful admin
// mutation (Set/ClearMMDSStoreValue, Set/ClearMMDSRelayAuth) and after
// Create declares new endpoints, so external proxy masters observe the
// change without waiting for their next full scan. A read error or a
// concurrently-deleted row is logged and dropped rather than propagated:
// publishing is best-effort next to the mutation that already succeeded.
func (o *Orchestrator) publishMmdsEntry(ctx context.Context, sid, name string) {
	e, found, err := o.st.GetMMDSEndpointFull(ctx, sid, name)
	if err != nil {
		o.log.Warn("mmds sync: read endpoint for publish", "sid", sid, "name", name, "err", err)
		return
	}
	if !found {
		return
	}
	o.publishMmds(routesync.MmdsEvent{Kind: routesync.TypeMmdsUpsert, Entry: mmdsWireEntry(e)})
}

// publishMmdsDelete announces one endpoint's removal (a sandbox delete
// cascades every one of its endpoints — see Kill in orch.go, which calls
// this once per endpoint before the cascading store delete).
func (o *Orchestrator) publishMmdsDelete(sid, name string) {
	o.publishMmds(routesync.MmdsEvent{Kind: routesync.TypeMmdsDelete, Key: routesync.MmdsEndpointKey{SandboxID: sid, Name: name}})
}

func (o *Orchestrator) publishMmds(ev routesync.MmdsEvent) {
	o.mmdsSubsMu.Lock()
	defer o.mmdsSubsMu.Unlock()
	for id, ch := range o.mmdsSubs {
		select {
		case ch <- ev:
		default:
			delete(o.mmdsSubs, id)
			close(ch)
			o.log.Warn("mmds sync subscriber lagged; dropped (will reconnect)", "sub", id)
		}
	}
}
