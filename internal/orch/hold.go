package orch

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// SQLite owns the value. This per-parent projection orders publications from
// old lifecycle snapshots and records only in-process continuous-zero proof.
// After restart, SQLite's explicit zero gets a new conservative baseline.
type holdProjection struct {
	instance string
	value    int64
	zero     idleclock.Point
}

func (o *Orchestrator) projectHold(sb *types.Sandbox) *routesync.HoldActivity {
	o.holdMu.Lock()
	defer o.holdMu.Unlock()
	return o.projectHoldLocked(sb)
}

func (o *Orchestrator) projectHoldLocked(sb *types.Sandbox) *routesync.HoldActivity {
	h, found := o.holds[sb.ID]
	current := found && h.instance == sb.InstanceID
	if !current {
		if sb.HoldValue == nil {
			return &routesync.HoldActivity{} // known initial zero, never interrupted idle
		}
		h = holdProjection{instance: sb.InstanceID, value: *sb.HoldValue}
	}
	if h.value < 0 {
		return nil // corrupt persisted evidence never proves idle
	}
	if h.value > 0 {
		return &routesync.HoldActivity{Value: h.value}
	}
	if h.zero.BootNS <= 0 || h.zero.Wall.IsZero() {
		if current {
			h.zero = o.idleTime()
			o.rememberHoldLocked(sb.ID, h)
		} else {
			// This authority has made no writes to this parent since startup.
			// Its persisted zero is continuous from our recovery baseline.
			// Reads of a stale SQLite range snapshot must never recreate a
			// per-parent entry after a concurrent hard delete.
			if o.holdRecovery.BootNS <= 0 || o.holdRecovery.Wall.IsZero() {
				o.holdRecovery = o.idleTime()
			}
			h.zero = o.holdRecovery
		}
		if h.zero.BootNS <= 0 || h.zero.Wall.IsZero() {
			return nil
		}
	}
	return &routesync.HoldActivity{IdleSince: &h.zero.Wall, IdleSinceBootNS: h.zero.BootNS}
}

func (o *Orchestrator) rememberHoldLocked(id string, h holdProjection) {
	if o.holds == nil {
		o.holds = make(map[string]holdProjection)
	}
	o.holds[id] = h
}

func (o *Orchestrator) GetSandboxHold(ctx context.Context, id, apiKey string) (int64, error) {
	return o.sandboxHold(ctx, id, nil, nil, hostHoldOwner(apiKey))
}

func (o *Orchestrator) SetSandboxHold(ctx context.Context, id, apiKey string, value int64, ifMatch *int64) (int64, error) {
	return o.sandboxHold(ctx, id, &value, ifMatch, hostHoldOwner(apiKey))
}

func (o *Orchestrator) GetGuestHold(ctx context.Context, id, runID, path string) (int64, error) {
	return o.sandboxHold(ctx, id, nil, nil, guestHoldOwner(runID, path))
}

func (o *Orchestrator) SetGuestHold(ctx context.Context, id, runID, path string, value int64, ifMatch *int64) (int64, error) {
	return o.sandboxHold(ctx, id, &value, ifMatch, guestHoldOwner(runID, path))
}

func hostHoldOwner(key string) func(*types.Sandbox) error {
	return func(sb *types.Sandbox) error {
		if !ownsSandbox(sb, key) {
			return api.ErrNotFound
		}
		return nil
	}
}

// runID belongs to the token actually authenticated, not to a later route lookup.
func guestHoldOwner(runID, path string) func(*types.Sandbox) error {
	return func(sb *types.Sandbox) error {
		if runID == "" || sb.RunID != runID || !holdRunning(sb) {
			return api.ErrHoldStateConflict
		}
		raw, ok := sb.Metadata[sandboxcfg.NsMMDS]
		if !ok {
			return api.ErrNotFound
		}
		routes, err := sandboxcfg.DecodePersistedMMDSRoutes(raw)
		if err != nil {
			return api.ErrProxyUnavailable
		}
		route, ok := sandboxcfg.LookupMMDSRoute(routes, path)
		if !ok || route.Type != sandboxcfg.MMDSRouteHoldTraffic {
			return api.ErrNotFound
		}
		return nil
	}
}

func holdRunning(sb *types.Sandbox) bool {
	return sb.State == types.StateRunning && sb.RunID != "" && sb.DeadRunID != sb.RunID &&
		(sb.ExecutionResult == nil || sb.ExecutionResult.RunID != sb.RunID)
}

func (o *Orchestrator) sandboxHold(ctx context.Context, id string, value, ifMatch *int64, authorize func(*types.Sandbox) error) (int64, error) {
	// Reads are a single authoritative DB snapshot; no Wake or lifecycle wait.
	// Writes share Pause's SID lock all the way through publication.
	if value != nil {
		// Hold never changes a snapshot/export source: only the SID fence is
		// needed, not the filesystem grant used by capture operations.
		unlock := o.lifecycle.Lock(id)
		defer unlock()
	}
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		return 0, err
	}
	if sb == nil {
		return 0, api.ErrNotFound
	}
	if err := authorize(sb); err != nil {
		return 0, err
	}
	if value == nil {
		if sb.HoldValue == nil {
			return 0, nil
		}
		return *sb.HoldValue, nil
	}
	if *value < 0 || (ifMatch != nil && *ifMatch < 0) {
		return 0, api.ErrBadRequest
	}
	if !holdRunning(sb) {
		return 0, api.ErrHoldStateConflict
	}
	// The session set is an admission cut, not a durable inventory. If a
	// legacy proxy joins afterwards, the authoritative count still vetoes
	// conditional Pause. Reading and releasing hold need no capable proxy.
	if *value > 0 {
		caps, ok := o.routeBarriers.(interface{ HoldTrafficReady() bool })
		if !ok || !caps.HoldTrafficReady() {
			return 0, api.ErrProxyUnavailable
		}
	}
	actual, changed, err := o.st.SetHoldValue(ctx, sb.ID, sb.InstanceID, sb.RunID, *value, ifMatch)
	switch {
	case errors.Is(err, store.ErrHoldNotMatched):
		return actual, api.ErrHoldNotMatched
	case errors.Is(err, store.ErrHoldState):
		return 0, api.ErrHoldStateConflict
	case err != nil:
		return 0, err
	case !changed:
		return actual, nil
	}
	sb.HoldValue = &actual
	h := holdProjection{instance: sb.InstanceID, value: actual}
	if actual == 0 {
		h.zero = o.idleTime()
	}
	o.holdMu.Lock()
	o.rememberHoldLocked(id, h)
	o.holdMu.Unlock()
	o.publishUpsert(sb)
	return actual, nil
}

// Physical activity/evidence has already been aggregated from every Proxy.
// Add the single authoritative hold once, regardless of the number of ACKs.
// Each existing clock proof independently certifies the historical zero point.
func withHoldActivity(ack routesync.PauseAck, h *routesync.HoldActivity) routesync.PauseAck {
	if !h.Valid() {
		ack.Error = "hold state unavailable"
		return ack
	}
	ack.Activity = maps.Clone(ack.Activity)
	if ack.Activity == nil {
		ack.Activity = make(map[string]routesync.PauseActivity)
	}
	ack.Activity["hold"] = routesync.PauseActivity{
		Connected: uint64(h.Value), Sampled: true,
		NeverActive: h.Value == 0 && h.IdleSince == nil,
		IdleSince:   h.IdleSince, IdleSinceBootNS: h.IdleSinceBootNS,
	}
	if h.IdleSince == nil || h.IdleSinceBootNS <= 0 {
		return ack
	}
	point := idleclock.Point{Wall: *h.IdleSince, BootNS: h.IdleSinceBootNS}
	ack.Evidence = slices.Clone(ack.Evidence)
	for i := range ack.Evidence {
		e := &ack.Evidence[i]
		// A logical service must never repair missing physical traffic proof.
		if e.SinceBootNS > 0 {
			e.SinceBootNS = max(e.SinceBootNS, point.BootNS)
		}
		if len(e.Points) > 0 {
			e.Points = append(slices.Clone(e.Points), point)
		}
	}
	return ack
}
