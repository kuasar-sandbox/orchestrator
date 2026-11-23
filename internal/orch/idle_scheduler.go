package orch

import (
	"context"
	"errors"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type idleRetry struct {
	observation routesync.Activity
	source      string
	failures    uint
	until       int64
	busy        bool
}

func sameIdle(a, b routesync.Activity) bool {
	return a.RunID == b.RunID && a.Generation == b.Generation && a.SinceBootNS == b.SinceBootNS
}

type idleCandidate struct {
	observation               routesync.Activity
	source                    string
	generation                uint64
	t0                        idleclock.Point
	deadline                  int64
	retryAt                   int64
	failures                  uint
	armed, deferred, inflight bool
}

// OnProxyActivity runs on the control reader: memory-only, never lifecycle I/O.
func (o *Orchestrator) OnProxyActivity(_ context.Context, source string, a *routesync.Activity) {
	now := o.idleTime()
	// Cache entries are immutable and published before their route. Hold this
	// short memory lock through admission so an old RunID cannot race a new run.
	if a != nil {
		o.mu.Lock()
		defer o.mu.Unlock()
		current := o.reg[a.SandboxID]
		if current == nil || a.RunID == "" || current.RunID != a.RunID {
			return
		}
		// v11 proxies report physical flows. A retained hold can survive a
		// proxy downgrade; use the authority even between route publications.
		// Preserve missing physical proof as unknown. A later refresh after a
		// release gets a distinct since value and starts a full timeout.
		if a.State == "idle" && a.SinceBootNS > 0 {
			h := o.projectHold(current)
			normalized := *a
			switch {
			case !h.Valid():
				normalized.State = "unknown"
			case h.Value > 0:
				normalized.State = "busy"
			default:
				normalized.SinceBootNS = max(normalized.SinceBootNS, h.IdleSinceBootNS)
			}
			a = &normalized
		}
	}
	o.idleMu.Lock()
	defer o.idleMu.Unlock()
	if o.idleCandidates == nil {
		o.idleCandidates = make(map[string]*idleCandidate)
	}
	if a == nil {
		for sid, retry := range o.idleRetries {
			if source == "" || retry.source == source {
				delete(o.idleRetries, sid)
			}
		}
		for sid, c := range o.idleCandidates {
			if source == "" || c.source == source {
				delete(o.idleCandidates, sid)
			}
		}
		return
	}
	if a.State != "idle" || a.RunID == "" || a.Generation <= 0 || a.SinceBootNS <= 0 || a.SinceBootNS > now.BootNS {
		if old := o.idleCandidates[a.SandboxID]; old != nil && old.source == source {
			delete(o.idleCandidates, a.SandboxID)
		}
		return
	}
	old := o.idleCandidates[a.SandboxID]
	if old != nil && old.source == source && old.observation.RunID == a.RunID && old.observation.Generation == a.Generation && old.observation.SinceBootNS > a.SinceBootNS {
		return
	}
	if old != nil && old.source == source && old.observation.RunID == a.RunID && old.observation.Generation == a.Generation && old.observation.SinceBootNS == a.SinceBootNS {
		return
	}
	o.idleGeneration++
	candidate := &idleCandidate{observation: *a, source: source, generation: o.idleGeneration, t0: now}
	if retry, ok := o.idleRetries[a.SandboxID]; ok && retry.source == source && sameIdle(retry.observation, *a) {
		if retry.busy {
			return
		}
		candidate.failures = retry.failures
		candidate.retryAt = retry.until
	}
	o.idleCandidates[a.SandboxID] = candidate
}
func (o *Orchestrator) validIdleSource(source string) bool {
	provider, ok := o.pauseBarriers.(interface{ ValidActivitySource(string) bool })
	return ok && provider.ValidActivitySource(source)
}

// RunIdleScheduler owns no lifecycle lock while waiting. All state is rebuilt
// from the current registration after restart; no timers are persisted.
func (o *Orchestrator) RunIdleScheduler(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer o.OnProxyActivity(context.Background(), "", nil)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.scheduleIdle(ctx)
		}
	}
}
func (o *Orchestrator) scheduleIdle(ctx context.Context) {
	o.idleMu.Lock()
	for sid, retry := range o.idleRetries {
		if o.idleTime().BootNS-retry.until > int64(5*time.Minute) {
			delete(o.idleRetries, sid)
		}
	}
	list := make(map[string]*idleCandidate, len(o.idleCandidates))
	for sid, c := range o.idleCandidates {
		if !c.inflight && o.idleInFlight[sid] == nil {
			list[sid] = c
		}
	}
	o.idleMu.Unlock()
	for sid, c := range list {
		// Waiting uses the immutable lifecycle cache; the due attempt reads
		// authoritative state again under the SID lock before installing a gate.
		sb := o.lookup(sid)
		now := o.idleTime()
		valid := o.validIdleSource(c.source)
		o.idleMu.Lock()
		if o.idleCandidates[sid] != c || c.inflight || o.idleInFlight[sid] != nil {
			o.idleMu.Unlock()
			continue
		}
		if sb == nil || !valid || sb.RunID != c.observation.RunID || sb.State != types.StateRunning || sb.IdleTimeout == nil || types.ValidateIdleTimeout(sb.IdleTimeout) != nil {
			delete(o.idleCandidates, sid)
			o.idleMu.Unlock()
			continue
		}
		if sb.ResourceObligation {
			c.armed = false
			c.deferred = true
			o.idleMu.Unlock()
			continue
		}
		if now.BootNS < c.retryAt {
			o.idleMu.Unlock()
			continue
		}
		if !c.armed {
			if c.deferred {
				c.t0 = now
				c.deferred = false
			}
			duration := int64(time.Duration(*sb.IdleTimeout) * time.Second)
			c.deadline = c.t0.BootNS + duration
			if c.deadline < c.t0.BootNS {
				c.deadline = int64(^uint64(0) >> 1)
			}
			c.armed = true
		}
		if now.BootNS-c.t0.BootNS < int64(time.Duration(*sb.IdleTimeout)*time.Second) {
			o.idleMu.Unlock()
			continue
		}
		c.inflight = true
		if o.idleInFlight == nil {
			o.idleInFlight = make(map[string]*idleCandidate)
		}
		o.idleInFlight[sid] = c
		o.idleMu.Unlock()
		go o.pauseIdleCandidate(ctx, sid, c)
	}
}

// pauseIdleCandidate is deliberately separate from the TTL wrapper. It holds
// the same SID lock through verification, capture and commit as ordinary Pause.
func (o *Orchestrator) pauseIdleCandidate(ctx context.Context, sid string, c *idleCandidate) {
	defer func() {
		o.idleMu.Lock()
		defer o.idleMu.Unlock()
		if o.idleInFlight[sid] == c {
			delete(o.idleInFlight, sid)
		}
		c.inflight = false
	}()
	ctx, unlock, err := o.lockLifecycleMutation(ctx, sid)
	if err != nil {
		o.finishIdleAttempt(ctx, sid, c, err)
		return
	}
	defer unlock()
	sb, err := o.st.Get(ctx, sid)
	o.idleMu.Lock()
	current := o.idleCandidates[sid] == c
	now := o.idleTime()
	due := now.BootNS >= c.retryAt && sb != nil && sb.IdleTimeout != nil && types.ValidateIdleTimeout(sb.IdleTimeout) == nil && now.BootNS-c.t0.BootNS >= int64(time.Duration(*sb.IdleTimeout)*time.Second)
	o.idleMu.Unlock()
	if current && !due {
		return
	}
	if err == nil && current && sb != nil && sb.RunID == c.observation.RunID && sb.State == types.StateRunning && sb.IdleTimeout != nil && types.ValidateIdleTimeout(sb.IdleTimeout) == nil && !sb.ResourceObligation && o.validIdleSource(c.source) {
		started := time.Now()
		o.log.Info("idle pause attempt", "sid", sid, "run_id", sb.RunID, "generation", c.generation,
			"observation_lag_ms", float64(c.t0.BootNS-c.observation.SinceBootNS)/1e6,
			"idle_wait_ms", float64(now.BootNS-c.t0.BootNS)/1e6, "retry", c.failures)
		defer func() {
			o.log.Info("idle pause result", "sid", sid, "generation", c.generation, "elapsed_ms", float64(time.Since(started))/1e6, "err", err)
		}()
		request := sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, IfIdle: true, IfIdleSince: cloneTime(&c.t0.Wall), IdleSinceBootNS: c.t0.BootNS}
		request.SnapshotPolicy, err = o.resolveSnapshotPolicy(sb.Metadata, sandboxcfg.SnapshotPolicy{})
		if err == nil {
			_, err = o.pauseSandboxLocked(ctx, sb, request)
		}
	} else if err == nil { // Eligibility changed while this callback waited for SID.
		o.idleMu.Lock()
		if o.idleCandidates[sid] == c {
			c.armed = false
			c.deferred = true
		}
		o.idleMu.Unlock()
		return
	}
	o.finishIdleAttempt(ctx, sid, c, err)
}
func idleBackoff(failures uint) time.Duration {
	if failures > 8 {
		failures = 8
	}
	return min(100*time.Millisecond*time.Duration(uint64(1)<<failures), 30*time.Second)
}
func (o *Orchestrator) finishIdleAttempt(ctx context.Context, sid string, c *idleCandidate, attemptErr error) {
	sb, err := o.st.Get(context.WithoutCancel(ctx), sid)
	now := o.idleTime()
	o.idleMu.Lock()
	defer o.idleMu.Unlock()
	// Record retry cost across the attempt's own pausing/unknown publication,
	// but never overwrite a newer idle edge or another source/RunID.
	current := o.idleCandidates[sid]
	same := current == nil || (current.source == c.source && sameIdle(current.observation, c.observation))
	var rejection *idlePauseRejection
	errors.As(attemptErr, &rejection)
	if attemptErr != nil && err == nil && sb != nil && sb.State == types.StateRunning && sb.RunID == c.observation.RunID && same {
		failures := c.failures + 1
		until := now.BootNS + int64(idleBackoff(failures))
		if o.idleRetries == nil {
			o.idleRetries = make(map[string]idleRetry)
		}
		o.idleRetries[sid] = idleRetry{observation: c.observation, source: c.source, failures: failures, until: until,
			busy: rejection != nil && !rejection.activity.Idle()}
		if current != nil {
			current.retryAt = until
			current.failures = failures
		}
	}

	if current != c {
		return
	}
	if attemptErr == nil || err != nil || sb == nil || sb.State != types.StateRunning || sb.RunID != c.observation.RunID {
		delete(o.idleCandidates, sid)
		delete(o.idleRetries, sid)
		return
	}
	c.armed = false
	if rejection != nil && rejection.activity.Idle() && errors.Is(attemptErr, api.ErrSandboxBusy) {
		c.t0 = rejection.received
		for _, e := range rejection.activity.Evidence {
			c.observation.SinceBootNS = max(c.observation.SinceBootNS, e.SinceBootNS)
		}
	} else if errors.Is(attemptErr, api.ErrProxyUnavailable) || (rejection != nil && !rejection.activity.Idle()) {
		delete(o.idleCandidates, sid)
	} else {
		c.t0 = now
	}

}

func (o *Orchestrator) idleTime() idleclock.Point {
	if o.idleNow != nil {
		return o.idleNow()
	}
	return idleclock.Now()
}
