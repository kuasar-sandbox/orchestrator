package orch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type idleTestCoordinator struct{ requests chan *fakePauseBarrier }

func (b *idleTestCoordinator) ValidActivitySource(s string) bool { return s == "proxy/1" }
func (b *idleTestCoordinator) BeginProxyPauseBarrier() (routesync.PauseBarrier, error) {
	p := &fakePauseBarrier{fakeRouteBarrier: newFakeRouteBarrier(nil, nil)}
	b.requests <- p
	return p, nil
}
func idleFixture(t *testing.T) (*Orchestrator, *types.Sandbox, *idleTestCoordinator, *atomic.Int64) {
	t.Helper()
	o, sb, _, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	zero := int64(0)
	sb.IdleTimeout = &zero
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	b := &idleTestCoordinator{requests: make(chan *fakePauseBarrier, 8)}
	o.pauseBarriers = b
	clock := &atomic.Int64{}
	clock.Store(int64(time.Second))
	o.idleNow = func() idleclock.Point {
		n := clock.Load()
		return idleclock.Point{Wall: time.Unix(1700000000, n), BootNS: n}
	}
	return o, sb, b, clock
}
func observeIdle(o *Orchestrator, sb *types.Sandbox, since int64) {
	o.OnProxyActivity(context.Background(), "proxy/1", &routesync.Activity{SandboxID: sb.ID, RunID: sb.RunID, State: "idle", Generation: 1, SinceBootNS: since})
}
func awaitCandidate(t *testing.T, o *Orchestrator, sid string, predicate func(*idleCandidate) bool) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		o.idleMu.Lock()
		ok := predicate(o.idleCandidates[sid])
		o.idleMu.Unlock()
		if ok {
			return
		}
		select {
		case <-deadline:
			t.Fatal("candidate did not converge")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestIdleDuplicateAndSourceFences(t *testing.T) {
	o, sb, _, clock := idleFixture(t)
	observeIdle(o, sb, 1)
	first := o.idleCandidates[sb.ID]
	clock.Store(int64(2 * time.Second))
	observeIdle(o, sb, 1)
	if o.idleCandidates[sb.ID] != first {
		t.Fatal("duplicate extended timer")
	}
	o.OnProxyActivity(context.Background(), "old", &routesync.Activity{SandboxID: sb.ID, State: "unknown"})
	if o.idleCandidates[sb.ID] != first {
		t.Fatal("old source cleared current")
	}
	observeIdle(o, sb, 2)
	second := o.idleCandidates[sb.ID]
	if second == first || second.t0.BootNS != clock.Load() {
		t.Fatal("new idle did not rearm")
	}
	o.finishIdleAttempt(context.Background(), sb.ID, first, api.ErrSandboxBusy)
	if o.idleCandidates[sb.ID] != second {
		t.Fatal("late failure replaced candidate")
	}
	o.OnProxyActivity(context.Background(), "proxy/1", &routesync.Activity{SandboxID: sb.ID, RunID: sb.RunID, State: "busy"})
	if o.idleCandidates[sb.ID] != nil {
		t.Fatal("busy retained timer")
	}
}
func TestIdleLateSampleRearmsWithoutNewBusiness(t *testing.T) {
	o, sb, b, clock := idleFixture(t)
	observeIdle(o, sb, 1)
	o.scheduleIdle(context.Background())
	first := <-b.requests
	<-first.waiting
	clock.Store(int64(2 * time.Second))
	first.activity = routesync.PauseAck{AutomaticSafe: true, Evidence: []routesync.IdleEvidence{{SinceBootNS: int64(1500 * time.Millisecond)}}}
	first.release()
	awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool { return c != nil && !c.inflight && c.failures == 1 })
	clock.Store(int64(3 * time.Second))
	o.scheduleIdle(context.Background())
	second := <-b.requests
	<-second.waiting
	second.activity = routesync.PauseAck{AutomaticSafe: true, Evidence: []routesync.IdleEvidence{{SinceBootNS: int64(1500 * time.Millisecond)}}}
	second.release()
	awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool { return c == nil })
	got, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || got.State != types.StatePaused || got.ResourceObligation || got.ResumeSource.Kind != types.ResumeSourceSnapshot {
		t.Fatalf("pause=%+v err=%v", got, err)
	}
}
func TestIdleResourceObligationReobserves(t *testing.T) {
	o, sb, b, clock := idleFixture(t)
	sb.ResourceObligation = true
	sb.PauseReason = types.PauseReasonResource
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	observeIdle(o, sb, 1)
	o.scheduleIdle(context.Background())
	select {
	case <-b.requests:
		t.Fatal("adopted resource responsibility")
	default:
	}
	one := int64(1)
	sb.IdleTimeout = &one
	sb.ResourceObligation = false
	sb.PauseReason = ""
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	clock.Store(int64(4 * time.Second))
	o.scheduleIdle(context.Background())
	if c := o.idleCandidates[sb.ID]; c.t0.BootNS != clock.Load() {
		t.Fatal("eligibility recovery reused old timer")
	}
	select {
	case <-b.requests:
		t.Fatal("did not wait fresh timeout")
	default:
	}
}
func TestIdleFailureBackoffSurvivesOwnGate(t *testing.T) {
	o, sb, _, clock := idleFixture(t)
	observeIdle(o, sb, 1)
	first := o.idleCandidates[sb.ID]
	o.OnProxyActivity(context.Background(), "proxy/1", &routesync.Activity{SandboxID: sb.ID, RunID: sb.RunID, State: "unknown"})
	o.finishIdleAttempt(context.Background(), sb.ID, first, errors.New("capture failed"))
	observeIdle(o, sb, 1)
	c := o.idleCandidates[sb.ID]
	if c.failures != 1 || c.retryAt <= clock.Load() {
		t.Fatal("own gate erased backoff")
	}
	newer := routesync.Activity{SandboxID: sb.ID, RunID: sb.RunID, State: "idle", Generation: 1, SinceBootNS: 2}
	o.OnProxyActivity(context.Background(), "proxy/1", &newer)
	c = o.idleCandidates[sb.ID]
	o.finishIdleAttempt(context.Background(), sb.ID, first, errors.New("late capture error"))
	if o.idleCandidates[sb.ID] != c || c.failures != 0 {
		t.Fatal("late result overwrote new idle")
	}
	if idleBackoff(100) > 30*time.Second {
		t.Fatal("unbounded retry")
	}
}

func TestIdleOldRunCannotChangeNewCandidate(t *testing.T) {
	o, sb, _, clock := idleFixture(t)
	old := *sb
	sb.RunID = "new-run"
	o.cache(sb)
	clock.Store(int64(2 * time.Second))
	observeIdle(o, sb, 1)
	current := o.idleCandidates[sb.ID]
	for _, state := range []string{"idle", "busy", "unknown"} {
		o.OnProxyActivity(context.Background(), "proxy/1", &routesync.Activity{SandboxID: old.ID, RunID: old.RunID, State: state, Generation: 2, SinceBootNS: 2})
		if o.idleCandidates[sb.ID] != current {
			t.Fatalf("old %s changed new run timer", state)
		}
	}
}

func TestIdleQueuedAttemptRechecksRunAndBackoff(t *testing.T) {
	for _, change := range []string{"run", "backoff"} {
		t.Run(change, func(t *testing.T) {
			o, sb, b, clock := idleFixture(t)
			observeIdle(o, sb, 1)
			c := o.idleCandidates[sb.ID]
			c.inflight = true
			unlock := o.lifecycle.Lock(sb.ID)
			done := make(chan struct{})
			go func() { o.pauseIdleCandidate(context.Background(), sb.ID, c); close(done) }()
			if change == "run" {
				sb.RunID = "successor"
				if err := o.st.Put(context.Background(), sb); err != nil {
					t.Fatal(err)
				}
				o.cache(sb)
			} else {
				o.idleMu.Lock()
				c.retryAt = clock.Load() + int64(time.Second)
				o.idleMu.Unlock()
			}
			unlock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("queued callback did not finish")
			}
			select {
			case <-b.requests:
				t.Fatal("queued stale attempt entered Pause")
			default:
			}
		})
	}
}

// The sender is never started: only the real Worker's SID-locked sample can
// reveal this completed flow. Exercise its result through the common Pause path.
func TestIdleSynchronousWorkerRejectsHiddenShortRequest(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprint("before-t0=", before), func(t *testing.T) {
			o, sb, b, clock := idleFixture(t)
			worker := proxystats.NewWorkerStats()
			business := func() { flow := worker.BeginParking(sb.ID, proxy.ConnectServiceForward); flow.Close() }
			if before {
				business()
			}
			clock.Store(idleclock.Now().BootNS)
			observeIdle(o, sb, 1)
			if !before {
				business()
			}
			clock.Store(idleclock.Now().BootNS)
			o.scheduleIdle(context.Background())
			barrier := <-b.requests
			<-barrier.waiting
			sample, err := worker.PauseActivity(sb.ID, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			since := int64(1)
			for _, service := range sample {
				if !service.ValidTimeSample() {
					t.Fatal(service)
				}
				since = max(since, service.IdleSinceBootNS)
			}
			barrier.activity = routesync.PauseAck{AutomaticSafe: true, Activity: sample, Evidence: []routesync.IdleEvidence{{SinceBootNS: since}}}
			barrier.release()
			awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool {
				if before {
					return c == nil
				}
				return c != nil && !c.inflight && c.failures == 1
			})
			got, _ := o.st.Get(context.Background(), sb.ID)
			want := types.StateRunning
			if before {
				want = types.StatePaused
			}
			if got.State != want {
				t.Fatal("synchronous Worker threshold result", got.State, want)
			}
		})
	}
}
