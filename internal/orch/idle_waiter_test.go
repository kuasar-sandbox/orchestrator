package orch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestIdleReplacementKeepsOnePendingAttempt(t *testing.T) {
	for _, gate := range []string{"lifecycle", "export"} {
		t.Run(gate, func(t *testing.T) {
			o, sb, b, clock := idleFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var unblock func()
			if gate == "lifecycle" {
				unblock = o.lifecycle.Lock(sb.ID)
			} else {
				attempt, ok := o.exports.Begin(sb.ID, sb.ResumeSource, true, "", nil)
				if !ok {
					t.Fatal("export gate")
				}
				unblock = func() { o.exports.Finish(attempt) }
			}
			release := sync.OnceFunc(unblock)
			defer release()
			observeIdle(o, sb, 1)
			o.scheduleIdle(ctx)
			o.idleMu.Lock()
			first := o.idleCandidates[sb.ID]
			o.idleMu.Unlock()
			for edge := int64(2); edge <= 65; edge++ {
				clock.Add(int64(time.Second))
				o.OnProxyActivity(ctx, "proxy/1", &routesync.Activity{SandboxID: sb.ID, RunID: sb.RunID, State: "busy"})
				observeIdle(o, sb, edge)
				o.scheduleIdle(ctx)
				o.idleMu.Lock()
				extra := o.idleCandidates[sb.ID].inflight
				o.idleMu.Unlock()
				if extra {
					t.Fatal("replacement spawned another blocked attempt")
				}
			}
			release()
			awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool { return c != nil && !first.inflight })
			o.scheduleIdle(ctx)
			var barrier *fakePauseBarrier
			select {
			case barrier = <-b.requests:
			case <-time.After(3 * time.Second):
				t.Fatal("latest idle was not scheduled")
			}
			<-barrier.waiting
			barrier.activity = routesync.PauseAck{AutomaticSafe: true, Evidence: []routesync.IdleEvidence{{SinceBootNS: 1}}}
			barrier.release()
			awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool { return c == nil })
		})
	}
}
