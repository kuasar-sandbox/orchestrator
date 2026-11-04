package orch

import (
	"context"
	"testing"
	"time"
)

func TestIdleWaitingDoesNotReadStore(t *testing.T) {
	o, sb, b, clock := idleFixture(t)
	seconds := int64(60)
	sb.IdleTimeout = &seconds
	o.cache(sb)
	observeIdle(o, sb, 1)
	first := o.idleCandidates[sb.ID]
	if err := o.st.Close(); err != nil {
		t.Fatal(err)
	}
	for tick := 0; tick < 100; tick++ {
		clock.Add(int64(100 * time.Millisecond))
		o.scheduleIdle(context.Background())
		if o.idleCandidates[sb.ID] != first {
			t.Fatal("waiting timer depended on database availability")
		}
	}
	select {
	case <-b.requests:
		t.Fatal("pause began before the deadline")
	default:
	}
	sb.IdleTimeout = nil
	o.cache(sb)
	o.scheduleIdle(context.Background())
	if o.idleCandidates[sb.ID] != nil {
		t.Fatal("cached policy disable retained timer")
	}
}

func TestIdleDueAttemptRechecksPersistedEligibility(t *testing.T) {
	for _, change := range []string{"policy", "resource"} {
		t.Run(change, func(t *testing.T) {
			o, sb, b, _ := idleFixture(t)
			observeIdle(o, sb, 1)
			if change == "policy" {
				sb.IdleTimeout = nil
			} else {
				sb.ResourceObligation = true
			}
			// Deliberately retain the eligible cache snapshot. The SID-locked
			// authoritative reread must still reject this attempt.
			if err := o.st.Put(context.Background(), sb); err != nil {
				t.Fatal(err)
			}
			o.scheduleIdle(context.Background())
			awaitCandidate(t, o, sb.ID, func(c *idleCandidate) bool {
				return o.idleInFlight[sb.ID] == nil
			})
			select {
			case <-b.requests:
				t.Fatal("stale cache bypassed the authoritative eligibility check")
			default:
			}
		})
	}
}
