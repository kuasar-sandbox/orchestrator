package nodectl

import (
	"testing"
	"time"
)

func TestCacheReclaimAdvisoryZeroAndPartialAcknowledgment(t *testing.T) {
	const miB = uint64(1 << 20)
	s := makeState(8<<30, 0)
	s.CachePoolCapacity = 112 * miB
	a := allocatorCacheTest()
	tokA := admitCacheTest(t, s, "dense-a", 1<<30)
	tokB := admitCacheTest(t, s, "idle-b", 1<<30)
	tokC := admitCacheTest(t, s, "busy-c", 1<<30)
	if g := mustCacheGrant(t, s, tokA, 0, 64*miB, 1<<30, a); g.GrantedDelta != 64*miB {
		t.Fatal(g)
	}
	if g := mustCacheGrant(t, s, tokB, 0, 16*miB, 1<<30, a); g.GrantedDelta != 16*miB {
		t.Fatal(g)
	}
	if g := mustCacheGrant(t, s, tokC, 0, 32*miB, 1<<30, a); g.GrantedDelta != 32*miB {
		t.Fatal(g)
	}
	// C's real headroom shortage first requests reclaim from highest holder A.
	_ = mustCacheGrant(t, s, tokC, 32*miB, 32*miB, 1<<30, a)
	time.Sleep(450 * time.Millisecond)
	_ = mustCacheGrant(t, s, tokC, 32*miB, 32*miB, 1<<30, a)
	seqA := reservationByTokenForTest(t, s, tokA).CacheReclaimSeq
	targetA, _ := s.CacheTarget(tokA)
	if targetA == 0 || seqA == 0 {
		t.Fatalf("A not asked: target=%d seq=%d", targetA, seqA)
	}
	// A cannot release any safe pages: an incorrect or stale ACK is ignored.
	stale, _, err := s.ReconcileCacheBudgetWithAck(tokA, 64*miB, 0, 1<<30, seqA+100, a)
	if err != nil || stale.ReclaimTarget == 0 {
		t.Fatalf("stale acknowledgment accepted: %+v %v", stale, err)
	}
	ack, _, err := s.ReconcileCacheBudgetWithAck(tokA, 64*miB, 0, 1<<30, seqA, a)
	if err != nil || ack.ReclaimTarget != 0 || s.reservedCache != 112*miB {
		t.Fatalf("zero release should complete without credit: %+v %v allocated=%d", ack, err, s.reservedCache)
	}
	// The next retry chooses B, even though B has less cache than C's
	// current budget and A still has the largest grant.
	_ = mustCacheGrant(t, s, tokC, 32*miB, 32*miB, 1<<30, a)
	seqB := reservationByTokenForTest(t, s, tokB).CacheReclaimSeq
	targetB, _ := s.CacheTarget(tokB)
	if seqB == 0 || targetB == 0 {
		t.Fatalf("B not selected: seq=%d target=%d", seqB, targetB)
	}
	// B safely returns only 8 MiB, not the complete requested reduction.
	partial, _, err := s.ReconcileCacheBudgetWithAck(tokB, 8*miB, 0, 1<<30, seqB, a)
	if err != nil || partial.ReclaimTarget != 0 || s.reservedCache != 104*miB {
		t.Fatalf("partial completion: %+v %v cache=%d", partial, err, s.reservedCache)
	}
	grant := mustCacheGrant(t, s, tokC, 32*miB, 32*miB, 1<<30, a)
	if grant.GrantedDelta != 8*miB || s.reservedCache != 112*miB {
		t.Fatalf("returned physical pages not transferred: %+v cache=%d", grant, s.reservedCache)
	}
}

func TestUnresponsiveDonorDoesNotBlockAnotherDonor(t *testing.T) {
	for _, reason := range []string{"disconnect", "no completion"} {
		t.Run(reason, func(t *testing.T) {
			s := makeState(8<<30, 0)
			a := allocatorCacheTest()
			largest := admitCacheTest(t, s, "largest", 1<<30)
			other := admitCacheTest(t, s, "other", 1<<30)
			mustCacheGrant(t, s, largest, 0, 64*testCacheMiB, 1<<30, a)
			mustCacheGrant(t, s, other, 0, 32*testCacheMiB, 1<<30, a)
			const shortage = 8 * testCacheMiB
			if !s.requestCacheDonorLocked("waiter", shortage) {
				t.Fatal("first request did not choose a donor")
			}
			first := reservationByTokenForTest(t, s, largest)
			if first.CacheReclaimTarget != 56*testCacheMiB {
				t.Fatalf("unexpected first target %d", first.CacheReclaimTarget)
			}
			if reason == "disconnect" {
				if _, ok := s.DropConnection(largest, first.Conn); !ok {
					t.Fatal("failed to detach donor")
				}
			} else {
				mutateReservationForTest(t, s, first.SandboxID, func(r *Reservation) {
					r.cacheReclaimUpdatedAt = time.Now().Add(-cacheReclaimPlanWindow)
				})
			}
			if !s.requestCacheDonorLocked("waiter", shortage) {
				t.Fatal("unresponsive donor blocked the next request")
			}
			if reason == "no completion" {
				result := mustCacheGrant(t, s, largest, 64*testCacheMiB, 4*testCacheMiB, 1<<30, a)
				if result.ReclaimTarget != 0 || result.GrantedDelta != 4*testCacheMiB {
					t.Fatalf("expired advice still blocks legacy donor: %+v", result)
				}
				// Return this test grant so the final balance reflects the original charges.
				mustCacheGrant(t, s, largest, 64*testCacheMiB, 0, 1<<30, a)
			}
			if got, _ := s.CacheTarget(other); got != 24*testCacheMiB || s.reservedCache != 96*testCacheMiB {
				t.Fatalf("fallback target %d, reserved %d", got, s.reservedCache)
			}
		})
	}
}
