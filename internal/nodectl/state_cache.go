package nodectl

import (
	"fmt"
	"math"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

const (
	cacheBudgetUnit = uint64(4 << 20)
	// Leave time for an idle sandbox to receive a request on a heartbeat.
	// After this window other donors can help, even if the owner never replies.
	cacheReclaimPlanWindow = 30 * time.Second
	cacheReclaimRetryDelay = 2 * time.Second
)

// CacheGrantResult has an absolute controller baseline so retries after lost
// replies can reconcile without treating allocated cache as Guest memory.
type CacheGrantResult struct {
	GrantedDelta    uint64
	Reservation     uint64
	ReclaimTarget   uint64
	ReclaimSeq      uint64
	CooldownMs      int64
	RecoveryBlocked bool
}

// changeCacheLocked is the only cache-reservation aggregate mutation. It is
// serialized with Admit, Guest grow, StateSync, and Release by the same mutex.
func (s *State) changeCacheLocked(r *Reservation, current uint64) error {
	if r.RetentionPrepared && current > r.CacheReservation {
		return fmt.Errorf("checkpoint memory capture freezes cache growth")
	}
	if r.CacheReservation == current {
		return nil
	}
	without, err := checkedAggregateSub(s.reservedCache, r.CacheReservation, "cache reservation")
	if err != nil {
		return err
	}
	next, err := checkedAggregateAdd(without, current, "cache reservation")
	if err != nil {
		return err
	}
	// Cache changes neither Guest watermarks nor Guest grant/protection age.
	// Admission is woken by the server after a confirmed return.
	s.reservedCache, r.CacheReservation = next, current
	return nil
}

// ReconcileCacheBudget charges the independent cache ledger only.
// It does not consume Guest memory, grant tokens or history. current is a safe, physically
// completed cache baseline; it cannot exceed a previously granted amount.
func (s *State) ReconcileCacheBudget(token string, current, requested, maximum uint64, allocator *Allocator) (CacheGrantResult, bool, error) {
	return s.ReconcileCacheBudgetWithAck(token, current, requested, maximum, 0, allocator)
}

// ReconcileCacheBudgetWithAck accepts a completed reclaim attempt even if
// nothing was safely releasable. Node requested amounts are advisory.
func (s *State) ReconcileCacheBudgetWithAck(token string, current, requested, maximum, doneSeq uint64, allocator *Allocator) (CacheGrantResult, bool, error) {
	if p := s.forToken(token); p != s {
		return p.ReconcileCacheBudgetWithAck(token, current, requested, maximum, doneSeq, allocator)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.byTokenLocked(token)
	if r == nil || r.Provisional {
		return CacheGrantResult{}, false, nil
	}
	if !resource.HasFeature(r.ClientFeatures, resource.FeatureCacheBudgetV1) {
		return CacheGrantResult{}, true, fmt.Errorf("sandbox has not negotiated cache budget")
	}
	if maximum == 0 || maximum > uint64(math.MaxInt) || current > maximum ||
		current > r.CacheReservation || current%4096 != 0 || requested%4096 != 0 {
		return CacheGrantResult{}, true, fmt.Errorf("invalid cache baseline, maximum or request")
	}
	if r.CacheMax != maximum {
		return CacheGrantResult{}, true, fmt.Errorf("cache maximum differs from the admitted run")
	}
	// A shrink is locally complete before being submitted. Even if its ACK
	// was lost, repeating the lower baseline is safe.
	if err := s.changeCacheLocked(r, current); err != nil {
		return CacheGrantResult{}, true, err
	}
	now := time.Now()
	r.expireCacheReclaim(now)
	if r.CacheReclaimTarget != 0 && (current <= r.CacheReclaimTarget ||
		doneSeq != 0 && doneSeq == r.CacheReclaimSeq) {
		r.CacheReclaimTarget = 0
		r.cacheReclaimUpdatedAt = now
	}
	out := CacheGrantResult{Reservation: current, ReclaimTarget: r.CacheReclaimTarget, CooldownMs: 200}
	if r.CacheReclaimTarget != 0 {
		out.ReclaimSeq = r.CacheReclaimSeq
	}
	if requested == 0 {
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	if s.cacheRecoveryUnknownCount > 0 {
		// Unknown recovered allocation is not a quota shortage. Continue to
		// settle confirmed returns, but do not grant or request donor eviction.
		delete(s.cacheDemandSince, token)
		out.RecoveryBlocked = true
		return out, true, nil
	}
	remaining := maximum - current
	if requested < remaining {
		remaining = requested
	}
	if remaining == 0 {
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	if r.CacheReclaimTarget != 0 {
		return out, true, nil
	}
	headroom := s.cacheHeadroomLocked()
	grant := cacheGrantBytes(remaining, headroom)
	if grant > 0 {
		next, err := checkedAggregateAdd(current, grant, "cache reservation")
		if err != nil || next > maximum {
			return CacheGrantResult{}, true, fmt.Errorf("cache grant overflow")
		}
		if err := s.changeCacheLocked(r, next); err != nil {
			return CacheGrantResult{}, true, err
		}
		out.GrantedDelta, out.Reservation = grant, next
		remaining -= grant
	}
	// Only a finite independent cache quota can cause cache shortage. An
	// internal grant batch, per-sandbox maximum or Guest pressure cannot.
	if remaining == 0 || s.CachePoolCapacity == 0 || remaining <= s.cacheHeadroomLocked() {
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	if s.cacheDemandSince == nil {
		s.cacheDemandSince = make(map[string]cacheDemandAge)
	}
	demand, found := s.cacheDemandSince[token]
	if !found || now.Sub(demand.last) > 2*time.Second {
		s.cacheDemandSince[token] = cacheDemandAge{since: now, last: now, amount: remaining}
		return out, true, nil
	}
	demand.last, demand.amount = now, remaining
	s.cacheDemandSince[token] = demand
	if now.Sub(demand.since) < 400*time.Millisecond {
		return out, true, nil
	}
	// A target remains charged until physical return is confirmed.
	s.requestCacheDonorLocked(r.SandboxID, remaining-s.cacheHeadroomLocked())
	return out, true, nil
}

// cacheHeadroomLocked does not derive a cache limit from Guest memory.
func (s *State) cacheHeadroomLocked() uint64 {
	if s.CachePoolCapacity == 0 {
		return math.MaxUint64
	}
	if s.reservedCache >= s.CachePoolCapacity {
		return 0
	}
	return s.CachePoolCapacity - s.reservedCache
}

// cacheGrantBytes keeps existing finite growth batches and physical units,
// without borrowing the Guest allocator's tokens, history or rate policy.
func cacheGrantBytes(requested, headroom uint64) uint64 {
	limit := min(requested, headroom, uint64(512<<20))
	if requested < cacheBudgetUnit {
		if limit < requested {
			return 0
		}
		return requested
	}
	return limit - limit%cacheBudgetUnit
}

// requestCacheDonorLocked assigns a target only; it never credits bytes to
// a waiting sandbox or decrements the donor's existing reservation. Queued
// admission is prioritized over optional cache growth, so it need not have
// a smaller existing cache reservation than the donor.
func (s *State) requestCacheDonorLocked(excludeSID string, shortage uint64) bool {
	if shortage == 0 {
		return false
	}
	// Pending reclaim targets remain charged. They will release independent
	// cache capacity when completed, so avoid requesting
	// the same shortage from every other donor during repeated retries.
	// This is planning credit only; admission and grants still use current
	// reservations and never spend unacknowledged bytes.
	now := time.Now()
	for _, candidate := range s.bySID {
		candidate.expireCacheReclaim(now)
		if candidate.Conn == nil {
			continue
		}
		if candidate.CacheReclaimTarget > 0 && candidate.CacheReclaimTarget < candidate.CacheReservation {
			pending := candidate.CacheReservation - candidate.CacheReclaimTarget
			if pending >= shortage {
				return false
			}
			shortage -= pending
		}
	}
	var donor *Reservation
	for _, candidate := range s.bySID {
		if candidate.SandboxID == excludeSID || candidate.Provisional || candidate.CacheCoordinated || candidate.Conn == nil ||
			candidate.CacheReclaimTarget != 0 || candidate.CacheReservation <= cacheBudgetUnit {
			continue
		}
		// A completed (possibly zero-byte) attempt must allow another donor
		// to be tried, rather than repeatedly targeting the largest holder.
		if now.Sub(candidate.cacheReclaimUpdatedAt) < cacheReclaimRetryDelay {
			continue
		}
		if donor == nil || candidate.CacheReservation > donor.CacheReservation {
			donor = candidate
		}
	}
	if donor == nil {
		return false
	}
	available := donor.CacheReservation - cacheBudgetUnit
	drop := min(shortage, available)
	// Keep the suggested amount in complete accounting units where possible.
	// The sandbox may return fewer bytes, including zero.
	if drop < cacheBudgetUnit {
		drop = min(cacheBudgetUnit, available)
	} else {
		drop -= drop % cacheBudgetUnit
	}
	if drop == 0 {
		return false
	}
	donor.CacheReclaimTarget = donor.CacheReservation - drop
	donor.cacheReclaimUpdatedAt = now
	if donor.CacheReclaimSeq == 0 {
		// Unique even after an independent node process restart.
		donor.CacheReclaimSeq = uint64(now.UnixNano())
	} else {
		donor.CacheReclaimSeq++
	}
	return true
}

// Retire an unanswered suggestion without touching the reservation. Older
// clients can report their physical balance but cannot acknowledge a shortfall.
func (r *Reservation) expireCacheReclaim(now time.Time) {
	if r.CacheReclaimTarget != 0 && now.Sub(r.cacheReclaimUpdatedAt) >= cacheReclaimPlanWindow {
		r.CacheReclaimTarget = 0
		r.cacheReclaimUpdatedAt = now
	}
}

// cachePendingReclaimSeq hides the last sequence after a completed request.
func cachePendingReclaimSeq(r Reservation) uint64 {
	if r.CacheReclaimTarget == 0 {
		return 0
	}
	return r.CacheReclaimSeq
}

// CacheReclaimTarget is the controller's latest absolute target, not a forced
// cgroup memory.max update. The sandbox decides which safe clean pages to drop.
func (s *State) CacheTarget(token string) (uint64, bool) {
	if p := s.forToken(token); p != s {
		return p.CacheTarget(token)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byTokenLocked(token); r != nil && !r.Provisional {
		return r.CacheReclaimTarget, true
	}
	return 0, false
}
