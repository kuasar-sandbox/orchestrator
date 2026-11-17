package nodectl

import (
	"fmt"
	"math"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

const cacheBudgetUnit = uint64(8 << 20)

// CacheGrantResult has an absolute controller baseline so retries after lost
// replies can reconcile without treating allocated cache as Guest memory.
type CacheGrantResult struct {
	GrantedDelta  uint64
	Reservation   uint64
	ReclaimTarget uint64
	CooldownMs    int64
}

// changeCacheLocked is the only cache-reservation aggregate mutation. It is
// serialized with Admit, Guest grow, StateSync, and Release by the same mutex.
func (s *State) changeCacheLocked(r *Reservation, current uint64) error {
	if r.CacheReservation == current {
		return nil
	}
	next := cloneReservation(r)
	next.CacheReservation = current
	if err := s.validateAggregateReplacementLocked([]*Reservation{r}, &next); err != nil {
		return err
	}
	if err := s.removeAggregatesLocked(r); err != nil {
		return err
	}
	old := r.CacheReservation
	r.CacheReservation = current
	if err := s.addAggregatesLocked(r); err != nil {
		r.CacheReservation = old
		_ = s.addAggregatesLocked(r)
		return err
	}
	return nil
}

// ReconcileCacheBudget charges only normal pool memory and never uses Guest
// emergency capacity or the Guest balloon step. current is a safe, physically
// completed cache baseline; it cannot exceed a previously granted amount.
func (s *State) ReconcileCacheBudget(token string, current, requested, maximum uint64, allocator *Allocator) (CacheGrantResult, bool, error) {
	if p := s.forToken(token); p != s {
		return p.ReconcileCacheBudget(token, current, requested, maximum, allocator)
	}
	if s.poolAllocator != nil {
		allocator = s.poolAllocator
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
	if r.CacheReclaimTarget != 0 && current <= r.CacheReclaimTarget {
		r.CacheReclaimTarget = 0
	}
	out := CacheGrantResult{Reservation: current, ReclaimTarget: r.CacheReclaimTarget, CooldownMs: 200}
	if requested == 0 {
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	remaining := maximum - current
	if requested < remaining {
		remaining = requested
	}
	if remaining == 0 || allocator == nil || r.CacheReclaimTarget != 0 {
		return out, true, nil
	}
	pool := s.AllocatablePool.MemoryBytes
	emergency := scaleUint64Floor(pool, s.Wm.EmergencyFactor)
	headroom := uint64(0)
	if pool > s.reservedMemory && pool-s.reservedMemory > emergency {
		headroom = pool - s.reservedMemory - emergency
	}
	// Guest growth and pending recovery take precedence over discretionary
	// cache grants. Reuse the existing beneficiary-protection calculation.
	protected := s.protectedLocked("cache:" + r.SandboxID)
	if protected >= headroom {
		headroom = 0
	} else {
		headroom -= protected
	}
	if s.CachePoolCapacity > 0 {
		if s.reservedCache >= s.CachePoolCapacity {
			headroom = 0
		} else {
			headroom = min(headroom, s.CachePoolCapacity-s.reservedCache)
		}
	}
	grant := allocator.GrantUnits(token, remaining, headroom, cacheBudgetUnit)
	out.CooldownMs = grant.CooldownMs
	if grant.GrantedDelta > 0 {
		next, err := checkedAggregateAdd(current, grant.GrantedDelta, "cache reservation")
		if err != nil || next > maximum {
			return CacheGrantResult{}, true, fmt.Errorf("cache grant overflow")
		}
		if err := s.changeCacheLocked(r, next); err != nil {
			return CacheGrantResult{}, true, err
		}
		out.GrantedDelta, out.Reservation = grant.GrantedDelta, next
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	// A token-bucket delay is not a node memory shortage. Start active
	// reclaim only after a real shortage persists across retries.
	if headroom >= min(remaining, cacheBudgetUnit) {
		delete(s.cacheDemandSince, token)
		return out, true, nil
	}
	if s.cacheDemandSince == nil {
		s.cacheDemandSince = make(map[string]cacheDemandAge)
	}
	now := time.Now()
	demand, found := s.cacheDemandSince[token]
	// A burst that ended before a later request must not inherit the old
	// wait age and immediately evict another active sandbox's cache.
	if !found || now.Sub(demand.last) > 2*time.Second {
		s.cacheDemandSince[token] = cacheDemandAge{since: now, last: now}
		return out, true, nil
	}
	demand.last = now
	s.cacheDemandSince[token] = demand
	if now.Sub(demand.since) < 400*time.Millisecond {
		return out, true, nil
	}
	// Persistent failed requests may borrow from another sandbox. A target
	// remains charged until its owner has actually returned physical pages.
	s.requestCacheDonorLocked(r.SandboxID, r.CacheReservation, remaining, false)
	return out, true, nil
}

// requestCacheDonorLocked assigns a target only; it never credits bytes to
// a waiting sandbox or decrements the donor's existing reservation. Queued
// admission is prioritized over optional cache growth, so it need not have
// a smaller existing cache reservation than the donor.
func (s *State) requestCacheDonorLocked(excludeSID string, borrower, shortage uint64, admission bool) bool {
	if shortage == 0 {
		return false
	}
	// Pending reclaim targets remain charged. They will release ordinary
	// memory and cache-subpool capacity when completed, so avoid requesting
	// the same shortage from every other donor during repeated retries.
	// This is planning credit only; admission and grants still use current
	// reservations and never spend unacknowledged bytes.
	for _, candidate := range s.bySID {
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
		if candidate.SandboxID == excludeSID || candidate.Provisional ||
			candidate.CacheReclaimTarget != 0 || candidate.CacheReservation <= cacheBudgetUnit {
			continue
		}
		if !admission && candidate.CacheReservation <= borrower+cacheBudgetUnit {
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
	// Active cache reclaims whole accounting units, except a final
	// configured sub-unit tail that can be reclaimed as one ordinary unit.
	if drop < cacheBudgetUnit {
		drop = min(cacheBudgetUnit, available)
	} else {
		drop -= drop % cacheBudgetUnit
	}
	if drop == 0 {
		return false
	}
	donor.CacheReclaimTarget = donor.CacheReservation - drop
	return true
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
