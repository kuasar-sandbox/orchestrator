package nodectl

import (
	"fmt"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func TestAllocatorPositiveRateEventuallyCompletesWithinEnvelope(t *testing.T) {
	const mib = uint64(1 << 20)
	for _, rate := range []uint64{53_687_091, mib, 1} {
		for _, urgency := range []string{UrgencyNormal, UrgencyLow} {
			t.Run(fmt.Sprintf("%d/%s", rate, urgency), func(t *testing.T) {
				a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: rate, MinGrantStep: 4 * mib, MaxGrantStep: 512 * mib})
				now := time.Unix(0, 0)
				start := now
				a.clock, a.lastRefill = func() time.Time { return now }, now
				if a.tokens != float64(rate) {
					t.Fatal("extra initial burst")
				}
				var total uint64
				const target = 64*mib + 1 // includes an exact sub-minimum tail
				for attempt := 0; attempt < 100 && total < target; attempt++ {
					remaining := target - total
					decision := a.Grant("grow", remaining, 2<<30, urgency)
					if decision.GrantedDelta > remaining || decision.GrantedDelta > a.bucketCapacity() || decision.GrantedDelta > a.policy.MaxGrantStep {
						t.Fatalf("unbounded grant: %+v", decision)
					}
					if decision.GrantedDelta > 0 && decision.GrantedDelta < a.policy.MinGrantStep && decision.GrantedDelta != remaining {
						t.Fatal("sub-minimum partial grant")
					}
					total += decision.GrantedDelta
					if float64(total) > float64(rate)*(1+now.Sub(start).Seconds()) {
						t.Fatal("grant exceeded initial tokens plus elapsed refill")
					}
					if decision.CooldownMs <= 0 {
						t.Fatal("invalid cooldown")
					}
					now = now.Add(time.Duration(decision.CooldownMs) * time.Millisecond)
				}
				if total != target {
					t.Fatalf("normal growth stalled: %d/%d", total, target)
				}
				// Long idle time never funds more than the bounded bucket.
				now = now.Add(time.Duration(a.bucketCapacity()/rate+1) * time.Second)
				first := a.Grant("other", 1<<30, 2<<30, urgency)
				second := a.Grant("other", 1<<30, 2<<30, urgency)
				if first.GrantedDelta+second.GrantedDelta > a.bucketCapacity() || second.GrantedDelta != 0 {
					t.Fatal("idle accumulation exceeded burst bound")
				}
			})
		}
	}
}

func TestPoolGrowRateUsesEffectiveMemoryAndWaitIsNotPressure(t *testing.T) {
	for _, multi := range []bool{false, true} {
		for _, factor := range []float64{0.05, 1.0 / 1024, 1.0 / (1 << 30)} {
			for _, urgency := range []string{UrgencyNormal, UrgencyLow} {
				t.Run(fmt.Sprintf("multi=%v/factor=%g/%s", multi, factor, urgency), func(t *testing.T) {
					memory, capacity := "1GiB", "8GiB"
					cfg := &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "small", Allocatable: config.ResourceAllocatable{Memory: &memory}, Capacity: config.ResourceAllocatable{Memory: &capacity}}}}
					if multi {
						large := "2GiB"
						cfg.Pools = append(cfg.Pools, config.ResourcePoolConfig{Name: "large", Allocatable: config.ResourceAllocatable{Memory: &large}})
					}
					cfg.RateLimits.MemoryGrantPerSecFactor = factor
					resolved, err := Resolve(cfg)
					if err != nil {
						t.Fatal(err)
					}
					state := NewPoolState(resolved)
					for _, pool := range resolved.Pools {
						otherTokens := map[string]float64{}
						for name, other := range state.PoolStates() {
							if name != pool.Name {
								otherTokens[name] = other.poolAllocator.tokens
							}
						}
						child := state.PoolStates()[pool.Name]
						allocator := child.poolAllocator
						wantRate := scaleUint64Floor(pool.Allocatable.MemoryBytes, factor)
						if allocator.policy.MemoryGrantPerSecBytes != wantRate {
							t.Fatalf("pool %s rate=%d want=%d", pool.Name, allocator.policy.MemoryGrantPerSecBytes, wantRate)
						}
						now := time.Unix(0, 0)
						allocator.clock, allocator.lastRefill = func() time.Time { return now }, now
						child.pressure.clock = func() time.Time { return now }
						if multi {
							placePoolTest(t, state, pool.Name, pool.Name, Resources{MemoryBytes: 512 << 20})
						}
						reservation, _, err := state.Admit(AdmitSpec{SandboxID: pool.Name, Token: pool.Name, Capacity: Resources{MemoryBytes: 512 << 20}, InitialBudget: 256 << 20})
						if err != nil {
							t.Fatal(err)
						}
						if _, _, err := state.SetSettled(pool.Name, 0, now); err != nil {
							t.Fatal(err)
						}
						const goal = uint64(320 << 20)
						for attempt := 0; attempt < 100 && reservation.ReservationMemory < goal; attempt++ {
							result, found, err := state.ReconcileAndGrant(pool.Name, reservation.ReservationMemory, goal-reservation.ReservationMemory, urgency, nil)
							if err != nil || !found {
								t.Fatalf("grow: %v %v", found, err)
							}
							reservation = result.Reservation
							budget, _ := executableMemoryBudget(reservation.Capacity.MemoryBytes, reservation.ReservationMemory)
							if budget > reservation.ReservationMemory || (reservation.Capacity.MemoryBytes-budget)%resource.MemoryStep != 0 {
								t.Fatal("executable Budget exceeds/alters SDK reservation alignment")
							}
							if p := child.PressureSnapshot(); p.PauseEligible || p.Zone != ZoneGreen {
								t.Fatalf("token wait became memory pressure: %+v", p)
							}
							now = now.Add(time.Duration(result.Decision.CooldownMs) * time.Millisecond)
						}
						if reservation.ReservationMemory != goal {
							t.Fatalf("pool %s stalled at %d", pool.Name, reservation.ReservationMemory)
						}
						for name, other := range state.PoolStates() {
							if name != pool.Name && other.poolAllocator.tokens != otherTokens[name] {
								t.Fatal("grow consumed another pool's tokens")
							}
						}
					}
				})
			}
		}
	}
}
