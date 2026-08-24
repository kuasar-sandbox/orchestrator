package nodectl

import (
	"testing"
	"time"
)

func TestAllocator_GrantWithinHeadroom(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 100 << 20, // 100 MiB/s
		MinGrantStep:           4 << 20,
		MaxGrantStep:           50 << 20,
	})
	// Pre-fill bucket.
	a.tokens = float64(100 << 20)

	d := a.Grant("token-a", 32<<20, 100<<20, UrgencyNormal)
	if d.GrantedDelta != 32<<20 {
		t.Errorf("granted = %d, want 32 MiB", d.GrantedDelta)
	}
}

func TestAllocator_CapByHeadroom(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30,
		MinGrantStep:           4 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = float64(1 << 30)

	d := a.Grant("token-b", 200<<20, 50<<20, UrgencyNormal)
	if d.GrantedDelta != 50<<20 {
		t.Errorf("granted = %d, want 50 MiB (headroom-bound)", d.GrantedDelta)
	}
}

func TestAllocator_GrantsExactSubMinimumTailWithoutOverGrant(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30,
		MinGrantStep:           4 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = float64(1 << 30)
	decision := a.Grant("small", 1<<20, 1<<30, UrgencyNormal)
	if decision.GrantedDelta != 1<<20 {
		t.Fatalf("sub-minimum tail was not granted exactly: %+v", decision)
	}
}

func TestAllocator_DefersSubMinimumPartialGrant(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30,
		MinGrantStep:           4 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = float64(1 << 30)
	decision := a.Grant("fragment", 8<<20, 1<<20, UrgencyNormal)
	if decision.GrantedDelta != 0 || decision.CooldownMs <= 0 {
		t.Fatalf("sub-minimum partial grant was not deferred: %+v", decision)
	}
}

func TestAllocator_RateLimited(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 10 << 20, // 10 MiB/s
		MinGrantStep:           1 << 20,
		MaxGrantStep:           100 << 20,
	})
	a.tokens = 5 << 20 // only 5 MiB available

	d := a.Grant("token-c", 100<<20, 1<<30, UrgencyNormal)
	if d.GrantedDelta != 0 {
		t.Errorf("rate-limited grant should be 0, got %d", d.GrantedDelta)
	}
	if d.CooldownMs <= 0 {
		t.Errorf("cooldown should be positive, got %d", d.CooldownMs)
	}
}

func TestAllocator_HighUrgencyBypassesRate(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 10 << 20,
		MinGrantStep:           1 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = 0 // rate-limited

	d := a.Grant("token-d", 50<<20, 1<<30, UrgencyHigh)
	if d.GrantedDelta != 50<<20 {
		t.Errorf("high urgency should bypass rate, got %d", d.GrantedDelta)
	}
}

func TestAllocator_FairnessScore(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30,
		MinGrantStep:           1 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = float64(1 << 30)
	_ = a.Grant("victim", 10<<20, 1<<30, UrgencyNormal)
	_ = a.Grant("victim", 20<<20, 1<<30, UrgencyNormal)
	score := a.FairnessScore("victim")
	if score != 30<<20 {
		t.Errorf("fairness score = %d, want 30 MiB", score)
	}
}

func TestAllocator_HistoryEvictsOldSamples(t *testing.T) {
	a := NewAllocator(AllocatorPolicy{
		MemoryGrantPerSecBytes: 1 << 30,
		MinGrantStep:           1 << 20,
		MaxGrantStep:           1 << 30,
	})
	a.tokens = float64(1 << 30)
	_ = a.Grant("oldie", 10<<20, 1<<30, UrgencyNormal)
	// Backdate the entry.
	a.mu.Lock()
	a.history["oldie"].last60s[0].at = time.Now().Add(-2 * time.Minute)
	a.mu.Unlock()
	// New grant triggers trim.
	_ = a.Grant("oldie", 5<<20, 1<<30, UrgencyNormal)
	score := a.FairnessScore("oldie")
	if score != 5<<20 {
		t.Errorf("after trim score = %d, want only most recent (5 MiB)", score)
	}
}
