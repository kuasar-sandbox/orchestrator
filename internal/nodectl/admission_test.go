package nodectl

import (
	"testing"
	"time"
)

func TestAdmission_TokenBucket(t *testing.T) {
	a := NewAdmissionController(AdmissionPolicy{
		Rate:                  4,
		Burst:                 2,
		MaxConcurrentCreating: 100,
	})
	// Burst=2: first two should pass.
	if d := a.TryAdmit(); d.Status != StatusAdmitted {
		t.Errorf("admit 1: status=%s, want admitted", d.Status)
	}
	a.CountAdmit()
	if d := a.TryAdmit(); d.Status != StatusAdmitted {
		t.Errorf("admit 2: status=%s, want admitted", d.Status)
	}
	a.CountAdmit()
	// Third must queue.
	d := a.TryAdmit()
	if d.Status != StatusQueued {
		t.Errorf("admit 3: status=%s, want queued", d.Status)
	}
	if d.QueueWait <= 0 {
		t.Errorf("queue wait = %v, want > 0", d.QueueWait)
	}
}

func TestAdmission_ConcurrencyCap(t *testing.T) {
	a := NewAdmissionController(AdmissionPolicy{
		Rate:                  100,
		Burst:                 100,
		MaxConcurrentCreating: 2,
	})
	a.CountAdmit()
	a.CountAdmit()
	d := a.TryAdmit()
	if d.Status != StatusQueued {
		t.Errorf("expected queued at cap, got %s", d.Status)
	}
	a.Settled()
	if a.CreatingCount() != 1 {
		t.Errorf("after Settled, creating = %d, want 1", a.CreatingCount())
	}
	d = a.TryAdmit()
	if d.Status != StatusAdmitted {
		t.Errorf("after settled drop, status=%s, want admitted", d.Status)
	}
}

func TestAdmission_RefillOverTime(t *testing.T) {
	a := NewAdmissionController(AdmissionPolicy{
		Rate:                  10, // 10/sec
		Burst:                 1,
		MaxConcurrentCreating: 100,
	})
	if d := a.TryAdmit(); d.Status != StatusAdmitted {
		t.Fatal("first admit should pass")
	}
	a.CountAdmit()
	// Bucket empty; manipulate lastRefill to simulate time passage.
	a.mu.Lock()
	a.lastRefill = time.Now().Add(-time.Second)
	a.mu.Unlock()
	if d := a.TryAdmit(); d.Status != StatusAdmitted {
		t.Errorf("after 1s refill should pass, got %s", d.Status)
	}
}

func TestNewToken_Format(t *testing.T) {
	t1 := NewToken()
	t2 := NewToken()
	if t1 == t2 {
		t.Error("two tokens should differ")
	}
	if len(t1) != 32 {
		t.Errorf("token length = %d, want 32 hex chars", len(t1))
	}
}
