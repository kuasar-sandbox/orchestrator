package idleclock

import (
	"testing"
	"time"
)

func TestHistoricalInterval(t *testing.T) {
	start := Point{time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), 1000000000}
	for _, step := range []time.Duration{0, time.Hour, -time.Hour} {
		p := Proof{}
		p.Observe(start)
		end := Point{start.Wall.Add(10*time.Second + step), start.BootNS + int64(10*time.Second)}
		p.Observe(end)
		ok, known := p.Compare(Point{start.Wall.Add(time.Second), start.BootNS + int64(time.Second)}, start.Wall.Add(2*time.Second))
		if step == 0 && (!ok || !known) {
			t.Fatal("continuous history rejected")
		}
		if step != 0 && known {
			t.Fatal("clock step retained historical proof")
		}
	}
	p := Proof{}
	p.Observe(start)
	p.Observe(Point{start.Wall.Add(10 * time.Second), start.BootNS + int64(10*time.Second)})
	for _, delta := range []int64{-1, 0, 1} {
		ok, known := p.Compare(Point{start.Wall.Add(100), start.BootNS + 100}, start.Wall.Add(time.Duration(100+delta)))
		if !known || ok != (delta >= 0) {
			t.Fatalf("boundary %d: %v %v", delta, ok, known)
		}
	}
}

func TestClockRecoveryWithoutNewTraffic(t *testing.T) {
	base := time.Now()
	p := Proof{}
	p.Observe(Point{base, 100})
	p.Observe(Point{base.Add(time.Hour), 200})
	if _, known := p.Compare(Point{base, 100}, base.Add(time.Second)); known {
		t.Fatal("old interval accepted")
	}
	p.Observe(Point{base.Add(time.Hour + time.Second), 200 + int64(time.Second)})
	if ok, known := p.Compare(Point{base, 100}, base.Add(time.Hour+time.Second)); !ok || !known {
		t.Fatal("recovered idle rejected")
	}
}

func TestRollbackDoesNotReinterpretRepeatedUTC(t *testing.T) {
	base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	p := Proof{}
	p.Observe(Point{base, 1})
	p.Observe(Point{base.Add(20 * time.Second), 1 + int64(20*time.Second)})
	p.Broken = true
	p.Observe(Point{base.Add(-40 * time.Second), 1 + int64(21*time.Second)})
	p.Observe(Point{base.Add(5 * time.Second), 1 + int64(66*time.Second)})
	idle := Point{base.Add(10 * time.Second), 1 + int64(10*time.Second)}
	if ok, known := p.Compare(idle, base); ok || known {
		t.Fatal("repeated UTC reinterpreted")
	}
	p.Observe(Point{base.Add(30 * time.Second), 1 + int64(91*time.Second)})
	if ok, known := p.Compare(idle, base.Add(25*time.Second)); !ok || !known {
		t.Fatal("new unambiguous interval rejected")
	}
}

func TestClockCancellationCannotInferUnobservedHighWater(t *testing.T) {
	var proof Proof
	proof.Observe(Point{Wall: time.Unix(10, 0), BootNS: int64(time.Second)})
	// UTC briefly reached 20 before rolling back. A timerfd cancellation gives
	// no bound for that hidden value; endpoint samples cannot reconstruct it.
	proof.Broken, proof.Unbounded = true, true
	proof.Observe(Point{Wall: time.Unix(10, 200000000), BootNS: int64(1200 * time.Millisecond)})
	proof.Observe(Point{Wall: time.Unix(20, 500000000), BootNS: int64(11500 * time.Millisecond)})
	idle := Point{Wall: time.Unix(10, 150000000), BootNS: int64(1150 * time.Millisecond)}
	if satisfied, known := proof.Compare(idle, time.Unix(20, 0)); satisfied || known {
		t.Fatal("unobserved UTC excursion certified historical idle")
	}
}
