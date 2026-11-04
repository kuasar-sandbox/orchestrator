// Package idleclock keeps historical UTC/BOOTTIME evidence for idle conditions.
package idleclock

import (
	"time"

	"golang.org/x/sys/unix"
)

type Point struct {
	Wall   time.Time
	BootNS int64
}

func Now() Point {
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts) != nil {
		return Point{}
	}
	return Point{Wall: time.Now().UTC(), BootNS: ts.Nano()}
}

// Proof bounds the UTC offset observed throughout one continuous clock interval.
// It is deliberately independent of a request's arrival time.
type Proof struct {
	Unbounded        bool // A clock-set cancellation cannot reveal an intervening UTC high-water mark.
	Discontinuous    bool
	Broken           bool
	UncertainThrough time.Time
	Start            Point
	End              Point
	MinOffset        int64
	MaxOffset        int64
}

func (p *Proof) Observe(n Point) {
	if n.BootNS <= 0 || n.Wall.IsZero() {
		*p = Proof{Unbounded: p.Unbounded}
		return
	}
	offset := n.Wall.UnixNano() - n.BootNS
	if p.Broken || p.Start.BootNS == 0 || n.BootNS < p.End.BootNS || abs(offset-(p.End.Wall.UnixNano()-p.End.BootNS)) > int64(time.Millisecond) {
		uncertain := p.UncertainThrough
		if p.Start.BootNS != 0 {
			upper := p.End.Wall.Add(time.Duration(max(int64(0), n.BootNS-p.End.BootNS)))
			if upper.After(uncertain) {
				uncertain = upper
			}
			if n.Wall.After(uncertain) {
				uncertain = n.Wall
			}
		}
		*p = Proof{Unbounded: p.Unbounded, Start: n, MinOffset: offset, MaxOffset: offset, Discontinuous: p.Discontinuous || p.Start.BootNS != 0, UncertainThrough: uncertain}
	}
	p.End = n
	p.MinOffset = min(p.MinOffset, offset)
	p.MaxOffset = max(p.MaxOffset, offset)
}
func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// Compare uses the UTC timestamp recorded at the historical last-zero event,
// never a request-time UTC-to-BOOTTIME conversion. BOOTTIME binds that event to
// the uninterrupted clock interval. Timerfd invalidation covers intervening
// clock sets; observed offset changes provide an additional conservative fence.
func (p Proof) Compare(idle Point, x time.Time) (satisfied, known bool) {
	if p.Unbounded || p.Broken || p.Start.BootNS <= 0 || idle.BootNS <= 0 || idle.Wall.IsZero() || idle.BootNS > p.End.BootNS || x.After(p.End.Wall) {
		return false, false
	}
	if !p.UncertainThrough.IsZero() && !x.After(p.UncertainThrough) {
		return false, false
	}
	if x.Before(p.Start.Wall) {
		return false, !p.Discontinuous
	}
	if idle.BootNS < p.Start.BootNS {
		if p.Discontinuous && !x.After(idle.Wall) {
			return false, false
		}
		idle = p.Start
	}
	if idle.Wall.Before(p.Start.Wall) {
		return false, false
	}
	return !idle.Wall.After(x), true
}
