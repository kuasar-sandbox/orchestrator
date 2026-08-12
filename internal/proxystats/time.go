package proxystats

import (
	"time"

	"golang.org/x/sys/unix"
)

type timePoint struct {
	wall   time.Time
	bootNS int64
}

func currentTimePoint() timePoint {
	wall := time.Now().UTC()
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return timePoint{wall: wall, bootNS: wall.UnixNano()}
	}
	return timePoint{wall: wall, bootNS: ts.Nano()}
}

func laterPoint(a, b timePoint) timePoint {
	if b.bootNS > a.bootNS {
		return b
	}
	return a
}

func pointFromSnapshot(snapshot ServiceSnapshot) timePoint {
	if snapshot.IdleSince == nil || snapshot.IdleSinceBootNS == 0 {
		return timePoint{}
	}
	return timePoint{wall: snapshot.IdleSince.UTC(), bootNS: snapshot.IdleSinceBootNS}
}

func snapshotIdle(point timePoint) (*time.Time, int64) {
	if point.bootNS == 0 || point.wall.IsZero() {
		return nil, 0
	}
	wall := point.wall.UTC()
	return &wall, point.bootNS
}
