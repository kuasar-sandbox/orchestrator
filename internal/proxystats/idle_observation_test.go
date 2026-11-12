package proxystats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStaleStatsQueryCannotChangeRouteIdleEvidence(t *testing.T) {
	for _, state := range []types.State{types.StateRunning, types.StateStarting, types.StatePausing, types.StatePaused} {
		t.Run(string(state), func(t *testing.T) {
			m := NewMasterStats(nil, []string{"w"})
			n := timePoint{wall: time.Unix(1700000000, 0), bootNS: 100}
			m.now = func() timePoint { return n }
			m.trustSince = n
			readyWorker(t, m, "w", 1)
			old := routesync.RouteEntry{SandboxID: "s", RunID: "old", State: routesync.StateRunning, Hold: &routesync.HoldActivity{}}
			m.ObserveRoute(old)
			n.bootNS = 200
			n.wall = n.wall.Add(100)
			current := old
			current.RunID = "new"
			m.ObserveRoute(current)
			want := m.Activity(current)
			if want.State != "idle" || want.SinceBootNS != 200 {
				t.Fatal(want)
			}
			// A read validated the old route before ObserveRoute installed its successor.
			n.bootNS = 300
			n.wall = n.wall.Add(100)
			if _, err := m.SandboxTrafficStats(context.Background(), "s", "old", types.ProfileBare, state); !errors.Is(err, api.ErrStatsUnavailable) {
				t.Fatalf("stale stats query: %v", err)
			}
			if got := m.Activity(current); got != want {
				t.Fatalf("stale read changed activity: %+v, want %+v", got, want)
			}
			if got := m.PauseEvidence("s", "new"); got.SinceBootNS != 200 {
				t.Fatalf("stale read changed final-check baseline: %+v", got)
			}
			stats, err := m.SandboxTrafficStats(context.Background(), "s", "new", types.ProfileBare, types.StateRunning)
			if err != nil || stats.IdleSince == nil || !stats.IdleSince.Equal(time.Unix(1700000000, 100)) {
				t.Fatalf("current query disagrees with route baseline: %+v %v", stats, err)
			}
			m.ObserveRoute(routesync.RouteEntry{SandboxID: "s", State: routesync.StateDead})
			if m.PauseEvidence("s", "new").SinceBootNS != 0 {
				t.Fatal("deleted route retained proof")
			}
		})
	}
}
