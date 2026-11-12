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

func TestHoldCombinesWithPhysicalFlowsOnceAndPreservesZeroEdge(t *testing.T) {
	m := NewMasterStats(nil, []string{"a", "b"})
	point := timePoint{wall: time.Unix(1700000000, 0), bootNS: 10}
	m.now = func() timePoint { return point }
	m.trustSince = point
	readyWorker(t, m, "a", 1)
	readyWorker(t, m, "b", 1)
	r := routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning, Hold: &routesync.HoldActivity{}}
	m.ObserveRoute(r)
	first := m.Activity(r)
	if first.State != "idle" || first.SinceBootNS != 10 {
		t.Fatalf("never-used baseline: %+v", first)
	}
	// The business interval can be shorter than one Activity scan. The closing
	// publication itself must make the old idle interval distinguishable.
	point = timePoint{wall: point.wall.Add(time.Second), bootNS: 20}
	r.Hold = &routesync.HoldActivity{Value: 2}
	m.ObserveRoute(r)
	point = timePoint{wall: point.wall.Add(time.Second), bootNS: 30}
	released := point.wall
	r.Hold = &routesync.HoldActivity{IdleSince: &released, IdleSinceBootNS: point.bootNS}
	m.ObserveRoute(r)
	if got := m.Activity(r); got.State != "idle" || got.SinceBootNS != 30 {
		t.Fatalf("coalesced hold did not invalidate old idle: %+v", got)
	}
	point = timePoint{wall: point.wall.Add(time.Second), bootNS: 40}
	m.ObserveRoute(r)
	if got := m.Activity(r); got.SinceBootNS != 30 {
		t.Fatalf("release replay moved idle: %+v", got)
	}
	report := func(worker string, seq, connected uint64) {
		t.Helper()
		if err := m.Receive(worker, 1, Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: seq, Traffic: []SandboxSnapshot{{
			SandboxID: "s", Services: map[string]ServiceSnapshot{"forward": {Connected: connected}},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	report("a", 2, 1)
	report("b", 2, 1)
	if got := m.Activity(r); got.State != "busy" {
		t.Fatalf("zero hold masked real flows: %+v", got)
	}
	r.Hold = &routesync.HoldActivity{Value: 3}
	m.ObserveRoute(r)
	for _, profile := range []types.Profile{types.ProfileE2B, types.ProfileBare} {
		got, err := m.SandboxTrafficStats(context.Background(), "s", "r", profile, types.StateRunning)
		if err != nil {
			t.Fatal(err)
		}
		h := got.Services["hold"]
		if h.Connected != 3 || h.Parking != 0 || h.IdleSince != nil || got.Inflight.Connected != 5 || got.IdleSince != nil {
			t.Fatalf("%s stats count a replicated source incorrectly: %+v", profile, got)
		}
	}
	report("a", 3, 0)
	report("b", 3, 0)
	if got := m.Activity(r); got.State != "busy" {
		t.Fatalf("worker recompute masked positive hold: %+v", got)
	}
	m.ResetObservation()
	if got := m.Activity(r); got.State != "unknown" {
		t.Fatalf("resync retained old proof: %+v", got)
	}
	m.ObserveRoute(r)
	if got := m.Activity(r); got.State != "busy" {
		t.Fatalf("replayed positive hold was lost: %+v", got)
	}
	r.Hold = nil
	m.ObserveRoute(r)
	if got := m.Activity(r); got.State != "unknown" {
		t.Fatalf("missing hold became zero: %+v", got)
	}
	if _, err := m.SandboxTrafficStats(context.Background(), "s", "r", types.ProfileBare, types.StateRunning); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("missing hold returned valid stats: %v", err)
	}
}
