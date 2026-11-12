package routesync

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestHoldOnWireIsExplicitAndRequiresCompleteEvidence(t *testing.T) {
	for _, raw := range []string{`{"hold":{}}`, `{"hold":{"value":null}}`, `{"hold":{"value":-1}}`, `{"hold":{"value":0,"idle_since_boot_ns":9}}`, `{"hold":{"value":0,"idle_since":"2026-01-01T00:00:00Z"}}`} {
		var r RouteEntry
		if err := json.Unmarshal([]byte(raw), &r); err == nil {
			t.Fatalf("accepted incomplete projection %s", raw)
		}
	}
	now := time.Unix(1700000000, 0).UTC()
	for _, h := range []*HoldActivity{nil, {}, {Value: 2}, {IdleSince: &now, IdleSinceBootNS: 30}} {
		b, err := json.Marshal(RouteEntry{SandboxID: "s", Hold: h})
		if err != nil {
			t.Fatal(err)
		}
		var r RouteEntry
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if (h == nil) != (r.Hold == nil) || h != nil && (r.Hold.Value != h.Value || r.Hold.IdleSinceBootNS != h.IdleSinceBootNS) {
			t.Fatalf("projection not preserved: %s", b)
		}
	}
}

type holdReplaySource struct{ mmdsProjectionSource }

func (s *holdReplaySource) Replay(_ context.Context, _ int64, send func(Event) error) error {
	s.replays++
	return send(Event{Kind: TypeUpsert, Route: s.initial})
}

func TestHoldProjectionOnlyReachesLocalTrafficProxy(t *testing.T) {
	for _, tc := range []struct {
		name               string
		kind               string
		proxy, link, token bool
		wantHold, full     bool
	}{
		{name: "proxy without MMDS restores all holds", kind: KindRouteWake, proxy: true, token: true, wantHold: true, full: true},
		{name: "telemetry route observer", kind: KindRoute, full: true},
		{name: "wake without proxy capability", kind: KindRouteWake, full: true},
		{name: "observer replay", kind: KindRoute, token: true},
		{name: "registry node link", kind: KindRegistry, link: true, full: true},
		{name: "node link claiming local proxy", kind: KindRouteWake, proxy: true, link: true, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &holdReplaySource{mmdsProjectionSource{
				initial: RouteEntry{SandboxID: "s", State: StateRunning, Hold: &HoldActivity{Value: 3}},
				events:  make(chan Event, 2),
			}}
			reg := Register{Subscribe: &Subscribe{Kind: tc.kind}}
			if tc.proxy {
				reg.Proxy = &Proxy{}
			}
			if tc.token {
				reg.ResumeFrom = MakeRevToken(src.SourceFingerprint(), 1)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upR, upW := io.Pipe()
			downR, downW := io.Pipe()
			defer upW.Close()
			defer downR.Close()
			if tc.link {
				go StreamAuthority(ctx, downW, func() {}, upR, src, reg, nil, nil, nil)
			} else {
				go ServeAuthority(ctx, downW, func() {}, upR, src, reg, nil, nil)
				if m := readTestMessage(t, downR); m.Type != TypeHello {
					t.Fatal(m.Type)
				}
			}
			check := func(kind string) {
				t.Helper()
				m := readTestMessage(t, downR)
				if m.Type != kind || m.Route == nil || (m.Route.Hold != nil) != tc.wantHold {
					t.Fatalf("%s: got %+v", kind, m)
				}
				if tc.wantHold && m.Route.Hold.Value != 3 {
					t.Fatalf("%s: hold = %+v", kind, m.Route.Hold)
				}
			}
			check(TypeUpsert)
			if tc.kind == KindRegistry {
				_ = readTestMessage(t, downR) // build snapshot bracket
				_ = readTestMessage(t, downR)
			}
			if m := readTestMessage(t, downR); m.Type != TypeBookmark || m.FullSync != tc.full {
				t.Fatalf("unexpected snapshot bookmark: %+v", m)
			}
			src.events <- Event{Kind: TypeUpsert, Route: src.initial}
			check(TypeUpsert)
			pausing := src.initial
			pausing.State = StatePausing
			src.events <- Event{Kind: TypePauseBarrier, BarrierID: "b", Route: pausing}
			check(TypePauseBarrier)
		})
	}
}
