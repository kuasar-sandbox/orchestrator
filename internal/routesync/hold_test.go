package routesync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
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
		name                                string
		kind                                string
		proxy, link, token                  bool
		legacyClient, legacyAuthority, mmds bool
		wantHold, full                      bool
	}{
		{name: "proxy without MMDS restores all holds", kind: KindRouteWake, proxy: true, token: true, wantHold: true, full: true},
		{name: "legacy traffic proxy replays physical routes", kind: KindRouteWake, proxy: true, legacyClient: true, token: true},
		{name: "legacy MMDS proxy retains previous fullsync contract", kind: KindRouteWake, proxy: true, mmds: true, legacyClient: true, token: true, full: true},
		{name: "authority without hold", kind: KindRouteWake, proxy: true, mmds: true, legacyAuthority: true, token: true, full: true},
		{name: "negotiated MMDS proxy", kind: KindRouteWake, proxy: true, mmds: true, wantHold: true, full: true},
		{name: "telemetry route observer", kind: KindRoute, full: true},
		{name: "wake without proxy capability", kind: KindRouteWake, full: true},
		{name: "observer replay", kind: KindRoute, token: true},
		{name: "registry node link", kind: KindRegistry, link: true, full: true},
		{name: "node link claiming local proxy", kind: KindRouteWake, proxy: true, link: true, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const mixed = `{"routes":[{"path":"/status","data":"ok"},{"path":"/job","type":"hold-traffic"},{"path":"/svc","type":"service","service":"svc"},{"path":"/s","type":"secret","secret":"v"}]}`
			values := MMDSRouteSecretValues{"v": []byte("secret")}
			src := &holdReplaySource{mmdsProjectionSource{
				initial: RouteEntry{SandboxID: "s", State: StateRunning, Hold: &HoldActivity{Value: 3}, MMDSRoutes: mixed, MMDSRouteSecretValues: &values},
				events:  make(chan Event, 2),
			}}
			reg := Register{HoldTraffic: !tc.legacyClient, Mmds: tc.mmds, Subscribe: &Subscribe{Kind: tc.kind}}
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
				offered := []string{InplaceRestartV1}
				if !tc.legacyAuthority {
					offered = append(offered, HoldTrafficV1)
				}
				go serveAuthority(ctx, downW, func() {}, upR, src, reg, nil, nil, offered, nil)
				if m := readTestMessage(t, downR); m.Type != TypeHello || m.Hello.Version != 11 ||
					slices.Contains(m.Hello.ProxyCapabilities, HoldTrafficV1) != tc.wantHold || !slices.Contains(m.Hello.ProxyCapabilities, InplaceRestartV1) {
					t.Fatalf("unexpected Hello: %+v", m.Hello)
				}
			}
			check := func(kind string) {
				t.Helper()
				m := readTestMessage(t, downR)
				if m.Type != kind || m.Route == nil || (m.Route.Hold != nil) != tc.wantHold {
					t.Fatalf("%s: got %+v", kind, m)
				}
				switch {
				case !tc.mmds:
					if m.Route.MMDSRoutes != "" || m.Route.MMDSRouteSecretValues != nil {
						t.Fatal("MMDS leaked")
					}
				case tc.wantHold:
					if m.Route.MMDSRoutes != mixed {
						t.Fatal("negotiated declaration changed")
					}
				default:
					raw := m.Route.MMDSRoutes
					if strings.Contains(raw, "hold-traffic") || !strings.Contains(raw, "/status") || !strings.Contains(raw, "/svc") || !strings.Contains(raw, `"type":"secret"`) || m.Route.MMDSRouteSecretValues == nil || string((*m.Route.MMDSRouteSecretValues)["v"]) != "secret" {
						t.Fatalf("invalid legacy projection: %s", raw)
					}
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

func TestLegacyProjectionRejectsMalformedHoldDeclaration(t *testing.T) {
	for _, raw := range []string{`{"routes":[{"path":"/job","type":"hold-traffic","secret":"oops"}]}`, `{"routes":[{"path":"/job","type":"future-type"}]}`} {
		var w bytes.Buffer
		e := Event{Kind: TypeUpsert, Route: RouteEntry{MMDSRoutes: raw}}
		if err := writeEvent(&w, e, routeProjection{mmds: true}); err == nil || w.Len() != 0 {
			t.Fatalf("silently stripped malformed declaration: %s", raw)
		}
	}
}

// A released v11 authority will ignore the new field in Register. With no
// MMDS it would otherwise honor a resume token, sending only changed routes.
func TestHoldSubscriberRequestsFullRangeFromLegacyAuthority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wire bytes.Buffer
	reg := Register{Subscribe: &Subscribe{Kind: KindRouteWake}, Proxy: &Proxy{}, HoldTraffic: true, ResumeFrom: "existing-token"}
	s := &Subscriber{reg: reg}
	_ = s.writeUp(ctx, &wire, nil)
	frame, err := ReadMsg(&wire)
	if err != nil || frame.Register == nil || frame.Register.ResumeFrom != "" || !frame.Register.HoldTraffic {
		t.Fatalf("register=%+v err=%v", frame, err)
	}
	s.reg = Register{Subscribe: &Subscribe{Kind: KindRoute}, ResumeFrom: "observer-token"}
	wire.Reset()
	_ = s.writeUp(ctx, &wire, nil)
	frame, err = ReadMsg(&wire)
	if err != nil || frame.Register == nil || frame.Register.ResumeFrom != "observer-token" {
		t.Fatal("lost observer replay")
	}
}
