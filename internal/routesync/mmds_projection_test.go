package routesync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

type mmdsProjectionSource struct {
	initial RouteEntry
	policy  Policy
	events  chan Event
	ranges  int
	replays int
}

func (s *mmdsProjectionSource) Range(_ context.Context, fn func(RouteEntry) error) error {
	s.ranges++
	return fn(s.initial)
}
func (s *mmdsProjectionSource) Subscribe() (<-chan Event, func()) {
	return s.events, func() {}
}
func (*mmdsProjectionSource) OnWake(context.Context, string) {}
func (s *mmdsProjectionSource) Policy() Policy               { return s.policy }
func (*mmdsProjectionSource) SourceFingerprint() string      { return "mmds-source" }
func (s *mmdsProjectionSource) Replay(_ context.Context, _ int64, _ func(Event) error) error {
	s.replays++
	return nil
}

func TestMMDSProjectionIsTrustedProxyOnly(t *testing.T) {
	values := MMDSRouteSecretValues{"key": []byte("value")}
	route := RouteEntry{
		SandboxID: "sandbox-1", State: StateRunning,
		MMDSRoutes:            `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
		MMDSRouteSecretValues: &values,
	}
	for _, tt := range []struct {
		name    string
		include bool
		want    bool
	}{
		{name: "trusted MMDS proxy", include: true, want: true},
		{name: "route observer", include: false, want: false},
		{name: "cluster node-link", include: false, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var wire bytes.Buffer
			if err := writeEvent(&wire, Event{Kind: TypeUpsert, Route: route}, tt.include); err != nil {
				t.Fatal(err)
			}
			message, err := ReadMsg(&wire)
			if err != nil {
				t.Fatal(err)
			}
			got := message.Route.MMDSRoutes != "" && message.Route.MMDSRouteSecretValues != nil
			if got != tt.want {
				t.Fatalf("MMDS projection present = %t", got)
			}
			encoded, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			if !tt.want && (bytes.Contains(encoded, []byte(`"mmds_routes"`)) || bytes.Contains(encoded, []byte(`"mmds_route_secret_values"`))) {
				t.Fatalf("untrusted projection retained MMDS fields")
			}
		})
	}
}

func TestMMDSPolicyProjectionIsTrustedProxyOnly(t *testing.T) {
	values := MMDSRouteSecretValues{"key": []byte("value")}
	initial := RouteEntry{
		SandboxID: "sandbox-1", State: StateRunning,
		MMDSRoutes:            `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
		MMDSRouteSecretValues: &values,
	}
	for _, tt := range []struct {
		name    string
		trusted bool
		claimed bool
	}{
		{name: "trusted proxy", trusted: true},
		{name: "observer", trusted: false},
		{name: "self-claimed observer", claimed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &mmdsProjectionSource{
				events: make(chan Event), initial: initial,
				policy: Policy{MMDS: &MMDSProxyPolicy{
					Enabled: true, Listen: "127.0.0.1:19254",
					Services: map[string]string{"svc": "unix:///run/svc.sock"},
				}},
			}
			downReader, downWriter := io.Pipe()
			upReader, upWriter := io.Pipe()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer upWriter.Close()
			reg := Register{Subscribe: &Subscribe{Kind: KindRoute}}
			reg.Mmds = tt.claimed
			if tt.trusted {
				reg.Subscribe.Kind = KindRouteWake
				reg.Proxy = &Proxy{}
				reg.Mmds = true
			}
			go ServeAuthority(ctx, downWriter, func() {}, upReader, source, reg, nil, nil)

			hello := readTestMessage(t, downReader)
			upsert := readTestMessage(t, downReader)
			bookmark := readTestMessage(t, downReader)
			if hello.Type != TypeHello || upsert.Type != TypeUpsert || bookmark.Type != TypeBookmark {
				t.Fatalf("stream types = %q %q %q", hello.Type, upsert.Type, bookmark.Type)
			}
			if (hello.Hello.Policy.MMDS != nil) != tt.trusted {
				t.Fatalf("policy MMDS present = %t", hello.Hello.Policy.MMDS != nil)
			}
			projected := upsert.Route.MMDSRoutes != "" && upsert.Route.MMDSRouteSecretValues != nil
			if projected != tt.trusted {
				t.Fatalf("route MMDS present = %t", projected)
			}
			cancel()
		})
	}
}

func TestDirectAuthorityNeverProjectsMMDSForNodeLink(t *testing.T) {
	values := MMDSRouteSecretValues{"key": []byte("value")}
	source := &mmdsProjectionSource{
		events: make(chan Event),
		initial: RouteEntry{
			SandboxID: "sandbox-1", State: StateRunning,
			MMDSRoutes:            `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
			MMDSRouteSecretValues: &values,
		},
	}
	downReader, downWriter := io.Pipe()
	upReader, upWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer upWriter.Close()
	// A remote node-link peer cannot turn on projection by setting the generic
	// capability bit; only ServeAuthority's validated proxy path may do so.
	reg := Register{Subscribe: &Subscribe{Kind: KindRegistry}, Mmds: true}
	go StreamAuthority(ctx, downWriter, func() {}, upReader, source, reg, nil, nil, nil)

	upsert := readTestMessage(t, downReader)
	bookmark := readTestMessage(t, downReader)
	if upsert.Type != TypeUpsert || bookmark.Type != TypeBookmark {
		t.Fatalf("stream types = %q %q", upsert.Type, bookmark.Type)
	}
	if upsert.Route.MMDSRoutes != "" || upsert.Route.MMDSRouteSecretValues != nil {
		t.Fatal("direct node-link authority projected MMDS state")
	}
}

func TestTrustedMMDSProxyAlwaysReceivesFullSnapshot(t *testing.T) {
	values := MMDSRouteSecretValues{"key": []byte("value")}
	source := &mmdsProjectionSource{
		events: make(chan Event),
		initial: RouteEntry{
			SandboxID: "sandbox-1", State: StateRunning,
			MMDSRoutes:            `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
			MMDSRouteSecretValues: &values,
		},
	}
	downReader, downWriter := io.Pipe()
	upReader, upWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer upWriter.Close()
	reg := Register{
		Subscribe:  &Subscribe{Kind: KindRouteWake},
		Proxy:      &Proxy{},
		Mmds:       true,
		ResumeFrom: MakeRevToken(source.SourceFingerprint(), 0),
	}
	go ServeAuthority(ctx, downWriter, func() {}, upReader, source, reg, nil, nil)

	_ = readTestMessage(t, downReader) // Hello
	upsert := readTestMessage(t, downReader)
	bookmark := readTestMessage(t, downReader)
	if upsert.Type != TypeUpsert || upsert.Route.MMDSRouteSecretValues == nil {
		t.Fatal("full MMDS snapshot did not include route values")
	}
	if bookmark.Type != TypeBookmark || !bookmark.FullSync || source.ranges != 1 || source.replays != 0 {
		t.Fatalf("MMDS sync metadata mismatch: full=%t ranges=%d replays=%d", bookmark.FullSync, source.ranges, source.replays)
	}
}

func readTestMessage(t *testing.T, reader io.Reader) *Msg {
	t.Helper()
	type result struct {
		message *Msg
		err     error
	}
	done := make(chan result, 1)
	go func() {
		message, err := ReadMsg(reader)
		done <- result{message: message, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		return got.message
	case <-time.After(time.Second):
		t.Fatal("timed out reading route-sync message")
		return nil
	}
}
