package configsock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func proxyCaps(path string) routesync.Register {
	return routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:     &routesync.Proxy{Socket: routesync.Socket{Path: path}},
	}
}

func addReadyPlugin(r *Registry, id, path string) *Plugin {
	p := &Plugin{ID: id, Caps: proxyCaps(path), cancel: func() {}}
	r.Add(p)
	r.markRouteStreamReady(p)
	return p
}

func TestProxyRouteBarrierRequiresCurrentReadyTrustedLease(t *testing.T) {
	r := NewRegistry()
	if _, err := r.BeginProxyRouteBarrier(); !errors.Is(err, ErrProxyRouteUnavailable) {
		t.Fatalf("barrier without proxy = %v", err)
	}
	p := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/proxy.sock"), cancel: func() {}}
	r.Add(p)
	if _, err := r.BeginProxyRouteBarrier(); !errors.Is(err, ErrProxyRouteUnavailable) {
		t.Fatalf("barrier before route subscription = %v", err)
	}
	r.markRouteStreamReady(p)
	b, err := r.BeginProxyRouteBarrier()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()
	r.routeBarrierAck(p, b.ID())
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestProxyRouteBarrierAllOfParticipants(t *testing.T) {
	r := NewRegistry()
	first := addReadyPlugin(r, "proxy-a", "/run/a.sock")
	second := addReadyPlugin(r, "proxy-b", "/run/b.sock")
	r.mu.Lock()
	b, err := r.beginRouteBarrierLocked([]*Plugin{first, second})
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()

	r.routeBarrierAck(first, b.ID())
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("one-of-two ACK completed barrier: %v", err)
	}
	r.routeBarrierAck(first, b.ID()) // duplicate is idempotent
	r.routeBarrierAck(second, b.ID())
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestProxyRouteBarrierFailsOnDisconnectAndReplacement(t *testing.T) {
	for _, test := range []struct {
		name string
		fail func(*Registry, *Plugin)
		want error
	}{
		{name: "disconnect", fail: func(r *Registry, p *Plugin) { r.Remove(p) }, want: ErrProxyRouteDisconnected},
		{name: "replacement", fail: func(r *Registry, _ *Plugin) {
			r.Add(&Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/new.sock"), cancel: func() {}})
		}, want: ErrProxyRouteLeaseChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := NewRegistry()
			p := addReadyPlugin(r, routesync.ProxyPluginID, "/run/proxy.sock")
			b, err := r.BeginProxyRouteBarrier()
			if err != nil {
				t.Fatal(err)
			}
			defer b.Cancel()
			test.fail(r, p)
			if err := b.Wait(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("Wait = %v, want %v", err, test.want)
			}
		})
	}
}

func TestProxyRouteBarrierCommitRevalidatesAckedLease(t *testing.T) {
	r := NewRegistry()
	p := addReadyPlugin(r, routesync.ProxyPluginID, "/run/proxy.sock")
	b, err := r.BeginProxyRouteBarrier()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()
	r.routeBarrierAck(p, b.ID())
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Add(&Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/new.sock"), cancel: func() {}})
	if err := b.Commit(); !errors.Is(err, ErrProxyRouteLeaseChanged) {
		t.Fatalf("Commit after replacement = %v", err)
	}
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRegistryProxyTargetsStableSorted: ProxyTargets returns only proxy plugins'
// sockets, in stable id order (so sandbox-id sharding is consistent), excluding
// pure observers.
func TestRegistryProxyTargetsStableSorted(t *testing.T) {
	r := NewRegistry()
	noop := func() {}
	r.Add(&Plugin{ID: "px2", Caps: proxyCaps("/run/px2.sock"), cancel: noop})
	r.Add(&Plugin{ID: "px0", Caps: proxyCaps("/run/px0.sock"), cancel: noop})
	// An observer (route only, no proxy socket) must not be a forward target.
	r.Add(&Plugin{ID: "agent", Caps: routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}}, cancel: noop})

	if got, want := r.ProxyTargets(), []string{"/run/px0.sock", "/run/px2.sock"}; !eq(got, want) {
		t.Fatalf("ProxyTargets = %v, want %v", got, want)
	}
}

// TestRegistryEvictsSameID: a second registration with the same id cancels the
// first (deregister via stream close), and the evicted plugin's later Remove must
// not drop its successor.
func TestRegistryEvictsSameID(t *testing.T) {
	r := NewRegistry()
	evicted := make(chan struct{}, 1)
	first := &Plugin{ID: "px0", Caps: proxyCaps("/run/a.sock"), cancel: func() { evicted <- struct{}{} }}
	second := &Plugin{ID: "px0", Caps: proxyCaps("/run/b.sock"), cancel: func() {}}

	r.Add(first)
	r.Add(second) // same id -> first.cancel() fires (closes its stream)
	select {
	case <-evicted:
	default:
		t.Fatal("re-register did not evict (cancel) the prior same-id plugin")
	}
	if got := r.ProxyTargets(); len(got) != 1 || got[0] != "/run/b.sock" {
		t.Fatalf("after eviction targets = %v, want [/run/b.sock]", got)
	}

	// The evicted plugin's deferred Remove must be a no-op (not its successor's).
	r.Remove(first)
	if got := r.ProxyTargets(); len(got) != 1 || got[0] != "/run/b.sock" {
		t.Fatalf("evicted Remove dropped the successor: targets = %v", got)
	}
	r.Remove(second)
	if got := r.ProxyTargets(); len(got) != 0 {
		t.Fatalf("after final remove targets = %v, want []", got)
	}
}

func TestRegistryProxyStatsTargetFollowsRegistrationLease(t *testing.T) {
	r := NewRegistry()
	noop := func() {}
	withoutStats := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/proxy.sock"), cancel: noop}
	r.Add(withoutStats)
	if path, found := r.ProxyStatsTarget(); found || path != "" {
		t.Fatalf("stats target without capability = %q/%t", path, found)
	}

	withStatsCaps := proxyCaps("/run/proxy.sock")
	withStatsCaps.Proxy.StatsSocket = &routesync.Socket{Path: "/run/proxy-stats.sock"}
	withStats := &Plugin{ID: routesync.ProxyPluginID, Caps: withStatsCaps, cancel: noop}
	r.Add(withStats)
	if path, found := r.ProxyStatsTarget(); !found || path != "/run/proxy-stats.sock" {
		t.Fatalf("stats target = %q/%t", path, found)
	}
	r.Remove(withoutStats)
	if path, found := r.ProxyStatsTarget(); !found || path != "/run/proxy-stats.sock" {
		t.Fatalf("evicted lease removed successor target = %q/%t", path, found)
	}
	r.Remove(withStats)
	if path, found := r.ProxyStatsTarget(); found || path != "" {
		t.Fatalf("removed lease retained target = %q/%t", path, found)
	}
}

func TestTrustedMMDSProxyRegistrationShape(t *testing.T) {
	trusted := proxyCaps("/run/proxy.sock")
	trusted.Mmds = true
	if !isTrustedMMDSProxyRegistration(routesync.ProxyPluginID, trusted) {
		t.Fatal("exact trusted registration rejected")
	}
	for _, mutation := range []func(*routesync.Register){
		func(r *routesync.Register) { r.Subscribe.Kind = routesync.KindRoute },
		func(r *routesync.Register) { r.Proxy = nil },
		func(r *routesync.Register) { r.Mmds = false },
	} {
		candidate := trusted
		subscribe := *trusted.Subscribe
		candidate.Subscribe = &subscribe
		mutation(&candidate)
		if isTrustedMMDSProxyRegistration(routesync.ProxyPluginID, candidate) {
			t.Fatal("non-proxy MMDS registration was trusted")
		}
	}
	if isTrustedMMDSProxyRegistration("observer", trusted) {
		t.Fatal("non-proxy plugin id was trusted")
	}
}
