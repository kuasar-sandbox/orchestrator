package configsock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func proxyCaps() routesync.Register {
	return routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:     &routesync.Proxy{},
	}
}

func addReadyPlugin(r *Registry, id string) *Plugin {
	p := &Plugin{ID: id, Caps: proxyCaps(), cancel: func() {}}
	r.Add(p)
	r.markRouteStreamReady(p)
	return p
}

func TestProxyRouteBarrierRequiresCurrentReadyTrustedLease(t *testing.T) {
	r := NewRegistry()
	if _, err := r.BeginProxyRouteBarrier(); !errors.Is(err, ErrProxyRouteUnavailable) {
		t.Fatalf("barrier without proxy = %v", err)
	}
	observer := &Plugin{ID: routesync.ProxyPluginID, Caps: routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}, Proxy: &routesync.Proxy{},
	}, cancel: func() {}}
	r.Add(observer)
	r.markRouteStreamReady(observer)
	if _, err := r.BeginProxyRouteBarrier(); !errors.Is(err, ErrProxyRouteUnavailable) {
		t.Fatalf("barrier with ordinary observer = %v", err)
	}
	p := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: func() {}}
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
	r.routeBarrierAck(p, b.ID()) // late ACK after Commit is a no-op.
	r.mu.Lock()
	remaining := len(r.barriers)
	r.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("late ACK recreated %d barrier(s)", remaining)
	}
}

func TestProxyRouteBarrierAllOfParticipants(t *testing.T) {
	r := NewRegistry()
	first := addReadyPlugin(r, "proxy-a")
	second := addReadyPlugin(r, "proxy-b")
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
			r.Add(&Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: func() {}})
		}, want: ErrProxyRouteLeaseChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := NewRegistry()
			p := addReadyPlugin(r, routesync.ProxyPluginID)
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
	p := addReadyPlugin(r, routesync.ProxyPluginID)
	b, err := r.BeginProxyRouteBarrier()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()
	r.routeBarrierAck(p, b.ID())
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Add(&Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: func() {}})
	if err := b.Commit(); !errors.Is(err, ErrProxyRouteLeaseChanged) {
		t.Fatalf("Commit after replacement = %v", err)
	}
}

// TestRegistryEvictsSameID: a second registration with the same id cancels the
// first (deregister via stream close), and the evicted plugin's later Remove must
// not drop its successor.
func TestRegistryEvictsSameID(t *testing.T) {
	r := NewRegistry()
	evicted := make(chan struct{}, 1)
	first := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: func() { evicted <- struct{}{} }}
	second := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: func() {}}

	r.Add(first)
	r.Add(second) // same id -> first.cancel() fires (closes its stream)
	select {
	case <-evicted:
	default:
		t.Fatal("re-register did not evict (cancel) the prior same-id plugin")
	}
	r.markRouteStreamReady(second)

	// The evicted plugin's deferred Remove must be a no-op (not its successor's).
	r.Remove(first)
	barrier, err := r.BeginProxyRouteBarrier()
	if err != nil {
		t.Fatalf("evicted Remove dropped the successor: %v", err)
	}
	barrier.Cancel()
	r.Remove(second)
	if _, err := r.BeginProxyRouteBarrier(); !errors.Is(err, ErrProxyRouteUnavailable) {
		t.Fatalf("after final remove barrier = %v", err)
	}
}

func TestRegistryProxyStatsTargetFollowsRegistrationLease(t *testing.T) {
	r := NewRegistry()
	noop := func() {}
	withoutStats := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps(), cancel: noop}
	r.Add(withoutStats)
	if path, found := r.ProxyStatsTarget(); found || path != "" {
		t.Fatalf("stats target without capability = %q/%t", path, found)
	}

	withStatsCaps := proxyCaps()
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
	trusted := proxyCaps()
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
