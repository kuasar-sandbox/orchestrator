package configsock

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func proxyCaps(path string) routesync.Register {
	return routesync.Register{
		Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:     &routesync.Proxy{Socket: routesync.Socket{Path: path}},
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

func TestRegistryMMDSAvailabilityTracksLiveProxyRegistration(t *testing.T) {
	r := NewRegistry()
	var states []bool
	r.SetMMDSChangeHook(func() { states = append(states, r.MMDSAvailable()) })
	if len(states) != 1 || states[0] {
		t.Fatalf("initial MMDS states = %v, want [false]", states)
	}

	wrongID := &Plugin{ID: "observer", Caps: proxyCaps("/run/observer.sock"), cancel: func() {}}
	wrongID.Caps.Mmds = true
	r.Add(wrongID)
	if r.MMDSAvailable() || len(states) != 1 {
		t.Fatalf("non-proxy registration changed MMDS capability: available=%v states=%v", r.MMDSAvailable(), states)
	}

	withoutListener := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/proxy.sock"), cancel: func() {}}
	r.Add(withoutListener)
	if r.MMDSAvailable() || len(states) != 1 {
		t.Fatalf("proxy without MMDS listener changed capability: available=%v states=%v", r.MMDSAvailable(), states)
	}

	withListener := &Plugin{ID: routesync.ProxyPluginID, Caps: proxyCaps("/run/proxy.sock"), cancel: func() {}}
	withListener.Caps.Mmds = true
	r.Add(withListener)
	if !r.MMDSAvailable() || len(states) != 2 || !states[1] {
		t.Fatalf("live MMDS proxy was not published: available=%v states=%v", r.MMDSAvailable(), states)
	}

	// The evicted registration's deferred Remove must not clear its successor.
	r.Remove(withoutListener)
	if !r.MMDSAvailable() || len(states) != 2 {
		t.Fatalf("stale remove cleared live MMDS proxy: available=%v states=%v", r.MMDSAvailable(), states)
	}
	r.Remove(withListener)
	if r.MMDSAvailable() || len(states) != 3 || states[2] {
		t.Fatalf("disconnect did not revoke MMDS capability: available=%v states=%v", r.MMDSAvailable(), states)
	}
}
