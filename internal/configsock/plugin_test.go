package configsock

import (
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
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
