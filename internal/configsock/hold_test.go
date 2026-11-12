package configsock

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type guestHoldTestCore struct {
	value  int64
	calls  int
	paused bool
}

func (c *guestHoldTestCore) GetGuestHold(_ context.Context, sid, run, path string) (int64, error) {
	c.calls++
	if sid != "sid" || run != "run" || path != "/work/hold" {
		return 0, api.ErrNotFound
	}
	return c.value, nil
}
func (c *guestHoldTestCore) SetGuestHold(ctx context.Context, sid, run, path string, v int64, m *int64) (int64, error) {
	if _, err := c.GetGuestHold(ctx, sid, run, path); err != nil {
		return 0, err
	}
	if c.paused {
		return 0, api.ErrHoldStateConflict
	}
	if m != nil && *m != c.value {
		return c.value, api.ErrHoldNotMatched
	}
	c.value = v
	return v, nil
}

type holdLoopbackTransport struct {
	handler http.Handler
	peer    int
}

func (rt *holdLoopbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.peer != 0 {
		req = req.WithContext(context.WithValue(req.Context(), peerPIDKey{}, rt.peer))
	}
	w := httptest.NewRecorder()
	rt.handler.ServeHTTP(w, req)
	return w.Result(), nil
}
func TestHoldRPCRequiresLiveRegisteredMMDSPeer(t *testing.T) {
	reg := NewRegistry()
	c := new(guestHoldTestCore)
	handler := New("", Deps{GuestHold: c, Plugins: reg}, slog.New(slog.NewTextHandler(io.Discard, nil))).router()
	rt := &holdLoopbackTransport{handler: handler}
	client := GuestHoldClient(&http.Client{Transport: rt}, "instance")
	p := &Plugin{ID: "instance", peerPID: 71, Caps: routesync.Register{Mmds: true, HoldTraffic: true, Proxy: &routesync.Proxy{}, Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake}}}
	reg.Add(p)
	ctx := context.Background()
	one, zero := int64(1), int64(0)
	call := func(v, m *int64) (*int64, int) { return client(ctx, "sid", "run", "/work/hold", v, m) }
	reject := func() {
		t.Helper()
		if actual, status := call(&one, nil); status != 503 || actual != nil {
			t.Fatal(status, actual)
		}
	}
	reject()
	rt.peer = 71
	reject()
	reg.markRouteStreamReady(p)
	rt.peer = 72
	reject()
	rt.peer = 71
	if c.calls != 0 {
		t.Fatal("unauthorized request reached core")
	}
	for _, tc := range []struct {
		v, m   *int64
		want   int
		actual int64
	}{{nil, nil, 200, 0}, {&one, &zero, 200, 1}, {&zero, &zero, 409, 1}} {
		v, s := call(tc.v, tc.m)
		if v == nil || *v != tc.actual || s != tc.want {
			t.Fatalf("status=%d actual=%v", s, v)
		}
	}
	c.paused = true
	if v, s := call(&zero, nil); s != 409 || v != nil {
		t.Fatal("state conflict returned a count", s, v)
	}
	reg.Remove(p)
	reject()
	legacy := &Plugin{ID: p.ID, peerPID: p.peerPID, Caps: p.Caps}
	legacy.Caps.HoldTraffic = false
	reg.Add(legacy)
	reg.markRouteStreamReady(legacy)
	reject()
	reg.Remove(legacy)
	observer := &Plugin{ID: "instance", peerPID: 71, Caps: routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}}}
	reg.Add(observer)
	reg.markRouteStreamReady(observer)
	reject()
}

func TestHoldAdmissionIncludesNotReadyAndLegacyServingPeers(t *testing.T) {
	r := NewRegistry()
	observer := &Plugin{ID: "observer", Caps: routesync.Register{HoldTraffic: true, Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}}}
	r.Add(observer)
	r.markRouteStreamReady(observer)
	if r.HoldTrafficReady() {
		t.Fatal("observer advertised hold support for the node")
	}
	modern := &Plugin{ID: "proxy", Caps: proxyCaps()}
	modern.Caps.HoldTraffic = true
	r.Add(modern)
	if r.HoldTrafficReady() {
		t.Fatal("incomplete registration enabled hold")
	}
	r.markRouteStreamReady(modern)
	if !r.HoldTrafficReady() {
		t.Fatal("ready modern proxy did not enable hold")
	}
	b, err := r.BeginProxyRouteBarrier()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()
	required := b.(*proxyRouteBarrier)
	if err = required.RequireHoldTraffic(); err != nil {
		t.Fatal(err)
	}
	r.routeBarrierAck(modern, b.ID())
	// A joining legacy proxy invalidates hold admission even before it can
	// participate in the ordinary route application barrier.
	legacy := &Plugin{ID: "candidate", Caps: proxyCaps()}
	r.Add(legacy)
	if r.HoldTrafficReady() || !errors.Is(b.Commit(), ErrProxyRouteLeaseChanged) {
		t.Fatal("mixed handover accepted hold admission")
	}
	r.markRouteStreamReady(legacy)
	if r.HoldTrafficReady() {
		t.Fatal("legacy participant accepted after sync")
	}
	r.Remove(legacy)
	if !r.HoldTrafficReady() {
		t.Fatal("departed legacy peer still suppresses support")
	}
	r.Remove(modern)
	if r.HoldTrafficReady() {
		t.Fatal("missing traffic authority accepted hold")
	}
}
