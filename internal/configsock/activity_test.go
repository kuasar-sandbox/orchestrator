package configsock

import (
	"context"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestActivityLeaseReplacementAndMultipleProxies(t *testing.T) {
	r := NewRegistry()
	old := addReadyPlugin(r, "proxy")
	source := activitySource(old)
	if !r.ValidActivitySource(source) {
		t.Fatal("single ready source unavailable")
	}
	next := addReadyPlugin(r, "proxy")
	called := false
	r.deliverActivity(old, func(bool) { called = true })
	if called {
		t.Fatal("old checked callback delivered after replacement")
	}
	if r.ValidActivitySource(source) || !r.ValidActivitySource(activitySource(next)) {
		t.Fatal("source fence failed")
	}
	other := addReadyPlugin(r, "other")
	if r.ValidActivitySource(activitySource(next)) {
		t.Fatal("multi-proxy range accepted")
	}
	r.Remove(other)
	if !r.ValidActivitySource(activitySource(next)) {
		t.Fatal("single source recovery failed")
	}
}

type activityTestSource struct {
	nativeStatsSource
	events chan *routesync.Activity
}

func (s *activityTestSource) OnProxyActivity(_ context.Context, _ string, a *routesync.Activity) {
	if a != nil {
		c := *a
		a = &c
	}
	s.events <- a
}
func TestActivitySequenceRejectsGapAndReplay(t *testing.T) {
	for _, sequence := range []uint64{0, 1, 3} {
		src := &activityTestSource{events: make(chan *routesync.Activity, 16)}
		socket, _ := startTestServer(t, Deps{Plugins: NewRegistry(), RouteSource: src})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		reader, writer := io.Pipe()
		caps := proxyCaps()
		go func() { _ = routesync.WriteMsg(writer, &routesync.Msg{Type: routesync.TypeRegister, Register: &caps}) }()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "http://conductor/internal/plugin/proxy/register", reader)
		response, err := HTTPClient(socket).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		for {
			msg, err := routesync.ReadMsg(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if msg.Type == routesync.TypeBookmark {
				break
			}
		}
		if err := routesync.WriteMsg(writer, &routesync.Msg{Type: routesync.TypeActivity, Activity: &routesync.Activity{Sequence: 1, SandboxID: "s", RunID: "r", State: "idle", Generation: 1, SinceBootNS: 1}}); err != nil {
			t.Fatal(err)
		}
		for {
			select {
			case a := <-src.events:
				if a != nil {
					if a.Sequence != 1 {
						t.Fatal(a)
					}
					goto received
				}
			case <-ctx.Done():
				t.Fatal("initial observation missing")
			}
		}
	received:
		if err := routesync.WriteMsg(writer, &routesync.Msg{Type: routesync.TypeActivity, Activity: &routesync.Activity{Sequence: sequence, SandboxID: "s", RunID: "r", State: "busy"}}); err != nil {
			t.Fatal(err)
		}
		select {
		case a := <-src.events:
			if a != nil {
				t.Fatal("out-of-order activity delivered", a)
			}
		case <-ctx.Done():
			t.Fatal("bad sequence did not invalidate lease")
		}
		writer.Close()
		response.Body.Close()
		cancel()
	}
}
