//go:build linux

package proxyshm

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestPausingParksHTTPAcrossCaptureAndResumesNewBackend(t *testing.T) {
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "new") }))
	defer next.Close()
	wakes := make(chan struct{}, 4)
	f := newHTTPRouteFixture(t, func(string) {
		select {
		case wakes <- struct{}{}:
		default:
		}
	})
	var oldDials, newDials atomic.Int32
	px := proxy.NewWithDialer(f.view, nil, nil, nil, func(ctx context.Context, route proxy.Route) (net.Conn, error) {
		if route.RunID != "run-2" {
			oldDials.Add(1)
		} else {
			newDials.Add(1)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", next.Listener.Addr().String())
	}, "").WithTrafficTracker(f.stats)
	pausing := f.entry
	pausing.State = routesync.StatePausing
	f.apply(t, pausing)
	response, done := startHTTPRouteRequest(t, context.Background(), px)
	select {
	case <-wakes:
	case <-time.After(time.Second):
		t.Fatal("pausing request did not Wake")
	}
	// Ordinary route synchronization loss must preserve the installed gate.
	f.master.InvalidateSync()
	paused := pausing
	paused.State = routesync.StatePaused
	paused.RunID = ""
	paused.FloatingIP = ""
	f.apply(t, paused)
	select {
	case <-done:
		t.Fatalf("capture progress completed parked request: HTTP %d", response.Code)
	case <-time.After(20 * time.Millisecond):
	}
	running := f.entry
	running.RunID = "run-2"
	running.FloatingIP = "100.101.0.1"
	f.apply(t, running)
	awaitHTTPRoute(t, done)
	if response.Code != http.StatusOK || response.Body.String() != "new" || oldDials.Load() != 0 || newDials.Load() != 1 {
		t.Fatalf("HTTP=%d old=%d new=%d", response.Code, oldDials.Load(), newDials.Load())
	}
	f.assertReleased(t)
}

func TestPausingPreservesBusyTransportAndFailedResumeRevision(t *testing.T) {
	f := newHTTPRouteFixture(t, nil)
	binding, ok := workerRouteBinding(f.entry, true, proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080})
	if !ok {
		t.Fatal("missing binding")
	}
	route := workerDialRoute(f.entry, binding)
	pausing := f.entry
	pausing.State = routesync.StatePausing
	f.apply(t, pausing)
	if !f.view.transportRouteCurrent(binding, route) {
		t.Fatal("pause gate canceled existing flow before idle sample")
	}
	_, _, revision := f.table.LookupRevision(f.entry.SandboxID)
	paused := pausing
	paused.State = routesync.StatePaused
	paused.RunID = ""
	paused.FloatingIP = ""
	f.apply(t, paused)
	_, _, progress := f.table.LookupRevision(f.entry.SandboxID)
	if progress != revision {
		t.Fatal("capture completion looks like failed Resume")
	}
	starting := paused
	starting.State = routesync.StateStarting
	f.apply(t, starting)
	f.apply(t, paused)
	_, _, failed := f.table.LookupRevision(f.entry.SandboxID)
	if failed == revision {
		t.Fatal("coalesced Resume rollback was hidden")
	}
}
