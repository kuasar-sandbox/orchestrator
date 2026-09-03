//go:build linux

package routesync_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestRouteBarrierAckFollowsEffectiveAdmissionPublication(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	table, err := proxyshm.Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	arena, err := proxyadmission.NewMaster(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer arena.Close()
	master := proxyshm.NewMasterViewWithAdmission(
		table,
		arena,
		config.MaxInflight{Total: 17, Forward: 11},
		time.Second,
		log,
	)

	sock := filepath.Join(t.TempDir(), "cfg.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	src := &fakeSource{sub: make(chan routesync.Event, 4), woke: make(chan string, 1)}
	streamReady := make(chan struct{}, 1)
	type ackSnapshot struct {
		id    string
		route routesync.RouteEntry
		found bool
	}
	barrierAcks := make(chan ackSnapshot, 1)
	httpSrv := &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reg, err := routesync.ReadRegister(r.Body)
		if err != nil {
			return
		}
		routesync.ServeStream(r.Context(), w, r.Body, src, reg, &routesync.StreamHooks{
			RouteStreamReady: func() { streamReady <- struct{}{} },
			RouteBarrierAck: func(id string) {
				route, found := table.Lookup("limited")
				barrierAcks <- ackSnapshot{id: id, route: route, found: found}
			},
		}, log)
	}), &http2.Server{})}
	go httpSrv.Serve(ln)
	defer httpSrv.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reg := routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRouteWake}}
	subscriberDone := make(chan struct{})
	go func() {
		defer close(subscriberDone)
		routesync.NewSubscriber(dial, "proxy", reg, master, master, log).Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-subscriberDone:
		case <-time.After(3 * time.Second):
			t.Error("subscriber did not stop before shared state cleanup")
		}
	}()
	recv(t, streamReady, "initial route stream")

	src.sub <- routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{
		SandboxID: "limited",
		StableID:  "stable-limited",
		Profile:   "e2b",
		State:     routesync.StateStarting,
	}}
	src.sub <- routesync.Event{Kind: routesync.TypeRouteBarrier, BarrierID: "effective-ready"}

	got := recv(t, barrierAcks, "route barrier ack")
	if got.id != "effective-ready" {
		t.Fatalf("barrier ack = %q", got.id)
	}
	if !got.found || got.route.State != routesync.StateStarting {
		t.Fatalf("route at ACK = %+v found=%v", got.route, got.found)
	}
	if got.route.EffectiveMaxInflight != (config.MaxInflight{Total: 17, Forward: 11}) {
		t.Fatalf("effective max_inflight at ACK = %+v", got.route.EffectiveMaxInflight)
	}
	if got.route.AdmissionSlot == 0 || got.route.AdmissionGeneration == 0 {
		t.Fatalf("admission binding at ACK = slot %d generation %d",
			got.route.AdmissionSlot, got.route.AdmissionGeneration)
	}
}
