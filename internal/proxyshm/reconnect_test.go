package proxyshm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type bookmarkSignal struct {
	routesync.Sink
	ready chan struct{}
}

func (s *bookmarkSignal) Bookmark() {
	s.Sink.Bookmark()
	close(s.ready)
}

func TestReconnectAfterOfflineDeleteAndReset(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			table, arena, view := newTrafficMasterView(t, capacity, 1, config.MaxInflight{Total: 1})
			gone := trafficRoute("deleted-A", routesync.StateRunning, nil)
			reset := trafficRoute("reset-B", routesync.StatePaused, nil)
			retained := trafficRoute("running-C", routesync.StateRunning, nil)
			if err := view.ApplyUpsert(gone); err != nil {
				t.Fatal(err)
			}
			rows := []routesync.RouteEntry{reset}
			var worker *proxyadmission.Worker
			var binding proxyadmission.Binding
			if capacity == 2 {
				if err := view.ApplyUpsert(retained); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, retained)
				route, _ := table.Lookup(retained.SandboxID)
				binding = proxyadmission.Binding{Slot: route.AdmissionSlot, Generation: route.AdmissionGeneration, Limits: route.EffectiveMaxInflight}
				if err := arena.BeginWorker(0, 1); err != nil {
					t.Fatal(err)
				}
				file, err := arena.DupFile()
				if err != nil {
					t.Fatal(err)
				}
				worker, err = proxyadmission.OpenWorker(file, capacity, 1, 0, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer worker.Close()
				lease, err := worker.TryAcquire(binding, proxyadmission.ServiceExec)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release()
			}
			dial := func(context.Context) (net.Conn, error) {
				client, server := net.Pipe()
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if _, err := routesync.ReadRegister(r.Body); err != nil {
						return
					}
					frames := []*routesync.Msg{{Type: routesync.TypeHello, Hello: &routesync.Hello{Version: routesync.Version}}}
					for i := range rows {
						frames = append(frames, &routesync.Msg{Type: routesync.TypeUpsert, Route: &rows[i]})
					}
					frames = append(frames, &routesync.Msg{Type: routesync.TypeBookmark, FullSync: true})
					for _, frame := range frames {
						if err := routesync.WriteMsg(w, frame); err != nil {
							return
						}
					}
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				})
				go new(http2.Server).ServeConn(server, &http2.ServeConnOpts{Handler: handler})
				return client, nil
			}
			sink := &bookmarkSignal{Sink: view, ready: make(chan struct{})}
			subscriber := routesync.NewSubscriber(dial, "proxy", routesync.Register{}, sink, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			subscriber.SnapshotSlots = table.SnapshotSlots
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); subscriber.Run(ctx) }()
			defer func() { cancel(); <-done }()
			select {
			case <-sink.ready:
			case <-time.After(3 * time.Second):
				t.Fatal("full Proxy table could not finish reconnect")
			}
			if _, found, rev := table.LookupRevision(gone.SandboxID); found || rev == 0 {
				t.Fatal("hard-deleted route did not receive a terminal revision")
			}
			if route, found := table.Lookup(reset.SandboxID); !found || route.State != routesync.StatePaused {
				t.Fatal("the Reset parent was not published as paused")
			}
			if worker != nil {
				if _, err := worker.TryAcquire(binding, proxyadmission.ServiceExec); !errors.Is(err, proxyadmission.ErrLimitReached) {
					t.Fatalf("replay did not preserve the active route's lease: %v", err)
				}
			}
		})
	}
}
