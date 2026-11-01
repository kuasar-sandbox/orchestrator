package configsock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestPauseHandshakeOnlineCutAndSynchronousActivity(t *testing.T) {
	r := NewRegistry()
	disconnected := addReadyPlugin(r, "gone")
	r.Remove(disconnected)
	empty, err := r.BeginProxyPauseBarrier()
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.Commit(); err != nil {
		t.Fatal(err)
	}
	a, b := addReadyPlugin(r, "a"), addReadyPlugin(r, "b")
	barrier, err := r.BeginProxyPauseBarrier()
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Cancel()
	r.routeBarrierAck(a, barrier.ID()) // A route-only ACK cannot certify activity.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := barrier.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("route ACK completed pause: %v", err)
	}
	sample := routesync.PauseAck{Activity: map[string]routesync.PauseActivity{"forward": {Parking: 1, Connected: 2}}}
	r.pauseAck(a, barrier.ID(), sample)
	r.pauseAck(a, barrier.ID(), sample)
	r.pauseAck(b, barrier.ID(), routesync.PauseAck{Activity: map[string]routesync.PauseActivity{"forward": {Connected: 3}}})
	if err := barrier.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := barrier.Activity(); got.Idle() || got.Activity["forward"].Parking != 1 || got.Activity["forward"].Connected != 5 {
		t.Fatalf("activity=%+v", got)
	}
	if err := barrier.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPauseHandshakeNeverShrinksParticipants(t *testing.T) {
	for _, action := range []string{"disconnect", "replace", "join"} {
		t.Run(action, func(t *testing.T) {
			r := NewRegistry()
			a, b := addReadyPlugin(r, "a"), addReadyPlugin(r, "b")
			barrier, err := r.BeginProxyPauseBarrier()
			if err != nil {
				t.Fatal(err)
			}
			defer barrier.Cancel()
			ack := routesync.PauseAck{Activity: map[string]routesync.PauseActivity{}}
			r.pauseAck(a, barrier.ID(), ack)
			switch action {
			case "disconnect":
				r.Remove(b)
			case "replace":
				addReadyPlugin(r, "b")
			case "join":
				addReadyPlugin(r, "c")
			}
			r.pauseAck(b, barrier.ID(), ack)
			if err := barrier.Wait(context.Background()); err == nil {
				t.Fatal("membership change accepted")
			}
			if err := barrier.Commit(); err == nil {
				t.Fatal("membership change committed")
			}
		})
	}
}
