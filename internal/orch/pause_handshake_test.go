package orch

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type fakePauseBarrier struct {
	*fakeRouteBarrier
	activity routesync.PauseAck
}

func (b *fakePauseBarrier) Activity() routesync.PauseAck                            { return b.activity }
func (b *fakePauseBarrier) BeginProxyPauseBarrier() (routesync.PauseBarrier, error) { return b, nil }

func TestPauseWaitsForActivityACKBeforeCapture(t *testing.T) {
	for _, test := range []struct {
		name         string
		ifIdle, busy bool
		failure      error
	}{
		{name: "idle", ifIdle: true}, {name: "busy conditional", ifIdle: true, busy: true}, {name: "busy ordinary", busy: true}, {name: "disconnect", failure: errors.New("disconnected")},
	} {
		t.Run(test.name, func(t *testing.T) {
			o, sb, key, _, _, args := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
			barrier := &fakePauseBarrier{fakeRouteBarrier: newFakeRouteBarrier(test.failure, nil), activity: routesync.PauseAck{Activity: map[string]routesync.PauseActivity{}}}
			if test.busy {
				barrier.activity.Activity["forward"] = routesync.PauseActivity{Connected: 1}
			}
			o.pauseBarriers = barrier
			done := make(chan error, 1)
			go func() {
				_, err := o.Pause(context.Background(), sb.ID, key, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox, IfIdle: test.ifIdle})
				done <- err
			}()
			select {
			case <-barrier.waiting:
			case <-time.After(time.Second):
				t.Fatal("no handshake")
			}
			if _, err := os.Stat(args); !os.IsNotExist(err) {
				t.Fatalf("capture preceded ACK: %v", err)
			}
			current, err := o.st.Get(context.Background(), sb.ID)
			if err != nil || current.State != types.StateRunning {
				t.Fatalf("durable transition preceded ACK: %v", err)
			}
			var projected string
			if err := o.Range(context.Background(), func(route routesync.RouteEntry) error {
				if route.SandboxID == sb.ID {
					projected = route.State
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if projected != routesync.StatePausing {
				t.Fatalf("new subscription bypasses gate: %s", projected)
			}
			barrier.release()
			err = <-done
			rejected := test.failure != nil || (test.ifIdle && test.busy)
			if (err != nil) != rejected {
				t.Fatalf("pause err=%v", err)
			}
			if test.ifIdle && test.busy && !errors.Is(err, api.ErrSandboxBusy) {
				t.Fatal(err)
			}
			current, getErr := o.st.Get(context.Background(), sb.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if rejected && current.State != types.StateRunning {
				t.Fatalf("rejected pause changed state: %s", current.State)
			}
			if !rejected && current.State != types.StatePaused {
				t.Fatalf("accepted pause state: %s", current.State)
			}
		})
	}
}

func TestWakePreservesUnresolvedPausingGate(t *testing.T) {
	o, sb, _, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	ctx := context.Background()
	if changed, err := o.st.SetPausing(ctx, sb.ID, sb.RunID, true); err != nil || !changed {
		t.Fatalf("set pausing: %v, %v", changed, err)
	}
	events, unsubscribe := o.Subscribe()
	defer unsubscribe()
	o.OnWake(ctx, sb.ID)
	select {
	case event := <-events:
		if event.Kind != routesync.TypeUpsert || event.Route.State != routesync.StatePausing {
			t.Fatalf("unresolved pause lost its gate: %+v", event)
		}
	default:
		t.Fatal("missing authoritative pausing route")
	}
}
