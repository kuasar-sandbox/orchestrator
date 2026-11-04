package orch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestIdleEvidenceThreshold(t *testing.T) {
	wall := time.Now()
	p := idleclock.Proof{}
	p.Observe(idleclock.Point{Wall: wall, BootNS: 100})
	p.Observe(idleclock.Point{Wall: wall.Add(100), BootNS: 200})
	x := wall.Add(50)
	for _, tc := range []struct {
		name  string
		since int64
		want  error
	}{{"before", 149, nil}, {"equal", 150, nil}, {"after", 151, api.ErrSandboxBusy}, {"missing", 0, api.ErrProxyUnavailable}} {
		t.Run(tc.name, func(t *testing.T) {
			a := routesync.PauseAck{AutomaticSafe: true, Evidence: []routesync.IdleEvidence{{Points: []idleclock.Point{{Wall: wall.Add(time.Duration(tc.since - 100)), BootNS: tc.since}}, SinceBootNS: tc.since, Clock: p}}}
			for _, r := range []sandboxcfg.CaptureRequest{{IfIdle: true, IfIdleSince: &x}, {IfIdle: true, IdleSinceBootNS: 150}} {
				if err := checkIdleEvidence(a, r); !errors.Is(err, tc.want) {
					t.Fatal(err, tc.want)
				}
			}
		})
	}
	if err := checkIdleEvidence(routesync.PauseAck{}, sandboxcfg.CaptureRequest{IfIdle: true, IfIdleSince: &x}); !errors.Is(err, api.ErrProxyUnavailable) {
		t.Fatal(err)
	}
}

func TestIdleConditionValidatesReceiptBeforeGate(t *testing.T) {
	o, sb, key, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	barrier := &idleTestCoordinator{requests: make(chan *fakePauseBarrier, 1)}
	o.pauseBarriers = barrier
	received := time.Now().Add(-2 * time.Second)
	x := received.Add(time.Second)
	ctx := sandboxcfg.WithPauseReceipt(context.Background(), received)
	for _, request := range []sandboxcfg.CaptureRequest{
		{Kind: types.CaptureSnapshot, IfIdle: true, IfIdleSince: &x},
		{Kind: types.CaptureSnapshot, IfIdleSince: &received},
	} {
		if _, err := o.Pause(ctx, sb.ID, key, request); !errors.Is(err, api.ErrBadRequest) {
			t.Fatal(err)
		}
	}
	select {
	case <-barrier.requests:
		t.Fatal("invalid request installed gate")
	default:
	}
}
