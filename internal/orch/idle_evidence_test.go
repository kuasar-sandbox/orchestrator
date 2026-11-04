package orch

import (
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"testing"
	"time"
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
			a := routesync.PauseAck{Evidence: []routesync.IdleEvidence{{SinceUTC: wall.Add(time.Duration(tc.since - 100)), SinceBootNS: tc.since, Clock: p}}}
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
