package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func checkIdleEvidence(a routesync.PauseAck, r sandboxcfg.CaptureRequest) error {
	if a.Error != "" || len(a.Evidence) == 0 {
		return api.ErrProxyUnavailable
	}
	if !a.Idle() {
		return api.ErrSandboxBusy
	}
	for _, e := range a.Evidence {
		if e.SinceBootNS <= 0 {
			return api.ErrProxyUnavailable
		}
		if r.IdleSinceBootNS > 0 {
			if e.SinceBootNS > r.IdleSinceBootNS {
				return api.ErrSandboxBusy
			}
			continue
		}
		ok, known := e.Clock.Compare(idleclock.Point{Wall: e.SinceUTC, BootNS: e.SinceBootNS}, *r.IfIdleSince)
		if !known {
			return api.ErrProxyUnavailable
		}
		if !ok {
			return api.ErrSandboxBusy
		}
	}
	return nil
}
