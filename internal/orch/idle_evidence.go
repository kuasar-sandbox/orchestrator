package orch

import (
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func checkIdleEvidence(a routesync.PauseAck, r sandboxcfg.CaptureRequest) error {
	if a.Error != "" || len(a.Evidence) == 0 {
		return api.ErrProxyUnavailable
	}
	if r.IdleSinceBootNS > 0 && !a.AutomaticSafe {
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
		if len(e.Points) == 0 {
			return api.ErrProxyUnavailable
		}
		for _, point := range e.Points {
			ok, known := e.Clock.Compare(point, *r.IfIdleSince)
			if !known {
				return api.ErrProxyUnavailable
			}
			if !ok {
				return api.ErrSandboxBusy
			}
		}
	}
	return nil
}
