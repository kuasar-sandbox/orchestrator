package orch

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdscfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// ValidatePersistedMMDSEndpoints re-validates every already-persisted MMDS
// endpoint declaration against the current node policy and confirms every
// present value still decrypts. At startup, enabled mode validates every
// persisted definition against current limits and reserved prefixes; any
// conflict or undecryptable present value fails startup with a non-secret
// diagnostic rather than truncating, grandfathering, or silently dropping
// data. Disabled (mmds.endpoints.enabled=false) skips this entirely —
// existing endpoint ciphertext remains persisted but is neither loaded nor
// served until re-enabled, so disabling the feature is always a safe way to
// bring the conductor back up if a startup validation failure blocks it.
//
// Scope is deliberately narrower than "every limit in MMDSEndpointsConfig":
// max_endpoints_per_sandbox is a Create-time, per-sandbox declaration count
// that can only be checked here (the conductor is the sole source of
// truth). max_metadata_bytes is NOT checked here — it bounds the raw
// Create-time YAML blob, which is never persisted in structured storage, so
// there is nothing left to re-check post-Create. Node-wide endpoint count
// is not a conductor concern at all (it has no in-memory footprint to
// bound the way external mode's proxyendpoints.Table does — see that
// package's own, hardcoded, non-operator-configurable safety net).
//
// store.RangeMMDSEndpoints already attempts decryption for every row's
// present value and returns a wrapped, non-secret error (sandbox_id + name,
// never plaintext/ciphertext) the moment one fails — this reuses that
// existing, already-tested code path rather than duplicating it.
func (o *Orchestrator) ValidatePersistedMMDSEndpoints(ctx context.Context) error {
	if !o.cfg.MMDS.Endpoints.Enabled {
		return nil
	}
	limits := o.cfg.MMDS.Endpoints
	perSandbox := map[string]int{}
	err := o.st.RangeMMDSEndpoints(ctx, func(e store.MMDSEndpointFull) error {
		if verr := mmdscfg.ValidateNameAndPath(e.Name, e.Path, limits); verr != nil {
			return fmt.Errorf("sandbox %s endpoint %q: %w", e.SandboxID, e.Name, verr)
		}
		perSandbox[e.SandboxID]++
		if limits.MaxEndpointsPerSandbox > 0 && perSandbox[e.SandboxID] > limits.MaxEndpointsPerSandbox {
			return fmt.Errorf("sandbox %s: more than max_endpoints_per_sandbox (%d) declared endpoints", e.SandboxID, limits.MaxEndpointsPerSandbox)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("mmds startup validation: %w", err)
	}
	return nil
}
