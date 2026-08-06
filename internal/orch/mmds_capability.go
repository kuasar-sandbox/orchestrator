package orch

// MMDSRuntimeAvailable reports whether this process currently has a serving
// path for MMDS traffic. It is runtime state, not desired configuration:
// internal mode becomes available only after its listener binds, while
// external mode follows the external proxy master's live registration lease.
func (o *Orchestrator) MMDSRuntimeAvailable() bool {
	o.mmdsCapabilityMu.RLock()
	defer o.mmdsCapabilityMu.RUnlock()
	return o.mmdsRuntimeAvailable
}

// SetMMDSRuntimeAvailable updates the actual MMDS serving capability. Desired
// configuration remains a necessary condition, so a proxy cannot advertise
// MMDS for a conductor that has MMDS disabled.
func (o *Orchestrator) SetMMDSRuntimeAvailable(available bool) {
	available = available && o.cfg != nil && o.cfg.MMDS.Enabled
	o.mmdsCapabilityMu.Lock()
	o.mmdsRuntimeAvailable = available
	o.mmdsCapabilityMu.Unlock()
}
