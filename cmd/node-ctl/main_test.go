package main

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestClusterNodeCapabilitiesFollowDataPlaneMode(t *testing.T) {
	for _, mode := range []string{config.ProxyInternal, config.ProxyExternal} {
		capabilities := clusterNodeCapabilities(mode)
		if !capabilities["sandbox"] || !capabilities["build"] {
			t.Fatalf("capabilities for %q = %#v", mode, capabilities)
		}
	}
	capabilities := clusterNodeCapabilities(config.ProxyOff)
	if _, found := capabilities["sandbox"]; found || !capabilities["build"] {
		t.Fatalf("capabilities for proxy off = %#v", capabilities)
	}
}

func TestEffectiveSandboxQueueLimitUsesAdmissionAuthority(t *testing.T) {
	if got := effectiveSandboxQueueLimit(17, nil); got != 17 {
		t.Fatalf("configured queue limit = %d", got)
	}
	if got := effectiveSandboxQueueLimit(17, &resourceRuntime{queueMax: 23}); got != 23 {
		t.Fatalf("resource-controller queue limit = %d", got)
	}
}
