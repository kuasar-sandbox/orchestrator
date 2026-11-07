package configresolve

import (
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
)

func TestEncryptionKeySpecEnvironmentOverridesYAMLOnlyAtRuntimeResolution(t *testing.T) {
	cfg := &publicconfig.Conductor{EncryptionKey: "yaml-key"}
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	if got := EncryptionKeySpec(cfg); got != "yaml-key" {
		t.Fatalf("YAML key = %q", got)
	}
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "environment-key")
	if got := EncryptionKeySpec(cfg); got != "environment-key" {
		t.Fatalf("environment override = %q", got)
	}
}

func TestDiffCOWPolicyAdaptersPreservePresenceAndOwnership(t *testing.T) {
	cache, dirty, discard := "128MiB", "32MiB", "manual"
	in := publicconfig.ResourcesConfig{DiffCOW: &publicconfig.ResourceDiffCOW{CacheSize: &cache, MaxDirtySize: &dirty, Discard: &discard}}
	policy := SandboxResources(in)
	out := PublicSandboxResources(policy)
	if *out.DiffCOW.CacheSize != cache || *out.DiffCOW.MaxDirtySize != dirty || *out.DiffCOW.Discard != discard ||
		out.DiffCOW.CacheSize == policy.DiffCOW.CacheSize || out.DiffCOW.MaxDirtySize == policy.DiffCOW.MaxDirtySize ||
		out.DiffCOW.Discard == policy.DiffCOW.Discard ||
		policy.DiffCOW.CacheSize == &cache || policy.DiffCOW.MaxDirtySize == &dirty || policy.DiffCOW.Discard == &discard {
		t.Fatalf("adapter lost or shared leaves: %+v", out.DiffCOW)
	}
	in.DiffCOW.MaxDirtySize = nil
	if got := MaterializedSandboxResources(in).DiffCOW; got == nil || got.MaxDirtySize != nil || *got.CacheSize != cache || *got.Discard != discard {
		t.Fatalf("diagnostic policy = %+v", got)
	}
	in.DiffCOW.CacheSize = nil
	in.DiffCOW.Discard = nil
	if got := MaterializedSandboxResources(in).DiffCOW; got == nil || got.CacheSize != nil || got.MaxDirtySize != nil || got.Discard != nil {
		t.Fatalf("empty mapping = %+v", got)
	}
	if got := MaterializedSandboxResources(publicconfig.ResourcesConfig{}).DiffCOW; got != nil {
		t.Fatalf("omitted mapping = %+v", got)
	}
}
