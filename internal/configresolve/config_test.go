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
