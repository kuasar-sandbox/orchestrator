package builder

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/secret"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestManifestPluginPublicationBindsResolvedMaterial(t *testing.T) {
	if !testsecret.InSubprocess(t) {
		return
	}
	fixture := newPublicationFixture(t)
	broker := testsecret.Start(t, map[string][]byte{"build/root-v1": fixture.key[:]})
	// The authenticated task carrier must override both manifest.yaml and an
	// unrelated inherited process environment. It must remain a reference.
	t.Setenv(manifest.CustomerKeyEnv, strings.Repeat("7", 64))
	fixture.keyHex = "@test/build/root-v1"
	pipeline := fixture.pipeline(t, "plugin-build", fixture.localImageRef, nil, true)
	pipeline.spec.Env[secret.Env] = "test::/run/host-broker.sock"
	for _, entry := range pipeline.tenantEnv() {
		if strings.HasPrefix(entry, secret.Env+"=") || strings.HasPrefix(entry, manifest.CustomerKeyEnv+"=") {
			t.Fatal("host Secret plugin configuration was exported to the guest")
		}
	}
	if pipeline.spec.Env[manifest.CustomerKeyEnv] != fixture.keyHex || pipeline.artifacts.customerKey != fixture.key || broker.Count("build/root-v1") != 1 {
		t.Fatal("publication preparation did not bind the authoritative task reference once")
	}
	// All reads and writes within this publication reuse that root even when
	// the external broker becomes unavailable after preparation.
	broker.Set("build/root-v1", nil)
	ref, err := pipeline.publishImageTarget()
	if err != nil || ref == "" {
		t.Fatalf("publish using bound plugin material: %v", err)
	}
	if broker.Count("build/root-v1") != 1 {
		t.Fatal("one publication reacquired ManifestKey after binding")
	}
}
