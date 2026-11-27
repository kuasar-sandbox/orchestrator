package placer

import (
	"encoding/hex"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestManifestPluginImport(t *testing.T) {
	if !testsecret.InSubprocess(t) {
		return
	}
	material, err := hex.DecodeString(testManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	var raw [32]byte
	copy(raw[:], material)
	broker := testsecret.Start(t, map[string][]byte{"Tenant/Manifest_v1": material})
	const reference = "@test/Tenant/Manifest_v1"
	record := clusterstate.SandboxGroupRecord{Group: "/group", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: reference}}
	// Pure source normalization carries text without depending on plugin reachability.
	before := broker.Count("Tenant/Manifest_v1")
	normalized, err := normalizeManifestKey(record.ManifestKey)
	if err != nil || normalized.Value != reference || broker.Count("Tenant/Manifest_v1") != before {
		t.Fatalf("source normalization resolved or changed reference: %v", err)
	}
	cmd, err := importCommandContext(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.KeyPut == nil || cmd.KeyPut.ManifestKey != reference || cmd.KeyPut.ManifestKeyFingerprint != manifestkey.Fingerprint(raw) || cmd.KeyPut.APISecret != hex.EncodeToString(apikey.DeriveAPISecret(material)) {
		t.Fatal("import did not bind the reference, fingerprint and default API secret to the same material")
	}
	if broker.Count("Tenant/Manifest_v1") != before+1 {
		t.Fatal("import resolved one root more than once")
	}

	// Exercise the real wire JSON and node SQLite transaction; the runtime-only
	// fingerprint does not cross the wire, so the node must resolve independently.
	f := newImportNode(t)
	literal := cmd
	literalPut := *cmd.KeyPut
	literalPut.ManifestKey = testManifestKey
	literal.KeyPut = &literalPut
	first := f.maintain(t, literal, false)
	if first.Status != "committed" {
		t.Fatalf("initial literal import: %+v", first)
	}
	result := f.maintain(t, cmd, false)
	if result.Status != "committed" || !result.KeyLeaseConfirmed || result.Version <= first.Version || len(f.puts) != 2 {
		t.Fatalf("import transaction: %+v", result)
	}
	_, pair, err := f.st.GroupAdmit(t.Context(), cmd.APIKey, "/group", result.Version)
	if err != nil || pair.ManifestKey != reference || pair.ManifestKeyFingerprint != manifestkey.Fingerprint(raw) {
		t.Fatalf("stored import reference: %v", err)
	}
	before = broker.Count("Tenant/Manifest_v1")
	broker.Set("Tenant/Manifest_v1", nil)
	unchanged := f.maintain(t, cmd, false)
	if unchanged.Status != "unchanged" || broker.Count("Tenant/Manifest_v1") != before {
		t.Fatalf("confirmed import projection contacted broker: %+v", unchanged)
	}
}
