package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestSecretPluginManifestRootOnly(t *testing.T) {
	raw := bytes.Repeat([]byte{0xaa}, 32)
	broker := testsecret.Start(t, map[string][]byte{"tenant/Manifest_v1": raw})
	ref := "@test/tenant/Manifest_v1"
	resolved, err := resolveRootSecret(t.Context(), ref, "MANIFEST_KEY")
	if err != nil || !bytes.Equal(apikey.DeriveAPISecret(resolved), apikey.DeriveAPISecret(raw)) {
		t.Fatalf("plugin root changed default APISecret derivation: %v", err)
	}
	if _, err := resolveRootSecret(t.Context(), ref, "API_SECRET"); err == nil {
		t.Fatal("APISecret unexpectedly accepted a plugin reference")
	}
	if broker.Count("tenant/Manifest_v1") != 1 {
		t.Fatal("APISecret reference must not be resolved")
	}
}

func TestNativeRootValidation(t *testing.T) {
	for _, purpose := range []string{"API_SECRET", "MANIFEST_KEY"} {
		for _, text := range []string{"", "short", strings.Repeat("A", 64), strings.Repeat("z", 64)} {
			if _, err := resolveRootSecret(t.Context(), text, purpose); err == nil {
				t.Fatalf("%s accepted noncanonical root", purpose)
			}
		}
		got, err := resolveRootSecret(t.Context(), strings.Repeat("a", 64), purpose)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0xaa}, 32)) {
			t.Fatalf("%s native decoding: %v", purpose, err)
		}
	}
}
