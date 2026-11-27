package migrationtoken

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestSecretPluginTokenCompatibility(t *testing.T) {
	raw, err := hex.DecodeString(testManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	broker := testsecret.Start(t, map[string][]byte{"tenant/Manifest_v1": raw})
	material := KeyMaterial{APISecret: testAPISecret, ManifestKey: "@test/tenant/Manifest_v1"}
	nonce := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

	t.Run("migration wire and fingerprints", func(t *testing.T) {
		before := broker.Count("tenant/Manifest_v1")
		key, apiFP, manifestFP, err := deriveMaterialContext(t.Context(), material)
		if err != nil || hex.EncodeToString(key) != testMigrationKey || apiFP != testAPIFingerprint || manifestFP != testManifestFingerprint {
			t.Fatalf("reference changed existing key/fingerprints: %v", err)
		}
		if broker.Count("tenant/Manifest_v1") != before+1 {
			t.Fatal("one material calculation must resolve the root once")
		}
		token, err := sealContext(t.Context(), bytes.NewReader(nonce), material, validPayload())
		if err != nil {
			t.Fatal(err)
		}
		legacy, err := seal(bytes.NewReader(nonce), testMaterial, validPayload())
		if err != nil || token != legacy {
			t.Fatalf("plugin root changed migration wire bytes: %v", err)
		}
		for _, m := range []KeyMaterial{material, testMaterial} {
			payload, err := OpenContext(t.Context(), m, token)
			if err != nil {
				t.Fatal(err)
			}
			assertPayloadEqual(t, payload, validPayload())
		}
	})

	t.Run("workspace wire", func(t *testing.T) {
		size := int64(4096)
		disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64)}
		token, err := sealWorkspaceContext(t.Context(), bytes.NewReader(nonce), material, disk)
		if err != nil {
			t.Fatal(err)
		}
		legacy, err := sealWorkspace(bytes.NewReader(nonce), testMaterial, disk)
		if err != nil || token != legacy {
			t.Fatalf("plugin root changed workspace wire bytes: %v", err)
		}
		for _, m := range []KeyMaterial{material, testMaterial} {
			got, err := OpenWorkspaceContext(t.Context(), m, token)
			if err != nil || !reflect.DeepEqual(got, disk) {
				t.Fatalf("workspace disk mismatch: %+v, %v", got, err)
			}
		}
	})

	t.Run("APISecret remains canonical hex", func(t *testing.T) {
		before := broker.Count("tenant/Manifest_v1")
		bad := KeyMaterial{APISecret: material.ManifestKey, ManifestKey: material.ManifestKey}
		if _, err := SealContext(t.Context(), bad, validPayload()); !errors.Is(err, ErrInvalidKeyMaterial) {
			t.Fatalf("APISecret accepted a reference: %v", err)
		}
		if broker.Count("tenant/Manifest_v1") != before {
			t.Fatal("invalid APISecret should not resolve plugin material")
		}
	})

	t.Run("wrong or unavailable root does not fall back", func(t *testing.T) {
		token := fixedToken(t, validPayload())
		t.Setenv("MANIFEST_KEY", testManifestKey)
		broker.Set("tenant/Manifest_v1", bytes.Repeat([]byte{3}, 32))
		if _, err := OpenContext(t.Context(), material, token); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("different resolved root did not fail authentication: %v", err)
		}
		broker.Set("tenant/Manifest_v1", []byte("short"))
		if _, err := OpenContext(t.Context(), material, token); !errors.Is(err, ErrInvalidKeyMaterial) {
			t.Fatalf("short resolved root accepted: %v", err)
		}
		broker.Set("tenant/Manifest_v1", nil)
		if _, err := OpenContext(t.Context(), material, token); !errors.Is(err, ErrInvalidKeyMaterial) {
			t.Fatalf("missing root did not return a key error: %v", err)
		}
	})
}

func TestTokenResolutionContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := SealContext(ctx, testMaterial, validPayload()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled migration operation: %v", err)
	}
	size := int64(4096)
	disk := rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &size, Base: "manifest://" + strings.Repeat("a", 64)}
	if _, err := SealWorkspaceContext(ctx, testMaterial, disk); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled workspace operation: %v", err)
	}
}
