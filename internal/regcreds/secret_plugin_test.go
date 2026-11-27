package regcreds

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestPullTokenKeyGoldenVector(t *testing.T) {
	key, err := sealKey(t.Context(), strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(key) != "a0d399aca5123b10ed4e5c13a8289b3a35427313673ff2f4211e9a2a5fd31afb" {
		t.Fatal("pull-token key no longer uses the canonical hex text protocol")
	}
}

func TestSecretPluginPullTokenCompatibility(t *testing.T) {
	raw := bytes.Repeat([]byte{0xaa}, 32)
	broker := testsecret.Start(t, map[string][]byte{"tenant/Manifest_v1": raw})
	ref, native := "@test/tenant/Manifest_v1", hex.EncodeToString(raw)
	creds := Creds{Username: "tenant-user", Password: "registry-secret"}
	legacy, err := Seal(native, creds)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := OpenContext(t.Context(), ref, legacy); err != nil || got != creds {
		t.Fatalf("cannot open a legacy token with the same plugin root: %v", err)
	}
	token, err := SealContext(t.Context(), ref, creds)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Open(native, token); err != nil || got != creds {
		t.Fatalf("plugin token is incompatible with native root: %v", err)
	}
	broker.Set("tenant/Manifest_v1", nil)
	t.Setenv("MANIFEST_KEY", native)
	if _, err := OpenContext(t.Context(), ref, legacy); err == nil {
		t.Fatal("unavailable plugin key fell back to ambient MANIFEST_KEY")
	}
}

func TestPullTokenResolutionContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := SealContext(ctx, strings.Repeat("a", 64), Creds{Token: "registry-token"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled pull-token operation: %v", err)
	}
}
