package keys

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestMmdsSecretDeterministicAndIsolated(t *testing.T) {
	// Two uniformly-random 32-byte API secrets (hex).
	mkA := hex.EncodeToString(bytes.Repeat([]byte{0xa1}, 32))
	mkB := hex.EncodeToString(bytes.Repeat([]byte{0xb2}, 32))

	s1 := MmdsSecret(mkA, "sbx-1")
	if len(s1) != 32 {
		t.Fatalf("secret len = %d, want 32", len(s1))
	}
	// Deterministic: the whole point — any worker derives the same key.
	if !bytes.Equal(s1, MmdsSecret(mkA, "sbx-1")) {
		t.Fatal("MmdsSecret not deterministic for the same (key, sid)")
	}
	// Distinct sandbox -> distinct secret (same tenant key).
	if bytes.Equal(s1, MmdsSecret(mkA, "sbx-2")) {
		t.Fatal("different sandbox ids share a secret")
	}
	// Distinct tenant key -> distinct secret (same sid).
	if bytes.Equal(s1, MmdsSecret(mkB, "sbx-1")) {
		t.Fatal("different API secrets share a secret")
	}
}

func TestMmdsSecretEmptyOrBadKey(t *testing.T) {
	if MmdsSecret("", "sbx-1") != nil {
		t.Fatal("empty API secret should yield nil secret")
	}
	if MmdsSecret("nothex!!", "sbx-1") != nil {
		t.Fatal("non-hex API secret should yield nil secret")
	}
	if MmdsSecret("ab", "sbx-1") != nil {
		t.Fatal("short API secret should yield nil secret")
	}
}

func TestMmdsSecretPreservesDirectHMACProtocol(t *testing.T) {
	root := bytes.Repeat([]byte{0x13}, 32)
	mac := hmac.New(sha256.New, root)
	_, _ = mac.Write([]byte("kuasar-mmds-v1:node-local-sandbox"))
	if !bytes.Equal(MmdsSecret(hex.EncodeToString(root), "node-local-sandbox"), mac.Sum(nil)) {
		t.Fatal("MMDS derivation changed its direct HMAC algorithm or domain")
	}
}
