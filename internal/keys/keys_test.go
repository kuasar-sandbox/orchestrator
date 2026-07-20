package keys

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestMmdsSecretDeterministicAndIsolated(t *testing.T) {
	// Two uniformly-random 32-byte ManifestKeys (hex).
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
		t.Fatal("different manifest keys share a secret")
	}
}

func TestMmdsSecretEmptyOrBadKey(t *testing.T) {
	if MmdsSecret("", "sbx-1") != nil {
		t.Fatal("empty manifest key should yield nil secret")
	}
	if MmdsSecret("nothex!!", "sbx-1") != nil {
		t.Fatal("non-hex manifest key should yield nil secret")
	}
}
