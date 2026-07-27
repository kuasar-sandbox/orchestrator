package apikey

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"
)

func randSecret(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestMintVerifyRoundtrip(t *testing.T) {
	manifestKey := randSecret(t)
	apiSecret := DeriveAPISecret(manifestKey)
	ak, err := Mint(apiSecret)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ak, "e2b_") {
		t.Fatalf("api key %q missing e2b_ prefix", ak)
	}
	if len(ak) != 76 { // e2b_(4) + hex(36 bytes)=72
		t.Fatalf("api key len=%d, want 76: %q", len(ak), ak)
	}
	// The e2b SDK validates against /^e2b_[0-9a-f]+$/ — lock that in.
	if !regexp.MustCompile(`^e2b_[0-9a-f]+$`).MatchString(ak) {
		t.Fatalf("api key %q is not e2b_<lowercase-hex> (e2b SDK rejects it)", ak)
	}
	p, err := Parse(ak)
	if err != nil {
		t.Fatal(err)
	}
	if !Verify(p, apiSecret) {
		t.Fatal("Verify rejected a genuine api key")
	}
	if !hmac.Equal(p.CandidateFingerprint, CandidateFingerprint(apiSecret)) {
		t.Fatal("embedded fingerprint != CandidateFingerprint(apiSecret)")
	}
	if Verify(p, manifestKey) {
		t.Fatal("Verify accepted the manifest key instead of the derived API secret")
	}
}

func TestDeriveAPISecretGoldenVector(t *testing.T) {
	manifestKey := make([]byte, 32)
	for i := range manifestKey {
		manifestKey[i] = byte(i)
	}

	want, err := hex.DecodeString("9fb638c22d854bdb6497e92caf7763ee26f99561fc436423b5fc448cb405d8be")
	if err != nil {
		t.Fatal(err)
	}
	got := DeriveAPISecret(manifestKey)
	if !bytes.Equal(got, want) {
		t.Fatalf("DeriveAPISecret() = %x, want %x", got, want)
	}
	if len(got) != FullFingerprintLen {
		t.Fatalf("derived API secret len=%d, want %d", len(got), FullFingerprintLen)
	}
}

func TestFingerprints(t *testing.T) {
	apiSecret := randSecret(t)
	candidate := CandidateFingerprint(apiSecret)
	full := FullFingerprint(apiSecret)

	if len(candidate) != CandidateFingerprintLen {
		t.Fatalf("candidate fingerprint len=%d, want %d", len(candidate), CandidateFingerprintLen)
	}
	if len(full) != FullFingerprintLen {
		t.Fatalf("full fingerprint len=%d, want %d", len(full), FullFingerprintLen)
	}
	if !bytes.Equal(candidate, full[:CandidateFingerprintLen]) {
		t.Fatalf("candidate fingerprint %x is not the prefix of full fingerprint %x", candidate, full)
	}
}

func TestVerifyRejectsWrongAPISecret(t *testing.T) {
	apiSecret := randSecret(t)
	ak, _ := Mint(apiSecret)
	p, _ := Parse(ak)
	if Verify(p, randSecret(t)) {
		t.Fatal("Verify accepted a different API secret")
	}
}

func TestVerifyRejectsTamperedMAC(t *testing.T) {
	apiSecret := randSecret(t)
	ak, _ := Mint(apiSecret)
	p, _ := Parse(ak)
	p.MAC[0] ^= 0xff
	if Verify(p, apiSecret) {
		t.Fatal("Verify accepted a tampered MAC")
	}
}

func TestParseBadFormat(t *testing.T) {
	for _, s := range []string{
		"", "nope", "e2b_", "e2b_!!!notb64", "abc123", "e2b_AAAA",
		"e2b_" + strings.Repeat("AA", payloadLen),
	} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) = nil err, want ErrBadFormat", s)
		}
	}
}

func TestEachMintDiffers(t *testing.T) {
	apiSecret := randSecret(t)
	a, _ := Mint(apiSecret)
	b, _ := Mint(apiSecret)
	if a == b {
		t.Fatal("two mints of the same key produced identical tokens (nonce not applied?)")
	}
	// ...but both verify.
	for _, ak := range []string{a, b} {
		p, err := Parse(ak)
		if err != nil || !Verify(p, apiSecret) {
			t.Fatalf("minted key %q failed verify", ak)
		}
	}
}
