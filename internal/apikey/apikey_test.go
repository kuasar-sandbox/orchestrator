package apikey

import (
	"crypto/hmac"
	"crypto/rand"
	"regexp"
	"strings"
	"testing"
)

func randKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestMintVerifyRoundtrip(t *testing.T) {
	authKey := randKey(t)
	ak, err := Mint(authKey)
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
	if !Verify(p, authKey) {
		t.Fatal("Verify rejected a genuine api key")
	}
	if !hmac.Equal(p.FP, Fingerprint(authKey)) {
		t.Fatal("embedded fp != Fingerprint(AuthKey)")
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	authKey := randKey(t)
	ak, _ := Mint(authKey)
	p, _ := Parse(ak)
	if Verify(p, randKey(t)) {
		t.Fatal("Verify accepted a different AuthKey")
	}
}

func TestVerifyRejectsTamperedMAC(t *testing.T) {
	mk := randKey(t)
	ak, _ := Mint(mk)
	p, _ := Parse(ak)
	p.MAC[0] ^= 0xff
	if Verify(p, mk) {
		t.Fatal("Verify accepted a tampered MAC")
	}
}

func TestParseBadFormat(t *testing.T) {
	for _, s := range []string{"", "nope", "e2b_", "e2b_!!!notb64", "abc123", "e2b_AAAA"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) = nil err, want ErrBadFormat", s)
		}
	}
}

func TestEachMintDiffers(t *testing.T) {
	mk := randKey(t)
	a, _ := Mint(mk)
	b, _ := Mint(mk)
	if a == b {
		t.Fatal("two mints of the same key produced identical tokens (nonce not applied?)")
	}
	// ...but both verify.
	for _, ak := range []string{a, b} {
		p, err := Parse(ak)
		if err != nil || !Verify(p, mk) {
			t.Fatalf("minted key %q failed verify", ak)
		}
	}
}
