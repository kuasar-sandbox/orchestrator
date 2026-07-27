package keys

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

const (
	testAPISecret     = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testAuthSandboxID = "sandbox-01"
	testServiceSecret = "0213051156fc06b40aebdeb333caeb9c95866d91f08298264901787551897989"
	testForwardToken  = "kat1.eyJ2IjoxLCJzaWQiOiJzYW5kYm94LTAxIiwiYXVkIjoiZm9yd2FyZCJ9.uk154F30--e531Hogd4pgj0oGFsuIJJT0nWr17gkdSw"
)

func TestMintSecretCanonicalHex(t *testing.T) {
	for range 2 {
		secret, err := MintSecret()
		if err != nil {
			t.Fatal(err)
		}
		if len(secret) != 64 {
			t.Fatalf("MintSecret length = %d, want 64", len(secret))
		}
		raw, err := hex.DecodeString(secret)
		if err != nil || len(raw) != 32 || secret != strings.ToLower(secret) {
			t.Fatalf("MintSecret returned non-canonical 32-byte hex")
		}
	}

	token, err := MintToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCanonicalHex32(token, errInvalidServiceSecret); err != nil {
		t.Fatal("MintToken no longer uses canonical 32-byte hex")
	}
}

func TestDeriveServiceSecretGoldenVector(t *testing.T) {
	got, err := DeriveServiceSecret(testAPISecret, testAuthSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if got != testServiceSecret {
		t.Fatalf("DeriveServiceSecret() = %q, want golden vector", got)
	}

	again, err := DeriveServiceSecret(testAPISecret, testAuthSandboxID)
	if err != nil || again != got {
		t.Fatal("DeriveServiceSecret is not deterministic")
	}
	other, err := DeriveServiceSecret(testAPISecret, testAuthSandboxID+"-other")
	if err != nil {
		t.Fatal(err)
	}
	if other == got {
		t.Fatal("different AuthSandboxID values derived the same ServiceSecret")
	}
}

func TestDeriveServiceSecretRejectsNonCanonicalInputs(t *testing.T) {
	badSecrets := []string{
		"",
		testAPISecret[:62],
		testAPISecret + "00",
		strings.ToUpper(testAPISecret),
		strings.Repeat("z", 64),
	}
	for _, secret := range badSecrets {
		if _, err := DeriveServiceSecret(secret, testAuthSandboxID); !errors.Is(err, errInvalidAPISecret) {
			t.Errorf("DeriveServiceSecret(bad APISecret) error = %v, want fixed invalid-secret error", err)
		}
	}
	for _, sid := range []string{"", string([]byte{0xff})} {
		if _, err := DeriveServiceSecret(testAPISecret, sid); !errors.Is(err, errInvalidAuthSandboxID) {
			t.Errorf("DeriveServiceSecret(bad SID) error = %v, want fixed invalid-SID error", err)
		}
	}
}

func TestForwardAccessTokenGoldenVector(t *testing.T) {
	token, err := MintForwardAccessToken(testServiceSecret, testAuthSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if token != testForwardToken {
		t.Fatalf("MintForwardAccessToken() = %q, want golden vector", token)
	}
	if err := VerifyForwardAccessToken(token, testServiceSecret, testAuthSandboxID); err != nil {
		t.Fatalf("VerifyForwardAccessToken(golden) = %v", err)
	}

	again, err := MintForwardAccessToken(testServiceSecret, testAuthSandboxID)
	if err != nil || again != token {
		t.Fatal("forward token is not deterministic for the same ServiceSecret and SID")
	}
}

func TestForwardAccessTokenStrictWire(t *testing.T) {
	parts := strings.Split(testForwardToken, ".")
	if len(parts) != 3 {
		t.Fatal("bad test golden token")
	}

	nonCanonicalSignature := nonCanonicalRawURL(t, parts[2])
	tests := map[string]string{
		"empty":                   "",
		"two segments":            parts[0] + "." + parts[1],
		"four segments":           testForwardToken + ".extra",
		"wrong prefix":            "KAT1." + parts[1] + "." + parts[2],
		"empty payload":           "kat1.." + parts[2],
		"empty signature":         "kat1." + parts[1] + ".",
		"padded payload":          "kat1." + parts[1] + "=." + parts[2],
		"padded signature":        testForwardToken + "=",
		"non-canonical signature": "kat1." + parts[1] + "." + nonCanonicalSignature,
		"short signature":         "kat1." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 31)),
		"long signature":          "kat1." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 33)),
		"standard alphabet":       strings.Replace(testForwardToken, "-", "+", 1),
		"tampered signature":      testForwardToken[:len(testForwardToken)-2] + "AA",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			if err := VerifyForwardAccessToken(token, testServiceSecret, testAuthSandboxID); !errors.Is(err, errInvalidForwardAccessToken) {
				t.Fatalf("VerifyForwardAccessToken() error = %v, want fixed invalid-token error", err)
			}
		})
	}
}

func TestForwardAccessTokenRequiresCanonicalClaims(t *testing.T) {
	payloads := map[string]string{
		"field order":    `{"sid":"sandbox-01","v":1,"aud":"forward"}`,
		"whitespace":     `{"v": 1,"sid":"sandbox-01","aud":"forward"}`,
		"escaped SID":    `{"v":1,"sid":"sand\u0062ox-01","aud":"forward"}`,
		"duplicate":      `{"v":1,"sid":"sandbox-01","sid":"sandbox-01","aud":"forward"}`,
		"extra":          `{"v":1,"sid":"sandbox-01","aud":"forward","extra":true}`,
		"missing":        `{"v":1,"sid":"sandbox-01"}`,
		"wrong version":  `{"v":2,"sid":"sandbox-01","aud":"forward"}`,
		"wrong SID":      `{"v":1,"sid":"sandbox-02","aud":"forward"}`,
		"empty SID":      `{"v":1,"sid":"","aud":"forward"}`,
		"wrong audience": `{"v":1,"sid":"sandbox-01","aud":"exec"}`,
		"trailing value": `{"v":1,"sid":"sandbox-01","aud":"forward"}{}`,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			token := signedRawPayload(t, []byte(payload), testServiceSecret)
			if err := VerifyForwardAccessToken(token, testServiceSecret, testAuthSandboxID); !errors.Is(err, errInvalidForwardAccessToken) {
				t.Fatalf("VerifyForwardAccessToken() error = %v, want fixed invalid-token error", err)
			}
		})
	}
}

func TestForwardAccessTokenRejectsNonCanonicalPayloadBase64(t *testing.T) {
	const sid = "ss"
	token, err := MintForwardAccessToken(testServiceSecret, sid)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	parts[1] = nonCanonicalRawURL(t, parts[1])
	parts[2] = signatureForPayloadB64(t, parts[1], testServiceSecret)
	token = strings.Join(parts, ".")

	if err := VerifyForwardAccessToken(token, testServiceSecret, sid); !errors.Is(err, errInvalidForwardAccessToken) {
		t.Fatalf("VerifyForwardAccessToken() error = %v, want non-canonical payload rejection", err)
	}
}

func TestForwardAccessTokenBindingAndInputValidation(t *testing.T) {
	otherSecret := strings.Repeat("a", 64)
	if err := VerifyForwardAccessToken(testForwardToken, otherSecret, testAuthSandboxID); !errors.Is(err, errInvalidForwardAccessToken) {
		t.Fatalf("wrong ServiceSecret error = %v, want invalid token", err)
	}
	if err := VerifyForwardAccessToken(testForwardToken, testServiceSecret, "sandbox-02"); !errors.Is(err, errInvalidForwardAccessToken) {
		t.Fatalf("wrong AuthSandboxID error = %v, want invalid token", err)
	}

	for _, badSecret := range []string{"", strings.ToUpper(testServiceSecret), strings.Repeat("z", 64)} {
		if _, err := MintForwardAccessToken(badSecret, testAuthSandboxID); !errors.Is(err, errInvalidServiceSecret) {
			t.Errorf("MintForwardAccessToken(bad secret) error = %v", err)
		}
		if err := VerifyForwardAccessToken(testForwardToken, badSecret, testAuthSandboxID); !errors.Is(err, errInvalidServiceSecret) {
			t.Errorf("VerifyForwardAccessToken(bad secret) error = %v", err)
		}
	}
	for _, sid := range []string{"", string([]byte{0xff})} {
		if _, err := MintForwardAccessToken(testServiceSecret, sid); !errors.Is(err, errInvalidAuthSandboxID) {
			t.Errorf("MintForwardAccessToken(bad SID) error = %v", err)
		}
		if err := VerifyForwardAccessToken(testForwardToken, testServiceSecret, sid); !errors.Is(err, errInvalidAuthSandboxID) {
			t.Errorf("VerifyForwardAccessToken(bad SID) error = %v", err)
		}
	}
}

func TestKATErrorsDoNotEchoInputs(t *testing.T) {
	badToken := "kat1.secret-token-fragment.bad"
	err := VerifyForwardAccessToken(badToken, testServiceSecret, testAuthSandboxID)
	if err == nil || strings.Contains(err.Error(), badToken) || strings.Contains(err.Error(), testServiceSecret) {
		t.Fatalf("verification error leaks token or secret: %v", err)
	}

	badSecret := strings.Repeat("S", 64)
	_, err = DeriveServiceSecret(badSecret, testAuthSandboxID)
	if err == nil || strings.Contains(err.Error(), badSecret) {
		t.Fatalf("derivation error leaks secret: %v", err)
	}
}

func signedRawPayload(t *testing.T, payload []byte, serviceSecretHex string) string {
	t.Helper()
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	return "kat1." + payloadB64 + "." + signatureForPayloadB64(t, payloadB64, serviceSecretHex)
}

func signatureForPayloadB64(t *testing.T, payloadB64, serviceSecretHex string) string {
	t.Helper()
	serviceSecret, err := hex.DecodeString(serviceSecretHex)
	if err != nil {
		t.Fatal(err)
	}
	signature := signKAT(serviceSecret, "kat1."+payloadB64)
	return base64.RawURLEncoding.EncodeToString(signature)
}

func nonCanonicalRawURL(t *testing.T, canonical string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)%3 == 0 {
		t.Fatalf("test value %q has no unused base64 bits", canonical)
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alphabet, canonical[len(canonical)-1])
	if last < 0 || last+1 >= len(alphabet) {
		t.Fatalf("cannot mutate final base64 character in %q", canonical)
	}
	nonCanonical := canonical[:len(canonical)-1] + string(alphabet[last+1])
	decoded, err := base64.RawURLEncoding.DecodeString(nonCanonical)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("test mutation did not preserve decoded bytes")
	}
	return nonCanonical
}
