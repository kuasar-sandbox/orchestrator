package keys

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	testAPISecret     = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testAuthSandboxID = "sandbox-01"
	testServiceSecret = "0213051156fc06b40aebdeb333caeb9c95866d91f08298264901787551897989"
	testForwardToken  = "kat1.eyJ2IjoxLCJzaWQiOiJzYW5kYm94LTAxIiwiYXVkIjoiZm9yd2FyZCJ9.uk154F30--e531Hogd4pgj0oGFsuIJJT0nWr17gkdSw"
	testExecSessionID = "01890f35-7b2c-7cc6-98c4-dc0c0c07398f"
	testExecExpiry    = int64(1784835600)
	testExecToken     = "kat1.eyJ2IjoxLCJzZXNzaW9uX2lkIjoiMDE4OTBmMzUtN2IyYy03Y2M2LTk4YzQtZGMwYzBjMDczOThmIiwic2lkIjoic2FuZGJveC0wMSIsImF1ZCI6ImV4ZWMiLCJleHAiOjE3ODQ4MzU2MDB9.Xjxz_Gy7SXW7P6J985sabIi_i0MWjRYCCNsvXZRoVWI"
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

func TestExecAccessTokenGoldenVector(t *testing.T) {
	token, err := mintExecAccessToken(testServiceSecret, testAuthSandboxID, testExecSessionID, testExecExpiry)
	if err != nil {
		t.Fatal(err)
	}
	if token != testExecToken {
		t.Fatalf("mintExecAccessToken() = %q, want golden vector", token)
	}
	if err := VerifyExecAccessToken(token, testServiceSecret, testAuthSandboxID, time.Unix(testExecExpiry-1, 0)); err != nil {
		t.Fatalf("VerifyExecAccessToken(golden) = %v", err)
	}
}

func TestMintExecAccessTokenUsesUUIDv7AndOptionalExpiry(t *testing.T) {
	tokens := make(map[string]bool)
	for _, expiry := range []int64{0, testExecExpiry} {
		token, err := MintExecAccessToken(testServiceSecret, testAuthSandboxID, expiry)
		if err != nil {
			t.Fatal(err)
		}
		if tokens[token] {
			t.Fatal("two exec sessions returned the same token")
		}
		tokens[token] = true
		claims := execClaimsFromToken(t, token)
		if claims.Version != 1 || claims.SID != testAuthSandboxID || claims.Audience != execAudience ||
			!validUUIDv7(claims.SessionID) {
			t.Fatalf("minted exec claims = %+v", claims)
		}
		if expiry == 0 && claims.Expires != nil {
			t.Fatalf("long-lived token contains exp=%v", *claims.Expires)
		}
		if expiry > 0 && (claims.Expires == nil || *claims.Expires != expiry) {
			t.Fatalf("expiring token claims = %+v", claims)
		}
	}
}

func TestExecAccessTokenStrictWire(t *testing.T) {
	token, err := mintExecAccessToken(testServiceSecret, testAuthSandboxID, testExecSessionID, 0)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	nonCanonicalSignature := nonCanonicalRawURL(t, parts[2])
	tests := map[string]string{
		"empty":                   "",
		"two segments":            parts[0] + "." + parts[1],
		"four segments":           token + ".extra",
		"wrong prefix":            "KAT1." + parts[1] + "." + parts[2],
		"empty payload":           "kat1.." + parts[2],
		"empty signature":         "kat1." + parts[1] + ".",
		"padded payload":          "kat1." + parts[1] + "=." + parts[2],
		"padded signature":        token + "=",
		"non-canonical signature": "kat1." + parts[1] + "." + nonCanonicalSignature,
		"short signature":         "kat1." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 31)),
		"long signature":          "kat1." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 33)),
		"tampered signature":      token[:len(token)-2] + "AA",
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			if err := VerifyExecAccessToken(candidate, testServiceSecret, testAuthSandboxID, time.Unix(1, 0)); !errors.Is(err, errInvalidExecAccessToken) {
				t.Fatalf("VerifyExecAccessToken() error = %v, want fixed invalid-token error", err)
			}
		})
	}
}

func TestExecAccessTokenRequiresCanonicalClaims(t *testing.T) {
	payloads := map[string]string{
		"field order":     `{"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","v":1,"sid":"sandbox-01","aud":"exec"}`,
		"whitespace":      `{"v": 1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec"}`,
		"escaped SID":     `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sand\u0062ox-01","aud":"exec"}`,
		"duplicate":       `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","sid":"sandbox-01","aud":"exec"}`,
		"extra":           `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec","extra":true}`,
		"missing session": `{"v":1,"sid":"sandbox-01","aud":"exec"}`,
		"wrong version":   `{"v":2,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec"}`,
		"wrong SID":       `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-02","aud":"exec"}`,
		"wrong audience":  `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"forward"}`,
		"UUIDv4":          `{"v":1,"session_id":"550e8400-e29b-41d4-a716-446655440000","sid":"sandbox-01","aud":"exec"}`,
		"UUIDv7 variant":  `{"v":1,"session_id":"01890f35-7b2c-7cc6-18c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec"}`,
		"upper UUID":      `{"v":1,"session_id":"01890F35-7B2C-7CC6-98C4-DC0C0C07398F","sid":"sandbox-01","aud":"exec"}`,
		"zero expiry":     `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec","exp":0}`,
		"trailing value":  `{"v":1,"session_id":"01890f35-7b2c-7cc6-98c4-dc0c0c07398f","sid":"sandbox-01","aud":"exec"}{}`,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			token := signedRawPayload(t, []byte(payload), testServiceSecret)
			if err := VerifyExecAccessToken(token, testServiceSecret, testAuthSandboxID, time.Unix(1, 0)); !errors.Is(err, errInvalidExecAccessToken) {
				t.Fatalf("VerifyExecAccessToken() error = %v, want fixed invalid-token error", err)
			}
		})
	}
}

func TestExecAccessTokenExpiryAndBinding(t *testing.T) {
	token, err := mintExecAccessToken(testServiceSecret, testAuthSandboxID, testExecSessionID, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyExecAccessToken(token, testServiceSecret, testAuthSandboxID, time.Unix(99, 0)); err != nil {
		t.Fatalf("token before expiry: %v", err)
	}
	for _, now := range []int64{100, 101} {
		if err := VerifyExecAccessToken(token, testServiceSecret, testAuthSandboxID, time.Unix(now, 0)); !errors.Is(err, errInvalidExecAccessToken) {
			t.Fatalf("token at now=%d error=%v, want expired", now, err)
		}
	}
	if err := VerifyExecAccessToken(token, strings.Repeat("a", 64), testAuthSandboxID, time.Unix(1, 0)); !errors.Is(err, errInvalidExecAccessToken) {
		t.Fatalf("wrong ServiceSecret error = %v", err)
	}
	if err := VerifyExecAccessToken(token, testServiceSecret, "sandbox-02", time.Unix(1, 0)); !errors.Is(err, errInvalidExecAccessToken) {
		t.Fatalf("wrong AuthSandboxID error = %v", err)
	}
	if _, err := mintExecAccessToken(testServiceSecret, testAuthSandboxID, testExecSessionID, -1); !errors.Is(err, errInvalidExecExpiry) {
		t.Fatalf("negative expiry error = %v", err)
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

func execClaimsFromToken(t *testing.T, token string) execPayload {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("exec token has %d segments", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims execPayload
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
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
