package migrationtoken

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	testAPISecret           = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testManifestKey         = "ffeeddccbbaa998877665544332211000123456789abcdef0011223344556677"
	testAPIFingerprint      = "630dcd2966c4336691125448bbb25b4ff412a49c732db2c8abc1b8581bd710dd"
	testManifestFingerprint = "fd0e9107486e4e9152c1dbc5b597e64173f07975a25e6dc1f71ef1ea22f8ce88"
	testMigrationKey        = "efc3f9ac75d63ece0a9065ab26bb24e6429bc78ef12ba07865bb99544c5e89b7"
	testServiceSecret       = "0213051156fc06b40aebdeb333caeb9c95866d91f08298264901787551897989"
	testForwardToken        = "kat1.eyJ2IjoxLCJzaWQiOiJzYW5kYm94LTAxIiwiYXVkIjoiZm9yd2FyZCJ9.uk154F30--e531Hogd4pgj0oGFsuIJJT0nWr17gkdSw"
	testGoldenToken         = "kmt1.AAECAwQFBgcICQoLUgPF0Ywtt-Hjs7DXBF2VenGKKCGN6Ka1gZILGm361mVCZWNqWgBO2W0KHoXI2SBvkdrsTY7bVcqbsDa12jMEPrdDx_S8E8x6MtkUWLUdA_K_vt4m6fWB81zlvzFiDogHg8SrjK4FgCKFjKqJJy5dIPu5FZJCGayVOlRBUx_M__bMbI6e0msljL8DYjwGa_0JYeaO76G_ZCAg3zucM6NxDp1G0jVlB3AgsR2qw6Q-GhNg-7MzcrbRWjHGU-sGGr4xYeCjpN3y-u6i345nIsIeMI4MTb_wUDOZdSokqy8GmcjwPwnWeg_qlMIm-RbR6x6P6LkmkRILdTyTH3qnIcqQc1WOCpz5KcUceS-quXNTOpl_QYue4l3SIps8N6I8ZTzUrhQ8JGCjXzKrjyhsHIFWy8HVmEXH0UaaUAO0czoepRXK1bJlEKbSoPsmjps0ac1mZ5BROeclCydlNDUODtS-ylyGjQwn9pDiuQLR2UN_dtABwYxgVX7dRcXDucTCdcw04mO2rwyqjJk7IkL-j9Byw8MJLgYc4ML72NcPtids5bHHnbJFsxf0qFOXPgQNZ9KdYTtxVI9U2LM4-HIw6Bj51T6Fofi2AAAvIWmdKJKBxrybtQVMAFWz0ylElqW5oXgH7-9KjuPdX-aX_HPHM4Qv6W9lyzzUJQHyLTldi434WQkzOQW3Ll8xekQjwDVrQ8HrFV6VD_YLBq2I6XeCPPWAO0Hko09n_nghhfHRU33stfYr73bJ95Skdd2wzKIJY-sfdFqzo-k9DWFnawgU1BDGYUwgeDdmQuK0UxwYdm_P1MQRy7m1vvH_17FzLbyqRS2Dmwrl_C4E--E_QRJDhkH4LW15dqeT5avt0mDCoAUhH-zyVH522Fl3HtS06_f-4FT21QLiyiM-MuO7Ad55BvYZ_6iTqUmKWgyt3FxaWdUa1oU9Ox17opm0JA1XJXn98eAVb8zxEirIELHizVpO1W96cXjZc6T7HIvw7Jwo1XN6s7GZZH5Dnf-Rqd9tyEYv8bb8uthgL2s-w6IChiHwGeYYenbKO8NzPcWIsow8WpCz7ATWzYQx_lKEyHyhGBmSoKFYCcHqoA8M1FehfBzkyg0HaGaQ5HZhXHPTqDBKvgheeN6iv-J0UkmH6n_c_fH0Xdr7RN3zGwbOo_2eOUDnd73d0J8vLt2pPysRn4c-oH4rSAg-9hpgZqqqW_au-_5T-J-YwHPEvf-ypXtIrFWHXOou"
)

var testMaterial = KeyMaterial{APISecret: testAPISecret, ManifestKey: testManifestKey}

func validPayload() MigrationTokenPayloadV1 {
	return MigrationTokenPayloadV1{
		Version:                1,
		NodeSandboxID:          "sandbox-01-g7",
		AuthSandboxID:          "sandbox-01",
		APISecretFingerprint:   testAPIFingerprint,
		ManifestKeyFingerprint: testManifestFingerprint,
		TemplateID:             "e2b-snp-" + strings.Repeat("a", 64),
		Profile:                "e2b",
		RuntimeDigest:          strings.Repeat("b", 64),
		SnapshotRef:            "manifest://" + strings.Repeat("c", 64),
		Env:                    map[string]string{"LANG": "C.UTF-8"},
		Metadata:               map[string]string{"purpose": "migration"},
		CreatedUnix:            1_700_000_000,
		DeadlineUnix:           0,
		ServiceSecret:          testServiceSecret,
		EnvdAccessToken:        "envd-token",
		TrafficAccessToken:     "traffic-token",
		ForwardAccessToken:     testForwardToken,
	}
}

func TestMigrationKeyGoldenVector(t *testing.T) {
	key, apiFingerprint, manifestFingerprint, err := deriveMaterial(testMaterial)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(key); got != testMigrationKey {
		t.Fatalf("migration key = %q, want golden vector", got)
	}
	if apiFingerprint != testAPIFingerprint || manifestFingerprint != testManifestFingerprint {
		t.Fatalf("fingerprints = (%q, %q), want golden vectors", apiFingerprint, manifestFingerprint)
	}
}

func TestSealOpenGoldenVector(t *testing.T) {
	nonce := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	token, err := seal(bytes.NewReader(nonce), testMaterial, validPayload())
	if err != nil {
		t.Fatal(err)
	}
	if token != testGoldenToken {
		t.Fatalf("golden token = %q", token)
	}
	payload, err := Open(testMaterial, token)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloadEqual(t, payload, validPayload())

	encoded := strings.TrimPrefix(token, wirePrefix)
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire[:nonceSize], nonce) {
		t.Fatalf("nonce = %x, want %x", wire[:nonceSize], nonce)
	}
	if len(wire) <= nonceSize+16 {
		t.Fatalf("wire length = %d, missing GCM ciphertext/tag", len(wire))
	}
}

func TestSealUsesFreshRandomNonce(t *testing.T) {
	first, err := Seal(testMaterial, validPayload())
	if err != nil {
		t.Fatal(err)
	}
	second, err := Seal(testMaterial, validPayload())
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("two seals reused the same nonce")
	}
	if _, err := Open(testMaterial, first); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(testMaterial, second); err != nil {
		t.Fatal(err)
	}
}

func TestWireSizeLimit(t *testing.T) {
	atLimit := payloadForWireSize(t, MaxWireSize)
	token, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, atLimit)
	if err != nil {
		t.Fatalf("Seal(%d-byte wire) error = %v", MaxWireSize, err)
	}
	if len(token) != MaxWireSize {
		t.Fatalf("token length = %d, want %d", len(token), MaxWireSize)
	}
	if _, err := Open(testMaterial, token); err != nil {
		t.Fatalf("Open(%d-byte wire) error = %v", MaxWireSize, err)
	}

	overLimit := payloadForWireSize(t, MaxWireSize+1)
	if _, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, overLimit); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("Seal(%d-byte wire) error = %v, want token too large", MaxWireSize+1, err)
	}

	// Invalid base64 proves Open enforces the complete-wire bound before decode.
	oversizedMalformed := wirePrefix + strings.Repeat("!", MaxWireSize-len(wirePrefix)+1)
	if _, err := Open(testMaterial, oversizedMalformed); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("Open(oversized malformed wire) error = %v, want token too large", err)
	}
}

func TestOpenRejectsTampering(t *testing.T) {
	token := fixedToken(t, validPayload())
	wire := decodeWire(t, token)
	for name, offset := range map[string]int{
		"nonce":      0,
		"ciphertext": nonceSize,
		"tag":        len(wire) - 1,
	} {
		t.Run(name, func(t *testing.T) {
			tampered := append([]byte(nil), wire...)
			tampered[offset] ^= 0x01
			_, err := Open(testMaterial, wirePrefix+base64.RawURLEncoding.EncodeToString(tampered))
			if !errors.Is(err, ErrAuthentication) {
				t.Fatalf("Open(tampered) error = %v, want authentication failure", err)
			}
		})
	}
}

func TestOpenRejectsInvalidWire(t *testing.T) {
	canonicalShort := base64.RawURLEncoding.EncodeToString(make([]byte, nonceSize+16))
	nonCanonicalShort := nonCanonicalRawURL(t, canonicalShort)
	tests := map[string]string{
		"empty":               "",
		"prefix only":         wirePrefix,
		"wrong prefix":        "KMT1." + canonicalShort,
		"old plain base64":    base64.StdEncoding.EncodeToString([]byte(`{"v":1}`)),
		"padded":              wirePrefix + canonicalShort + "=",
		"standard alphabet":   wirePrefix + "+///",
		"non canonical bits":  wirePrefix + nonCanonicalShort,
		"too short":           wirePrefix + base64.RawURLEncoding.EncodeToString(make([]byte, nonceSize+15)),
		"extra segment":       wirePrefix + canonicalShort + ".extra",
		"leading whitespace":  " " + wirePrefix + canonicalShort,
		"trailing whitespace": wirePrefix + canonicalShort + " ",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(testMaterial, token); !errors.Is(err, ErrMalformedToken) {
				t.Fatalf("Open() error = %v, want malformed token", err)
			}
		})
	}
}

func TestOpenStrictJSON(t *testing.T) {
	canonical, err := json.Marshal(validPayload())
	if err != nil {
		t.Fatal(err)
	}
	withoutVersion := strings.Replace(string(canonical), `"v":1,`, "", 1)
	tests := map[string]string{
		"not object":           `[]`,
		"null object":          `null`,
		"unknown field":        strings.TrimSuffix(string(canonical), "}") + `,"extra":true}`,
		"duplicate field":      strings.Replace(string(canonical), `"v":1`, `"v":1,"v":1`, 1),
		"missing field":        withoutVersion,
		"trailing object":      string(canonical) + `{}`,
		"trailing scalar":      string(canonical) + ` 1`,
		"null field":           strings.Replace(string(canonical), `"nodeSandboxID":"sandbox-01-g7"`, `"nodeSandboxID":null`, 1),
		"wrong field type":     strings.Replace(string(canonical), `"nodeSandboxID":"sandbox-01-g7"`, `"nodeSandboxID":7`, 1),
		"duplicate env key":    strings.Replace(string(canonical), `"env":{"LANG":"C.UTF-8"}`, `"env":{"LANG":"C.UTF-8","LANG":"other"}`, 1),
		"null env":             strings.Replace(string(canonical), `"env":{"LANG":"C.UTF-8"}`, `"env":null`, 1),
		"null env value":       strings.Replace(string(canonical), `"env":{"LANG":"C.UTF-8"}`, `"env":{"LANG":null}`, 1),
		"non string env value": strings.Replace(string(canonical), `"env":{"LANG":"C.UTF-8"}`, `"env":{"LANG":1}`, 1),
	}
	for name, plaintext := range tests {
		t.Run(name, func(t *testing.T) {
			token := encryptPlaintext(t, []byte(plaintext))
			if _, err := Open(testMaterial, token); !errors.Is(err, ErrMalformedToken) {
				t.Fatalf("Open() error = %v, want malformed token", err)
			}
		})
	}

	invalidUTF8 := append(append([]byte(nil), canonical[:1]...), 0xff)
	invalidUTF8 = append(invalidUTF8, canonical[1:]...)
	if _, err := Open(testMaterial, encryptPlaintext(t, invalidUTF8)); !errors.Is(err, ErrMalformedToken) {
		t.Fatalf("invalid UTF-8 error = %v, want malformed token", err)
	}

	if _, err := Open(testMaterial, encryptPlaintext(t, append(canonical, ' ', '\n', '\t'))); err != nil {
		t.Fatalf("trailing JSON whitespace was rejected: %v", err)
	}
}

func TestOptionalEnvAndMetadataMayBeOmitted(t *testing.T) {
	payload := validPayload()
	payload.Env = nil
	payload.Metadata = nil
	token := fixedToken(t, payload)
	got, err := Open(testMaterial, token)
	if err != nil {
		t.Fatal(err)
	}
	if got.Env != nil || got.Metadata != nil {
		t.Fatalf("omitted maps decoded as env=%v metadata=%v", got.Env, got.Metadata)
	}
}

func TestOpenValidatesPayloadSemantics(t *testing.T) {
	tests := map[string]func(*MigrationTokenPayloadV1){
		"version":              func(p *MigrationTokenPayloadV1) { p.Version = 2 },
		"node sandbox ID":      func(p *MigrationTokenPayloadV1) { p.NodeSandboxID = "" },
		"auth sandbox ID":      func(p *MigrationTokenPayloadV1) { p.AuthSandboxID = "" },
		"API fingerprint":      func(p *MigrationTokenPayloadV1) { p.APISecretFingerprint = strings.ToUpper(p.APISecretFingerprint) },
		"manifest fingerprint": func(p *MigrationTokenPayloadV1) { p.ManifestKeyFingerprint = p.ManifestKeyFingerprint[:62] },
		"profile":              func(p *MigrationTokenPayloadV1) { p.Profile = "unknown" },
		"template":             func(p *MigrationTokenPayloadV1) { p.TemplateID = "bad" },
		"template profile":     func(p *MigrationTokenPayloadV1) { p.TemplateID = "bare-snp-" + strings.Repeat("a", 64) },
		"runtime":              func(p *MigrationTokenPayloadV1) { p.RuntimeDigest = strings.Repeat("B", 64) },
		"snapshot":             func(p *MigrationTokenPayloadV1) { p.SnapshotRef = "/local/snapshot" },
		"created":              func(p *MigrationTokenPayloadV1) { p.CreatedUnix = 0 },
		"deadline":             func(p *MigrationTokenPayloadV1) { p.DeadlineUnix = -1 },
		"service secret":       func(p *MigrationTokenPayloadV1) { p.ServiceSecret = strings.Repeat("S", 64) },
		"envd token":           func(p *MigrationTokenPayloadV1) { p.EnvdAccessToken = "" },
		"traffic token":        func(p *MigrationTokenPayloadV1) { p.TrafficAccessToken = "" },
		"forward token":        func(p *MigrationTokenPayloadV1) { p.ForwardAccessToken = "" },
		"forward subject":      func(p *MigrationTokenPayloadV1) { p.AuthSandboxID = "other-sandbox" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			mutate(&payload)
			plaintext, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Open(testMaterial, encryptPlaintext(t, plaintext)); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Open() error = %v, want invalid payload", err)
			}
		})
	}
}

func TestNodeSandboxIDContract(t *testing.T) {
	payload := validPayload()
	payload.NodeSandboxID = "a" + strings.Repeat("-", 55) + "z"
	if _, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, payload); err != nil {
		t.Fatalf("Seal(57-byte node sandbox ID) error = %v", err)
	}

	for name, id := range map[string]string{
		"empty":           "",
		"58 bytes":        strings.Repeat("a", 58),
		"leading hyphen":  "-sandbox",
		"trailing hyphen": "sandbox-",
		"uppercase":       "Sandbox",
		"path separator":  "sandbox/id",
		"non ASCII":       "sandébox",
	} {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			payload.NodeSandboxID = id
			if _, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Seal(nodeSandboxID=%q) error = %v, want invalid payload", id, err)
			}
		})
	}
}

func TestE2BAccessTokenSizeContract(t *testing.T) {
	atLimit := strings.Repeat("界", 85) + "a"
	payload := validPayload()
	payload.EnvdAccessToken = atLimit
	payload.TrafficAccessToken = atLimit
	token := fixedToken(t, payload)
	got, err := Open(testMaterial, token)
	if err != nil {
		t.Fatal(err)
	}
	if got.EnvdAccessToken != atLimit || got.TrafficAccessToken != atLimit {
		t.Fatal("256-byte e2b access token changed during round trip")
	}

	overLimit := atLimit + "b"
	for name, mutate := range map[string]func(*MigrationTokenPayloadV1){
		"envd":    func(p *MigrationTokenPayloadV1) { p.EnvdAccessToken = overLimit },
		"traffic": func(p *MigrationTokenPayloadV1) { p.TrafficAccessToken = overLimit },
	} {
		t.Run(name, func(t *testing.T) {
			payload := validPayload()
			mutate(&payload)
			if _, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Seal(257-byte %s token) error = %v, want invalid payload", name, err)
			}
			plaintext, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Open(testMaterial, encryptPlaintext(t, plaintext)); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("Open(257-byte %s token) error = %v, want invalid payload", name, err)
			}
		})
	}
}

func TestProfileCredentialContract(t *testing.T) {
	payload := validPayload()
	payload.Profile = "bare"
	payload.TemplateID = "bare-snp-" + strings.Repeat("a", 64)
	payload.EnvdAccessToken = ""
	payload.TrafficAccessToken = ""
	token := fixedToken(t, payload)
	got, err := Open(testMaterial, token)
	if err != nil {
		t.Fatal(err)
	}
	if got.EnvdAccessToken != "" || got.TrafficAccessToken != "" || got.ForwardAccessToken == "" {
		t.Fatalf("bare credentials changed: %+v", got)
	}

	payload.EnvdAccessToken = "e2b-only"
	plaintext, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(testMaterial, encryptPlaintext(t, plaintext)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("bare e2b credential error = %v, want invalid payload", err)
	}
}

func TestTenantFingerprintBinding(t *testing.T) {
	token := fixedToken(t, validPayload())

	wrongAPI := testMaterial
	wrongAPI.APISecret = strings.Repeat("1", 64)
	if _, err := Open(wrongAPI, token); !errors.Is(err, ErrCredentialMismatch) {
		t.Fatalf("wrong API secret error = %v, want credential mismatch", err)
	}

	wrongManifest := testMaterial
	wrongManifest.ManifestKey = strings.Repeat("2", 64)
	if _, err := Open(wrongManifest, token); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong manifest key error = %v, want authentication failure", err)
	}

	payload := validPayload()
	payload.APISecretFingerprint = strings.Repeat("d", 64)
	if _, err := seal(bytes.NewReader(make([]byte, nonceSize)), testMaterial, payload); !errors.Is(err, ErrCredentialMismatch) {
		t.Fatalf("Seal(wrong fingerprint) error = %v, want credential mismatch", err)
	}
}

func TestValidateExpectations(t *testing.T) {
	payload := validPayload()
	matching := Expectations{
		AuthSandboxID: payload.AuthSandboxID,
		TemplateID:    payload.TemplateID,
		Profile:       types.Profile(payload.Profile),
		RuntimeDigest: payload.RuntimeDigest,
		SnapshotRef:   payload.SnapshotRef,
	}
	if err := ValidateExpectations(payload, matching); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExpectations(payload, Expectations{}); err != nil {
		t.Fatal(err)
	}

	tests := map[string]Expectations{
		"subject":  {AuthSandboxID: "other"},
		"template": {TemplateID: "other"},
		"profile":  {Profile: types.ProfileBare},
		"runtime":  {RuntimeDigest: strings.Repeat("d", 64)},
		"snapshot": {SnapshotRef: "manifest://" + strings.Repeat("e", 64)},
	}
	for name, expected := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateExpectations(payload, expected); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("ValidateExpectations() error = %v, want incompatible", err)
			}
		})
	}
}

func TestKeyMaterialAndRandomSourceValidation(t *testing.T) {
	for name, material := range map[string]KeyMaterial{
		"empty API secret":     {ManifestKey: testManifestKey},
		"uppercase API secret": {APISecret: strings.ToUpper(testAPISecret), ManifestKey: testManifestKey},
		"short manifest key":   {APISecret: testAPISecret, ManifestKey: testManifestKey[:62]},
		"non hex manifest key": {APISecret: testAPISecret, ManifestKey: strings.Repeat("z", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Seal(material, validPayload()); !errors.Is(err, ErrInvalidKeyMaterial) {
				t.Fatalf("Seal() error = %v, want invalid key material", err)
			}
			if _, err := Open(material, "secret-token-fragment"); !errors.Is(err, ErrInvalidKeyMaterial) {
				t.Fatalf("Open() error = %v, want invalid key material", err)
			}
		})
	}

	if _, err := seal(errReader{}, testMaterial, validPayload()); err == nil {
		t.Fatal("Seal succeeded with a failing random source")
	}
}

func TestErrorsDoNotEchoSensitiveInputs(t *testing.T) {
	badToken := "kmt1.secret-token-fragment"
	_, err := Open(testMaterial, badToken)
	if err == nil || strings.Contains(err.Error(), badToken) || strings.Contains(err.Error(), testManifestKey) {
		t.Fatalf("Open error leaks token or key material: %v", err)
	}

	badKey := strings.Repeat("S", 64)
	_, err = Seal(KeyMaterial{APISecret: testAPISecret, ManifestKey: badKey}, validPayload())
	if err == nil || strings.Contains(err.Error(), badKey) {
		t.Fatalf("Seal error leaks key material: %v", err)
	}
}

func fixedToken(t *testing.T, payload MigrationTokenPayloadV1) string {
	t.Helper()
	token, err := seal(bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), testMaterial, payload)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func encryptPlaintext(t *testing.T, plaintext []byte) string {
	t.Helper()
	key, _, _, err := deriveMaterial(testMaterial)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	wire := append(append([]byte(nil), nonce...), gcm.Seal(nil, nonce, plaintext, nil)...)
	return wirePrefix + base64.RawURLEncoding.EncodeToString(wire)
}

func payloadForWireSize(t *testing.T, want int) MigrationTokenPayloadV1 {
	t.Helper()
	payload := validPayload()
	payload.Metadata = map[string]string{"padding": ""}
	base, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	wireSize := func(padding int) int {
		binarySize := nonceSize + 16 + len(base) + padding
		return len(wirePrefix) + base64.RawURLEncoding.EncodedLen(binarySize)
	}

	low, high := 0, want
	for low <= high {
		padding := low + (high-low)/2
		size := wireSize(padding)
		switch {
		case size < want:
			low = padding + 1
		case size > want:
			high = padding - 1
		default:
			payload.Metadata["padding"] = strings.Repeat("x", padding)
			plaintext, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(wirePrefix) + base64.RawURLEncoding.EncodedLen(nonceSize+16+len(plaintext)); got != want {
				t.Fatalf("constructed wire size = %d, want %d", got, want)
			}
			return payload
		}
	}
	t.Fatalf("cannot construct a token with wire size %d", want)
	return MigrationTokenPayloadV1{}
}

func decodeWire(t *testing.T, token string) []byte {
	t.Helper()
	encoded, ok := strings.CutPrefix(token, wirePrefix)
	if !ok {
		t.Fatal("missing test token prefix")
	}
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func assertPayloadEqual(t *testing.T, got, want MigrationTokenPayloadV1) {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("payload = %s, want %s", gotJSON, wantJSON)
	}
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
	if last < 0 {
		t.Fatal("canonical test value has invalid alphabet")
	}
	unusedBits := 2
	if len(raw)%3 == 1 {
		unusedBits = 4
	}
	mask := (1 << unusedBits) - 1
	if last&mask != 0 {
		t.Fatal("canonical test value already has nonzero unused bits")
	}
	changed := (last &^ mask) | 1
	return canonical[:len(canonical)-1] + string(alphabet[changed])
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
