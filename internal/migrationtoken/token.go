// Package migrationtoken implements the opaque kmt1 sandbox migration token.
package migrationtoken

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	wirePrefix      = "kmt1."
	kdfDomain       = "kuasar-migration-token-v1"
	nonceSize       = 12
	secretHexLength = sha256.Size * 2

	// MaxWireSize is the maximum complete kmt1 wire representation, including
	// the kmt1. prefix and base64url envelope.
	MaxWireSize = 512 * 1024
)

var (
	// ErrMalformedToken identifies an invalid kmt1 envelope or plaintext JSON.
	ErrMalformedToken = errors.New("migrationtoken: malformed token")
	// ErrAuthentication identifies a token whose GCM authentication failed.
	ErrAuthentication = errors.New("migrationtoken: authentication failed")
	// ErrInvalidPayload identifies a structurally or semantically invalid payload.
	ErrInvalidPayload = errors.New("migrationtoken: invalid payload")
	// ErrCredentialMismatch identifies tenant key material that does not match the
	// fingerprints carried by an otherwise authenticated payload.
	ErrCredentialMismatch = errors.New("migrationtoken: credential mismatch")
	// ErrIncompatible identifies a valid token that does not match target-owned
	// profile, template, runtime, snapshot, or stable-ID state.
	ErrIncompatible = errors.New("migrationtoken: incompatible target")
	// ErrInvalidKeyMaterial identifies a non-canonical tenant root supplied by the
	// trusted caller. Errors never include the supplied material.
	ErrInvalidKeyMaterial = errors.New("migrationtoken: invalid key material")
	// ErrTokenTooLarge identifies a complete kmt1 wire representation that exceeds
	// MaxWireSize.
	ErrTokenTooLarge = errors.New("migrationtoken: token too large")
)

// KeyMaterial is the paired tenant credential material used to protect and
// authorize a migration token. Only fingerprints, never these raw roots, enter
// the encrypted payload.
type KeyMaterial struct {
	APISecret   string
	ManifestKey string
}

// MigrationTokenPayloadV1 is the complete kmt1 plaintext contract.
type MigrationTokenPayloadV1 struct {
	Version uint16 `json:"v"`

	NodeSandboxID string `json:"nodeSandboxID"`
	StableID      string `json:"stableID"`

	APISecretFingerprint   string `json:"apiSecretFingerprint"`
	ManifestKeyFingerprint string `json:"manifestKeyFingerprint"`

	TemplateID    string `json:"templateID"`
	Profile       string `json:"profile"`
	RuntimeDigest string `json:"runtimeDigest"`
	SnapshotRef   string `json:"snapshotRef"`

	Env      map[string]string `json:"env,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`

	CreatedUnix  int64 `json:"createdUnix"`
	DeadlineUnix int64 `json:"deadlineUnix"`

	ServiceSecret      string `json:"serviceSecret"`
	EnvdAccessToken    string `json:"envdAccessToken"`
	TrafficAccessToken string `json:"trafficAccessToken"`
	ForwardAccessToken string `json:"forwardAccessToken"`
}

// Expectations contains target-owned state that, when present, must match a
// decrypted token. Empty fields are not target constraints.
type Expectations struct {
	StableID      string
	TemplateID    string
	Profile       types.Profile
	RuntimeDigest string
	SnapshotRef   string
}

// Seal validates payload and key ownership, then encrypts a fresh kmt1 token
// using a cryptographically random 12-byte AES-GCM nonce.
func Seal(material KeyMaterial, payload MigrationTokenPayloadV1) (string, error) {
	return seal(rand.Reader, material, payload)
}

func seal(random io.Reader, material KeyMaterial, payload MigrationTokenPayloadV1) (string, error) {
	key, apiFingerprint, manifestFingerprint, err := deriveMaterial(material)
	if err != nil {
		return "", err
	}
	if err := validatePayload(payload); err != nil {
		return "", err
	}
	if !equalHexDigest(payload.APISecretFingerprint, apiFingerprint) ||
		!equalHexDigest(payload.ManifestKeyFingerprint, manifestFingerprint) {
		return "", ErrCredentialMismatch
	}

	plaintext, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: cannot encode JSON", ErrInvalidPayload)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", ErrInvalidKeyMaterial
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", ErrInvalidKeyMaterial
	}
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(random, nonce); err != nil {
		return "", fmt.Errorf("migrationtoken: generate nonce: %w", err)
	}
	sealed := gcm.Seal(nil, nonce, plaintext, nil)
	wire := make([]byte, 0, len(nonce)+len(sealed))
	wire = append(wire, nonce...)
	wire = append(wire, sealed...)
	token := wirePrefix + base64.RawURLEncoding.EncodeToString(wire)
	if len(token) > MaxWireSize {
		return "", ErrTokenTooLarge
	}
	return token, nil
}

// Open authenticates and decrypts token, strictly parses and validates its V1
// payload, and verifies both tenant fingerprints against material.
func Open(material KeyMaterial, token string) (MigrationTokenPayloadV1, error) {
	var zero MigrationTokenPayloadV1
	key, apiFingerprint, manifestFingerprint, err := deriveMaterial(material)
	if err != nil {
		return zero, err
	}

	encoded, ok := strings.CutPrefix(token, wirePrefix)
	if !ok || encoded == "" {
		return zero, ErrMalformedToken
	}
	if len(token) > MaxWireSize {
		return zero, ErrTokenTooLarge
	}
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(wire) != encoded {
		return zero, ErrMalformedToken
	}
	if len(wire) < nonceSize+16 {
		return zero, ErrMalformedToken
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return zero, ErrInvalidKeyMaterial
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return zero, ErrInvalidKeyMaterial
	}
	plaintext, err := gcm.Open(nil, wire[:nonceSize], wire[nonceSize:], nil)
	if err != nil {
		return zero, ErrAuthentication
	}
	payload, err := parsePayload(plaintext)
	if err != nil {
		return zero, err
	}
	if err := validatePayload(payload); err != nil {
		return zero, err
	}
	if !equalHexDigest(payload.APISecretFingerprint, apiFingerprint) ||
		!equalHexDigest(payload.ManifestKeyFingerprint, manifestFingerprint) {
		return zero, ErrCredentialMismatch
	}
	return payload, nil
}

// ValidateExpectations compares an already authenticated payload with trusted
// target-owned compatibility constraints.
func ValidateExpectations(payload MigrationTokenPayloadV1, expected Expectations) error {
	if expected.StableID != "" && payload.StableID != expected.StableID {
		return fmt.Errorf("%w: stable ID", ErrIncompatible)
	}
	if expected.TemplateID != "" && payload.TemplateID != expected.TemplateID {
		return fmt.Errorf("%w: template", ErrIncompatible)
	}
	if expected.Profile != "" && payload.Profile != string(expected.Profile) {
		return fmt.Errorf("%w: profile", ErrIncompatible)
	}
	if expected.RuntimeDigest != "" && payload.RuntimeDigest != expected.RuntimeDigest {
		return fmt.Errorf("%w: runtime", ErrIncompatible)
	}
	if expected.SnapshotRef != "" && payload.SnapshotRef != expected.SnapshotRef {
		return fmt.Errorf("%w: snapshot", ErrIncompatible)
	}
	return nil
}

func validatePayload(payload MigrationTokenPayloadV1) error {
	if payload.Version != 1 {
		return invalidPayload("version")
	}
	if !types.ValidLocalSandboxID(payload.NodeSandboxID) {
		return invalidPayload("node sandbox ID")
	}
	if !validRequiredString(payload.StableID) {
		return invalidPayload("stable ID")
	}
	if !validHexDigest(payload.APISecretFingerprint) || !validHexDigest(payload.ManifestKeyFingerprint) {
		return invalidPayload("credential fingerprint")
	}

	profile, err := types.ParseProfile(payload.Profile)
	if err != nil {
		return invalidPayload("profile")
	}
	template, err := types.ParseTemplateID(payload.TemplateID)
	if err != nil || template.Profile != profile {
		return invalidPayload("template")
	}
	if !validHexDigest(payload.RuntimeDigest) {
		return invalidPayload("runtime digest")
	}
	if !validSnapshotRef(payload.SnapshotRef) {
		return invalidPayload("snapshot reference")
	}
	if payload.CreatedUnix <= 0 || payload.DeadlineUnix < 0 {
		return invalidPayload("timestamp")
	}
	if !validStringMap(payload.Env) || !validStringMap(payload.Metadata) {
		return invalidPayload("environment or metadata")
	}
	if !validHexDigest(payload.ServiceSecret) {
		return invalidPayload("service secret")
	}
	if !sandboxcfg.ValidE2BAccessToken(payload.EnvdAccessToken) ||
		!sandboxcfg.ValidE2BAccessToken(payload.TrafficAccessToken) ||
		!validRequiredString(payload.ForwardAccessToken) {
		return invalidPayload("service credential")
	}
	switch profile {
	case types.ProfileE2B:
		if payload.EnvdAccessToken == "" || payload.TrafficAccessToken == "" {
			return invalidPayload("e2b service credential")
		}
	case types.ProfileBare:
		if payload.EnvdAccessToken != "" || payload.TrafficAccessToken != "" {
			return invalidPayload("bare service credential")
		}
	}
	if err := keys.VerifyForwardAccessToken(
		payload.ForwardAccessToken,
		payload.ServiceSecret,
		payload.StableID,
	); err != nil {
		return invalidPayload("forward access token")
	}
	return nil
}

func deriveMaterial(material KeyMaterial) (key []byte, apiFingerprint, manifestFingerprint string, err error) {
	apiSecret, ok := decodeCanonicalHex32(material.APISecret)
	if !ok {
		return nil, "", "", ErrInvalidKeyMaterial
	}
	manifestKey, ok := decodeCanonicalHex32(material.ManifestKey)
	if !ok {
		return nil, "", "", ErrInvalidKeyMaterial
	}

	mac := hmac.New(sha256.New, manifestKey)
	_, _ = mac.Write([]byte(kdfDomain))
	key = mac.Sum(nil)
	apiDigest := sha256.Sum256(apiSecret)
	manifestDigest := sha256.Sum256(manifestKey)
	return key, hex.EncodeToString(apiDigest[:]), hex.EncodeToString(manifestDigest[:]), nil
}

func decodeCanonicalHex32(encoded string) ([]byte, bool) {
	if !validHexDigest(encoded) {
		return nil, false
	}
	decoded, err := hex.DecodeString(encoded)
	return decoded, err == nil && len(decoded) == sha256.Size
}

func validHexDigest(value string) bool {
	if len(value) != secretHexLength {
		return false
	}
	for i := range value {
		if c := value[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func equalHexDigest(left, right string) bool {
	return hmac.Equal([]byte(left), []byte(right))
}

func validRequiredString(value string) bool {
	return value != "" && utf8.ValidString(value)
}

func validStringMap(values map[string]string) bool {
	for key, value := range values {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func validSnapshotRef(raw string) bool {
	ref, err := types.ParsePortableRef(raw)
	return err == nil && (ref.Scheme != "file" ||
		strings.HasSuffix(ref.Path, ".snapshot") || strings.HasSuffix(ref.Path, ".bundle"))
}

func invalidPayload(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalidPayload, field)
}
