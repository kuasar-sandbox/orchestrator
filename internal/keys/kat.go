package keys

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
)

const (
	serviceSecretInfo = "kuasar-service-secret-v1:"
	kat1Prefix        = "kat1"
	forwardAudience   = "forward"
	execAudience      = "exec"
)

var (
	// Fixed errors deliberately never include the supplied secret, stable ID, or
	// token. API handlers map token verification failures to their public error.
	errInvalidAPISecret          = errors.New("keys: invalid API secret")
	errInvalidServiceSecret      = errors.New("keys: invalid service secret")
	errInvalidStableID           = errors.New("keys: invalid stable ID")
	errInvalidForwardAccessToken = errors.New("keys: invalid forward access token")
	errInvalidExecAccessToken    = errors.New("keys: invalid exec access token")
	errInvalidExecExpiry         = errors.New("keys: invalid exec access token expiry")
)

type forwardPayload struct {
	Version  int    `json:"v"`
	SID      string `json:"sid"`
	Audience string `json:"aud"`
}

type execPayload struct {
	Version    int      `json:"v"`
	SessionID  string   `json:"session_id"`
	SID        string   `json:"sid"`
	Audience   string   `json:"aud"`
	Expires    *int64   `json:"exp,omitempty"`
	Conditions []string `json:"conditions,omitempty"`
}

// ExecAccessClaims are the verified request-authorization claims carried by an
// exec KAT. All reference fields are defensive copies.
type ExecAccessClaims struct {
	SessionID   string
	ExpiresUnix *int64
	Conditions  []string
}

// DeriveServiceSecret derives the default sandbox-scoped ServiceSecret from a
// tenant APISecret and the sandbox's stable identity. Both input
// validation and output encoding follow the wire contract: the root and result
// are 32 bytes represented as canonical lowercase hex.
func DeriveServiceSecret(apiSecretHex, stableID string) (string, error) {
	apiSecret, err := decodeCanonicalHex32(apiSecretHex, errInvalidAPISecret)
	if err != nil {
		return "", err
	}
	if !validStableID(stableID) {
		return "", errInvalidStableID
	}

	mac := hmac.New(sha256.New, apiSecret)
	_, _ = mac.Write([]byte(serviceSecretInfo))
	_, _ = mac.Write([]byte(stableID))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// MintForwardAccessToken returns the deterministic kat1 forward token bound to
// stableID and signed by serviceSecretHex.
func MintForwardAccessToken(serviceSecretHex, stableID string) (string, error) {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return "", err
	}
	payload, err := canonicalForwardPayload(stableID)
	if err != nil {
		return "", err
	}

	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := kat1Prefix + "." + payloadB64
	signature := signKAT(serviceSecret, signingInput)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// VerifyForwardAccessToken strictly validates token's kat1 wire encoding,
// canonical forward claims, StableID binding, and HMAC-SHA256 signature.
func VerifyForwardAccessToken(token, serviceSecretHex, stableID string) error {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return err
	}
	wantPayload, err := canonicalForwardPayload(stableID)
	if err != nil {
		return err
	}

	segments := bytes.Split([]byte(token), []byte{'.'})
	if len(segments) != 3 || string(segments[0]) != kat1Prefix ||
		len(segments[1]) == 0 || len(segments[2]) == 0 {
		return errInvalidForwardAccessToken
	}
	payload, ok := decodeCanonicalRawURL(segments[1])
	if !ok || !bytes.Equal(payload, wantPayload) {
		return errInvalidForwardAccessToken
	}
	signature, ok := decodeCanonicalRawURL(segments[2])
	if !ok || len(signature) != sha256.Size {
		return errInvalidForwardAccessToken
	}

	signingInput := kat1Prefix + "." + string(segments[1])
	wantSignature := signKAT(serviceSecret, signingInput)
	if !hmac.Equal(signature, wantSignature) {
		return errInvalidForwardAccessToken
	}
	return nil
}

// MintExecAccessToken returns a new kat1 exec capability bound to
// stableID. expiresUnix == 0 produces a long-lived token without an exp
// claim; a positive value is encoded verbatim. The UUIDv7 session id remains an
// internal token claim and is intentionally not returned separately.
func MintExecAccessToken(serviceSecretHex, stableID string, expiresUnix int64) (string, error) {
	return MintExecAccessTokenWithConditions(serviceSecretHex, stableID, expiresUnix, nil)
}

// MintExecAccessTokenWithConditions returns a new exec capability carrying the
// ordered condition sources. Empty input is normalized to an omitted claim.
func MintExecAccessTokenWithConditions(
	serviceSecretHex, stableID string,
	expiresUnix int64,
	conditions []string,
) (string, error) {
	sessionID, err := uuid.NewV7()
	if err != nil {
		return "", errInvalidExecAccessToken
	}
	return mintExecAccessTokenWithConditions(
		serviceSecretHex, stableID, sessionID.String(), expiresUnix, conditions,
	)
}

// VerifyExecAccessToken strictly validates token's kat1 wire encoding,
// canonical exec claims, StableID binding, expiry, and HMAC-SHA256 signature.
func VerifyExecAccessToken(token, serviceSecretHex, stableID string, now time.Time) error {
	_, err := ParseAndVerifyExecAccessToken(token, serviceSecretHex, stableID, now)
	return err
}

// ParseAndVerifyExecAccessToken verifies HMAC and all canonical/binding claims
// before returning claims to CEL admission. No condition is interpreted here.
func ParseAndVerifyExecAccessToken(
	token, serviceSecretHex, stableID string,
	now time.Time,
) (ExecAccessClaims, error) {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return ExecAccessClaims{}, err
	}
	if !validStableID(stableID) {
		return ExecAccessClaims{}, errInvalidStableID
	}

	if len(token) > limits.MaxExecTokenBytes {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	segments := bytes.Split([]byte(token), []byte{'.'})
	if len(segments) != 3 || string(segments[0]) != kat1Prefix ||
		len(segments[1]) == 0 || len(segments[2]) == 0 {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	payload, ok := decodeCanonicalRawURL(segments[1])
	if !ok {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	signature, ok := decodeCanonicalRawURL(segments[2])
	if !ok || len(signature) != sha256.Size {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	signingInput := kat1Prefix + "." + string(segments[1])
	if !hmac.Equal(signature, signKAT(serviceSecret, signingInput)) {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}

	var claims execPayload
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	canonical, err := json.Marshal(claims)
	if err != nil || !bytes.Equal(payload, canonical) ||
		claims.Version != 1 || claims.SID != stableID || claims.Audience != execAudience ||
		!validUUIDv7(claims.SessionID) {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	if err := limits.ValidateExpressions(claims.Conditions); err != nil {
		return ExecAccessClaims{}, errInvalidExecAccessToken
	}
	if claims.Expires != nil {
		if *claims.Expires <= 0 || now.Unix() >= *claims.Expires {
			return ExecAccessClaims{}, errInvalidExecAccessToken
		}
	}
	result := ExecAccessClaims{
		SessionID:  claims.SessionID,
		Conditions: append([]string(nil), claims.Conditions...),
	}
	if claims.Expires != nil {
		expires := *claims.Expires
		result.ExpiresUnix = &expires
	}
	return result, nil
}

func mintExecAccessToken(serviceSecretHex, stableID, sessionID string, expiresUnix int64) (string, error) {
	return mintExecAccessTokenWithConditions(
		serviceSecretHex, stableID, sessionID, expiresUnix, nil,
	)
}

func mintExecAccessTokenWithConditions(
	serviceSecretHex, stableID, sessionID string,
	expiresUnix int64,
	conditions []string,
) (string, error) {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return "", err
	}
	if !validStableID(stableID) {
		return "", errInvalidStableID
	}
	if !validUUIDv7(sessionID) {
		return "", errInvalidExecAccessToken
	}
	if expiresUnix < 0 {
		return "", errInvalidExecExpiry
	}
	normalizedConditions, err := limits.NormalizeExpressions(conditions)
	if err != nil {
		return "", errInvalidExecAccessToken
	}
	claims := execPayload{
		Version: 1, SessionID: sessionID, SID: stableID, Audience: execAudience,
		Conditions: normalizedConditions,
	}
	if expiresUnix > 0 {
		claims.Expires = &expiresUnix
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", errInvalidExecAccessToken
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := kat1Prefix + "." + payloadB64
	signature := signKAT(serviceSecret, signingInput)
	token := signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
	if len(token) > limits.MaxExecTokenBytes {
		return "", errInvalidExecAccessToken
	}
	return token, nil
}

func validUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.Version() == 7 && id.Variant() == uuid.RFC4122 && id.String() == value
}

func canonicalForwardPayload(stableID string) ([]byte, error) {
	if !validStableID(stableID) {
		return nil, errInvalidStableID
	}
	payload, err := json.Marshal(forwardPayload{
		Version:  1,
		SID:      stableID,
		Audience: forwardAudience,
	})
	if err != nil {
		// All fields are primitive values and stableID is valid UTF-8, so
		// this is unreachable unless encoding/json's contract changes.
		return nil, errInvalidStableID
	}
	return payload, nil
}

func validStableID(stableID string) bool {
	return stableID != "" && utf8.ValidString(stableID)
}

func decodeCanonicalHex32(encoded string, invalid error) ([]byte, error) {
	if len(encoded) != hex.EncodedLen(sha256.Size) {
		return nil, invalid
	}
	for i := range encoded {
		c := encoded[i]
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'f') {
			return nil, invalid
		}
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return nil, invalid
	}
	return decoded, nil
}

func decodeCanonicalRawURL(encoded []byte) ([]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(string(encoded))
	if err != nil || !bytes.Equal([]byte(base64.RawURLEncoding.EncodeToString(decoded)), encoded) {
		return nil, false
	}
	return decoded, true
}

func signKAT(serviceSecret []byte, signingInput string) []byte {
	mac := hmac.New(sha256.New, serviceSecret)
	_, _ = mac.Write([]byte(signingInput))
	return mac.Sum(nil)
}
