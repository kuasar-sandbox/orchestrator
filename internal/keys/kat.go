package keys

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

const (
	serviceSecretInfo = "kuasar-service-secret-v1:"
	kat1Prefix        = "kat1"
	forwardAudience   = "forward"
)

var (
	// Fixed errors deliberately never include the supplied secret, subject, or
	// token. API handlers map token verification failures to their public error.
	errInvalidAPISecret          = errors.New("keys: invalid API secret")
	errInvalidServiceSecret      = errors.New("keys: invalid service secret")
	errInvalidAuthSandboxID      = errors.New("keys: invalid auth sandbox id")
	errInvalidForwardAccessToken = errors.New("keys: invalid forward access token")
)

type forwardPayload struct {
	Version  int    `json:"v"`
	SID      string `json:"sid"`
	Audience string `json:"aud"`
}

// DeriveServiceSecret derives the default sandbox-scoped ServiceSecret from a
// tenant APISecret and the sandbox's stable authentication subject. Both input
// validation and output encoding follow the wire contract: the root and result
// are 32 bytes represented as canonical lowercase hex.
func DeriveServiceSecret(apiSecretHex, authSandboxID string) (string, error) {
	apiSecret, err := decodeCanonicalHex32(apiSecretHex, errInvalidAPISecret)
	if err != nil {
		return "", err
	}
	if !validAuthSandboxID(authSandboxID) {
		return "", errInvalidAuthSandboxID
	}

	mac := hmac.New(sha256.New, apiSecret)
	_, _ = mac.Write([]byte(serviceSecretInfo))
	_, _ = mac.Write([]byte(authSandboxID))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// MintForwardAccessToken returns the deterministic kat1 forward token bound to
// authSandboxID and signed by serviceSecretHex.
func MintForwardAccessToken(serviceSecretHex, authSandboxID string) (string, error) {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return "", err
	}
	payload, err := canonicalForwardPayload(authSandboxID)
	if err != nil {
		return "", err
	}

	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := kat1Prefix + "." + payloadB64
	signature := signKAT(serviceSecret, signingInput)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// VerifyForwardAccessToken strictly validates token's kat1 wire encoding,
// canonical forward claims, subject binding, and HMAC-SHA256 signature.
func VerifyForwardAccessToken(token, serviceSecretHex, authSandboxID string) error {
	serviceSecret, err := decodeCanonicalHex32(serviceSecretHex, errInvalidServiceSecret)
	if err != nil {
		return err
	}
	wantPayload, err := canonicalForwardPayload(authSandboxID)
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

func canonicalForwardPayload(authSandboxID string) ([]byte, error) {
	if !validAuthSandboxID(authSandboxID) {
		return nil, errInvalidAuthSandboxID
	}
	payload, err := json.Marshal(forwardPayload{
		Version:  1,
		SID:      authSandboxID,
		Audience: forwardAudience,
	})
	if err != nil {
		// All fields are primitive values and authSandboxID is valid UTF-8, so
		// this is unreachable unless encoding/json's contract changes.
		return nil, errInvalidAuthSandboxID
	}
	return payload, nil
}

func validAuthSandboxID(authSandboxID string) bool {
	return authSandboxID != "" && utf8.ValidString(authSandboxID)
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
