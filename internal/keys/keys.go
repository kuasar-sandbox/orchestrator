// Package keys mints sandbox access tokens and derives sandbox-scoped service
// and MMDS material. Tenant API authentication lives in internal/apikey;
// persisted root secrets are protected by internal/secretbox.
package keys

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// MintSecret returns 32 cryptographically random bytes encoded as 64 canonical
// lowercase hexadecimal characters.
func MintSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("keys: mint secret: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// MintToken returns a random opaque token for the existing envd and traffic
// token fields. Those fields use the same 32-byte canonical hex representation
// as a random secret, but retain their purpose-specific API name.
func MintToken() (string, error) { return MintSecret() }

// mmdsSecretInfo binds the derivation to its purpose + version so the key never
// collides with another use of the manifest key.
const mmdsSecretInfo = "kuasar-mmds-v1:"

// MmdsSecret derives the per-sandbox HMAC key the MMDS service signs session tokens
// with. It is deterministic: every proxy worker (and the orchestrator) derives the
// same 32-byte key from the sandbox's manifest key + id, so a token minted by one
// process verifies in any other — which is what makes a multi-worker external proxy
// (and node failover) coherent, unlike a per-process random secret. manifestKeyHex
// is the tenant manifest key (hex); an empty or non-hex key yields nil (the caller
// treats that as "no secret available").
//
// HMAC-SHA256 is used directly as the KDF (no HKDF-Extract): the manifest key is
// already a uniformly random 32-byte root, so expand alone is sound.
func MmdsSecret(manifestKeyHex, sandboxID string) []byte {
	mk, err := hex.DecodeString(manifestKeyHex)
	if err != nil || len(mk) == 0 {
		return nil
	}
	mac := hmac.New(sha256.New, mk)
	mac.Write([]byte(mmdsSecretInfo + sandboxID))
	return mac.Sum(nil)
}
