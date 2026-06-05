// Package keys mints sandbox access tokens. (Tenant identity is the manifest key
// — see internal/apikey for api-key derivation/verification and internal/secretbox
// for at-rest encryption.)
package keys

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// MintToken returns a random 32-byte hex token (envd / traffic access tokens).
// We own both the minting and verifying ends, so any unguessable value works.
func MintToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("keys: mint token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
