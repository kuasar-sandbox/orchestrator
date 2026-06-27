package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const accessTokenInfo = "kuasar-access-token-v1:"

// DeriveAccessToken deterministically derives the sandbox data-plane token from
// a group's auth_key and the opaque sandbox id. The token is intentionally opaque
// to callers; only registry/node/router need to agree on the value.
func DeriveAccessToken(authKeyHex, sandboxID string) (string, error) {
	key, err := hex.DecodeString(authKeyHex)
	if err != nil || len(key) == 0 {
		return "", fmt.Errorf("cluster: invalid auth_key")
	}
	if sandboxID == "" {
		return "", fmt.Errorf("cluster: empty sandbox_id")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(accessTokenInfo + sandboxID))
	return "sat_" + hex.EncodeToString(mac.Sum(nil)), nil
}
