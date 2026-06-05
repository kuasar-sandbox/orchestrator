package orch

import (
	"context"
	"encoding/hex"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

// verifyKey reports whether apiKey was genuinely minted from the manifest key
// (hex): parse the token, then recompute + compare the MAC against the key.
func verifyKey(apiKey, manifestKeyHex string) bool {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return false
	}
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil {
		return false
	}
	return apikey.Verify(p, raw)
}

// ownsSandbox / ownsBuild are the per-resource ownership tests: the api key must
// verify against the resource's own (decrypted) manifest key. A non-existent row
// or a non-matching key is treated as not-owned (=> not-found, no existence leak).
func ownsSandbox(sb *types.Sandbox, apiKey string) bool {
	return sb != nil && verifyKey(apiKey, sb.ManifestKey)
}

func ownsBuild(b *types.Build, apiKey string) bool {
	return b != nil && verifyKey(apiKey, b.ManifestKey)
}

// ownerHash is the manifest_key_hash an api key scopes to — the non-unique
// pre-filter for List (callers still verify the MAC per row).
func ownerHash(apiKey string) string {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(p.FP)
}

// resolveAllowed resolves an api key to an allowlisted manifest key (for
// create/build): match by fingerprint in the manifest_keys table, then verify the
// MAC. Returns "" (not an error) when the key is valid-looking but not allowed.
func (o *Orchestrator) resolveAllowed(ctx context.Context, apiKey string) (string, error) {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return "", nil
	}
	candidates, err := o.st.AllowedManifestKeysByHash(ctx, hex.EncodeToString(p.FP))
	if err != nil {
		return "", err
	}
	for _, mk := range candidates {
		if raw, err := hex.DecodeString(mk); err == nil && apikey.Verify(p, raw) {
			return mk, nil
		}
	}
	return "", nil
}
