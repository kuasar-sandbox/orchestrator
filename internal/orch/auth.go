package orch

import (
	"context"
	"encoding/hex"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// verifyKey reports whether apiKey was genuinely minted from apiSecretHex.
func verifyKey(apiKey, apiSecretHex string) bool {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return false
	}
	raw, err := hex.DecodeString(apiSecretHex)
	if err != nil || len(raw) != 32 {
		return false
	}
	return apikey.Verify(p, raw)
}

// ownsSandbox / ownsBuild are the per-resource ownership tests: the API key must
// verify against the resource's own (decrypted) APISecret. A non-existent row
// or a non-matching key is treated as not-owned (=> not-found, no existence leak).
func ownsSandbox(sb *types.Sandbox, apiKey string) bool {
	return sb != nil && verifyKey(apiKey, sb.APISecret)
}

func ownsBuild(b *types.Build, apiKey string) bool {
	return b != nil && verifyKey(apiKey, b.APISecret)
}

// ownerHash is the truncated APISecret fingerprint embedded in an API key. It
// is only a non-unique List pre-filter; callers still verify the MAC per row.
func ownerHash(apiKey string) string {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(p.CandidateFingerprint)
}

// resolveAllowed resolves an API key to one allowlisted APISecret/ManifestKey
// pair. The token fingerprint only selects candidates; the MAC check is
// authoritative. A zero pair means the key is not allowlisted.
func (o *Orchestrator) resolveAllowed(ctx context.Context, apiKey string) (store.KeyPair, error) {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return store.KeyPair{}, nil
	}
	candidates, err := o.st.AllowedKeyPairsByAPISecretHashPrefix(ctx, hex.EncodeToString(p.CandidateFingerprint))
	if err != nil {
		return store.KeyPair{}, err
	}
	for _, pair := range candidates {
		if raw, err := hex.DecodeString(pair.APISecret); err == nil && apikey.Verify(p, raw) {
			return pair, nil
		}
	}
	return store.KeyPair{}, nil
}
