package orch

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// verifyKey reports whether apiKey was genuinely minted from the AuthKey
// (hex): parse the token, then recompute + compare the MAC against the key.
func verifyKey(apiKey, authKeyHex string) bool {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return false
	}
	raw, err := hex.DecodeString(authKeyHex)
	if err != nil {
		return false
	}
	return apikey.Verify(p, raw)
}

// ownsSandbox / ownsBuild are the per-resource ownership tests: the api key must
// verify against the resource's own decrypted AuthKey. A non-existent row
// or a non-matching key is treated as not-owned (=> not-found, no existence leak).
func ownsSandbox(sb *types.Sandbox, apiKey string) bool {
	return sb != nil && verifyKey(apiKey, sb.AuthKey)
}

func ownsBuild(b *types.Build, apiKey string) bool {
	return b != nil && verifyKey(apiKey, b.AuthKey)
}

// ownerHash is the auth_key_hash an api key scopes to — the non-unique
// pre-filter for List (callers still verify the MAC per row).
func ownerHash(apiKey string) string {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(p.FP)
}

// resolveAllowed resolves an API key to one unexpired dual-key lease. AuthKey
// verifies the caller; ManifestKey is returned only as content key material.
func (o *Orchestrator) resolveAllowed(ctx context.Context, apiKey string) (store.KeyLease, error) {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return store.KeyLease{}, nil
	}
	candidates, err := o.st.KeyLeasesByAuthHash(ctx, hex.EncodeToString(p.FP))
	if err != nil {
		return store.KeyLease{}, err
	}
	var resolved store.KeyLease
	for _, lease := range candidates {
		if raw, err := hex.DecodeString(lease.AuthKey); err == nil && apikey.Verify(p, raw) {
			if resolved.AuthKey != "" && (resolved.Group != lease.Group ||
				resolved.AuthKey != lease.AuthKey || resolved.ManifestKey != lease.ManifestKey ||
				resolved.RegistryAuth != lease.RegistryAuth) {
				return store.KeyLease{}, errors.New("orch: API key matches multiple node key leases")
			}
			resolved = lease
		}
	}
	return resolved, nil
}
