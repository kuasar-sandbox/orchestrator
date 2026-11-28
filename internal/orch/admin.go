package orch

// Admin plane: tenant APISecret/ManifestKey pair allowlist wrappers. The local
// control socket remains the only writer of the encrypted node key table.

import (
	"context"
	"encoding/hex"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// materializeKeyPair validates a manifest key and either validates the supplied
// APISecret or derives its default. Derivation happens only at this ingestion
// boundary; request paths always consume a complete persisted pair.
func materializeKeyPair(ctx context.Context, manifestKey, apiSecret string) (store.KeyPair, string, string, error) {
	pair, err := store.ResolveKeyPair(ctx, store.KeyPair{ManifestKey: manifestKey, APISecret: apiSecret})
	if err != nil {
		return store.KeyPair{}, "", "", err
	}
	if apiSecret == "" {
		rawManifest, _ := pair.ManifestKeyBytes()
		apiSecret = hex.EncodeToString(apikey.DeriveAPISecret(rawManifest[:]))
		clear(rawManifest[:])
	}
	apiFP, err := store.APISecretHash(apiSecret)
	if err != nil {
		return store.KeyPair{}, "", "", err
	}
	pair.APISecret = apiSecret
	return pair, apiFP, pair.ManifestKeyFingerprint, nil
}

// AddKeyPair adds or refreshes one exact tenant credential pair. An empty
// apiSecret selects the deterministic default derived from manifestKey.
func (o *Orchestrator) AddKeyPair(ctx context.Context, manifestKey, apiSecret, label string, ttlSec int64, registryAuth string) (bool, string, string, error) {
	pair, apiFP, manifestFP, err := materializeKeyPair(ctx, manifestKey, apiSecret)
	if err != nil {
		return false, "", "", err
	}
	added, err := o.st.AddKeyPair(ctx, pair, label, ttlSec, registryAuth)
	return added, apiFP, manifestFP, err
}

func (o *Orchestrator) RemoveKeyPair(ctx context.Context, manifestKey, apiSecret string) (bool, string, string, error) {
	pair, apiFP, manifestFP, err := materializeKeyPair(ctx, manifestKey, apiSecret)
	if err != nil {
		return false, "", "", err
	}
	n, err := o.st.RemoveKeyPair(ctx, pair)
	return n > 0, apiFP, manifestFP, err
}

func (o *Orchestrator) HasKeyPair(ctx context.Context, manifestKey, apiSecret string) (bool, string, string, error) {
	pair, apiFP, manifestFP, err := materializeKeyPair(ctx, manifestKey, apiSecret)
	if err != nil {
		return false, "", "", err
	}
	ok, err := o.st.HasKeyPair(ctx, pair)
	return ok, apiFP, manifestFP, err
}

func (o *Orchestrator) ListKeyPairs(ctx context.Context) ([]configsock.AdminKeyInfo, error) {
	infos, err := o.st.ListKeyPairs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]configsock.AdminKeyInfo, 0, len(infos))
	for _, info := range infos {
		out = append(out, configsock.AdminKeyInfo{
			APISecretFingerprint:   info.APISecretHash,
			ManifestKeyFingerprint: info.ManifestKeyHash,
			Label:                  info.Label,
			CreatedUnix:            info.CreatedUnix,
			ExpiresUnix:            info.ExpiresUnix,
		})
	}
	return out, nil
}
