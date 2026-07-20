package orch

// The local admin plane manages complete node key leases. The daemon remains
// the sole writer of encrypted key material; socket clients receive only
// fingerprints.

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func (o *Orchestrator) PutKeyLease(
	ctx context.Context,
	group, authKey, manifestKey, label string,
	ttlSec int64,
	registryAuth string,
) (bool, string, string, error) {
	authFP, manifestFP, err := keyFingerprints(authKey, manifestKey)
	if err != nil {
		return false, "", "", err
	}
	if ttlSec < 0 {
		return false, "", "", fmt.Errorf("key-lease: ttl must not be negative")
	}
	expires := int64(0)
	if ttlSec > 0 {
		now := time.Now().Unix()
		if ttlSec > int64(^uint64(0)>>1)-now {
			return false, "", "", fmt.Errorf("key-lease: ttl overflows unix time")
		}
		expires = now + ttlSec
	}
	added, err := o.st.PutKeyLease(ctx, store.KeyLease{
		Group: group, AuthKey: authKey, ManifestKey: manifestKey,
		RegistryAuth: registryAuth, Label: label, ExpiresUnix: expires,
	})
	return added, authFP, manifestFP, err
}

func (o *Orchestrator) DropKeyLease(ctx context.Context, group, authKey, manifestKey string) (bool, string, string, error) {
	authFP, manifestFP, err := keyFingerprints(authKey, manifestKey)
	if err != nil {
		return false, "", "", err
	}
	removed, err := o.st.RemoveKeyLease(ctx, group, authKey, manifestKey)
	return removed, authFP, manifestFP, err
}

func (o *Orchestrator) HasKeyLease(ctx context.Context, group, authKey, manifestKey string) (bool, string, string, error) {
	authFP, manifestFP, err := keyFingerprints(authKey, manifestKey)
	if err != nil {
		return false, "", "", err
	}
	present, err := o.st.HasKeyLease(ctx, group, authKey, manifestKey)
	return present, authFP, manifestFP, err
}

func (o *Orchestrator) ListKeyLeases(ctx context.Context) ([]configsock.AdminKeyLeaseInfo, error) {
	infos, err := o.st.ListKeyLeases(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]configsock.AdminKeyLeaseInfo, 0, len(infos))
	for _, info := range infos {
		out = append(out, configsock.AdminKeyLeaseInfo{
			Group:                  info.Group,
			AuthKeyFingerprint:     info.AuthKeyFingerprint,
			ManifestKeyFingerprint: info.ManifestKeyFingerprint,
			Label:                  info.Label, CreatedUnix: info.CreatedUnix, ExpiresUnix: info.ExpiresUnix,
		})
	}
	return out, nil
}

func keyFingerprints(authKey, manifestKey string) (string, string, error) {
	authFP, err := keyFingerprint("AuthKey", authKey)
	if err != nil {
		return "", "", err
	}
	manifestFP, err := keyFingerprint("ManifestKey", manifestKey)
	if err != nil {
		return "", "", err
	}
	if authKey == manifestKey {
		return "", "", fmt.Errorf("key-lease: AuthKey and ManifestKey must be different")
	}
	return authFP, manifestFP, nil
}

func keyFingerprint(name, keyHex string) (string, error) {
	raw, err := hex.DecodeString(keyHex)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != keyHex {
		return "", fmt.Errorf("key-lease: %s must be canonical 64-character lowercase hex", name)
	}
	return hex.EncodeToString(apikey.Fingerprint(raw)), nil
}
