package orch

// Admin plane: thin manifest-key allowlist wrappers the local control socket's
// admin plane (internal/configsock) calls. They validate the key, compute its
// fingerprint, and delegate to the store — so the daemon is the sole writer of the
// manifest_keys table (the manifest-key CLI is now a socket client, not a second
// process opening the DB).

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// AddManifestKey adds key (64-hex) to the allowlist, or refreshes it if it already
// exists (added=false). ttlSec>0 expires it after ttlSec; <=0 = never. registryAuth
// (a docker config.json; "" = leave) is the tenant's default registry pull creds.
// Returns the key's 24-hex fingerprint.
func (o *Orchestrator) AddManifestKey(ctx context.Context, key, label string, ttlSec int64, registryAuth string) (bool, string, error) {
	fp, err := fingerprintHex(key)
	if err != nil {
		return false, "", err
	}
	added, err := o.st.AddManifestKey(ctx, key, label, ttlSec, registryAuth)
	return added, fp, err
}

// RemoveManifestKey removes key from the allowlist; removed=false means it was absent.
func (o *Orchestrator) RemoveManifestKey(ctx context.Context, key string) (bool, string, error) {
	fp, err := fingerprintHex(key)
	if err != nil {
		return false, "", err
	}
	n, err := o.st.RemoveManifestKey(ctx, key)
	return n > 0, fp, err
}

// HasManifestKey reports whether key is in the allowlist.
func (o *Orchestrator) HasManifestKey(ctx context.Context, key string) (bool, string, error) {
	fp, err := fingerprintHex(key)
	if err != nil {
		return false, "", err
	}
	ok, err := o.st.HasManifestKey(ctx, key)
	return ok, fp, err
}

// ListManifestKeys returns the allowlist as fingerprint-only entries.
func (o *Orchestrator) ListManifestKeys(ctx context.Context) ([]configsock.AdminKeyInfo, error) {
	infos, err := o.st.ListManifestKeys(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]configsock.AdminKeyInfo, 0, len(infos))
	for _, mi := range infos {
		out = append(out, configsock.AdminKeyInfo{Fingerprint: mi.Hash, Label: mi.Label, CreatedUnix: mi.CreatedUnix, ExpiresUnix: mi.ExpiresUnix})
	}
	return out, nil
}

// fingerprintHex validates a 64-hex manifest key and returns its 24-hex fingerprint.
func fingerprintHex(manifestKeyHex string) (string, error) {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("manifest-key: %q is not a 64-hex (32-byte) key", manifestKeyHex)
	}
	return hex.EncodeToString(apikey.Fingerprint(raw)), nil
}
