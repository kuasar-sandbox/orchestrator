package placer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	DefaultNodeKeyLeaseTTL  = 3 * time.Hour
	NodeKeyLeaseRenewBefore = time.Hour
)

// ResolveNodeKeyLease builds the exact dual-key lease delivered to a selected
// node. Provider remains key authority; Registry only transports this value and
// records non-secret ACK metadata.
func ResolveNodeKeyLease(
	ctx context.Context,
	provider clusterstate.SandboxGroupProvider,
	group string,
	expiresUnix int64,
) (routesync.NodeKeyLeaseV1, error) {
	if provider == nil || group == "" || expiresUnix <= 0 {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: Provider, group, and key lease expiry are required")
	}
	groupConfig, found, err := provider.Get(ctx, group)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	if !found || groupConfig.Group != group {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group is not present in Provider")
	}
	authSecret, found, err := provider.GetAuthKey(ctx, group)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	if !found {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group has no AuthKey")
	}
	manifestSecret, found, err := provider.GetManifestKey(ctx, group)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	if !found {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group has no ManifestKey")
	}
	authKey, err := nodeKeyMaterial("AuthKey", authSecret)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	manifestKey, err := nodeKeyMaterial("ManifestKey", manifestSecret)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	registryAuth, err := nodeRegistryAuth(groupConfig.RegistryAuth)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: group,
		AuthKey: authKey, ManifestKey: manifestKey, RegistryAuth: registryAuth,
		ExpiresUnix: expiresUnix,
	}
	return lease, lease.Validate()
}

func nodeKeyMaterial(name string, secret clusterstate.Secret) (routesync.NodeKeyMaterialV1, error) {
	keyType := secret.Type
	if keyType == "" && secret.Value != "" {
		keyType = clusterstate.SecretInline
	}
	switch keyType {
	case clusterstate.SecretInline:
		raw, err := hex.DecodeString(secret.Value)
		if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != secret.Value {
			return routesync.NodeKeyMaterialV1{}, fmt.Errorf("placer: %s must be a canonical 32-byte hexadecimal key", name)
		}
		digest := sha256.Sum256(raw)
		fingerprint := hex.EncodeToString(digest[:12])
		if secret.Fingerprint != "" && secret.Fingerprint != fingerprint {
			return routesync.NodeKeyMaterialV1{}, fmt.Errorf("placer: %s fingerprint does not match inline key", name)
		}
		return routesync.NodeKeyMaterialV1{
			Type: routesync.KeyMaterialInline, Value: secret.Value, Fingerprint: fingerprint,
		}, nil
	case clusterstate.SecretRef:
		if secret.Value == "" || !validSecretFingerprint(secret.Fingerprint) {
			return routesync.NodeKeyMaterialV1{}, fmt.Errorf("placer: referenced %s requires a value and 24-character fingerprint", name)
		}
		return routesync.NodeKeyMaterialV1{
			Type: routesync.KeyMaterialRef, Ref: secret.Value, Fingerprint: secret.Fingerprint,
		}, nil
	default:
		return routesync.NodeKeyMaterialV1{}, fmt.Errorf("placer: unsupported %s secret type %q", name, keyType)
	}
}

func nodeRegistryAuth(secret clusterstate.Secret) (routesync.NodeRegistryAuthV1, error) {
	if secret.Type == "" && secret.Value == "" {
		return routesync.NodeRegistryAuthV1{}, nil
	}
	keyType := secret.Type
	if keyType == "" {
		keyType = clusterstate.SecretInline
	}
	switch keyType {
	case clusterstate.SecretInline:
		if secret.Value == "" {
			return routesync.NodeRegistryAuthV1{}, errors.New("placer: inline registry auth is empty")
		}
		return routesync.NodeRegistryAuthV1{Type: routesync.KeyMaterialInline, Value: secret.Value}, nil
	case clusterstate.SecretRef:
		if secret.Value == "" {
			return routesync.NodeRegistryAuthV1{}, errors.New("placer: registry auth reference is empty")
		}
		return routesync.NodeRegistryAuthV1{Type: routesync.KeyMaterialRef, Ref: secret.Value}, nil
	default:
		return routesync.NodeRegistryAuthV1{}, fmt.Errorf("placer: unsupported registry auth type %q", keyType)
	}
}

func validSecretFingerprint(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 12 && hex.EncodeToString(raw) == value
}
