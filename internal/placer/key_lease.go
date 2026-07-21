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
	record, found, err := provider.GetRecord(ctx, group)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	if !found || record.Group != group {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group is not present in Provider")
	}
	return nodeKeyLeaseFromRecord(record, expiresUnix)
}

func nodeKeyLeaseFromRecord(record clusterstate.SandboxGroupRecord, expiresUnix int64) (routesync.NodeKeyLeaseV1, error) {
	if record.Group == "" || expiresUnix <= 0 {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group record and key lease expiry are required")
	}
	if record.AuthKey.Value == "" {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group has no AuthKey")
	}
	if record.ManifestKey.Value == "" {
		return routesync.NodeKeyLeaseV1{}, errors.New("placer: group has no ManifestKey")
	}
	authKey, err := nodeKeyMaterial("AuthKey", record.AuthKey)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	manifestKey, err := nodeKeyMaterial("ManifestKey", record.ManifestKey)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	registryAuth, err := nodeRegistryAuth(record.RegistryAuth)
	if err != nil {
		return routesync.NodeKeyLeaseV1{}, err
	}
	lease := routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: record.Group,
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
		return routesync.NodeKeyMaterialV1{}, fmt.Errorf(
			"placer: referenced %s is not supported without a configured Provider materializer", name,
		)
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
		return routesync.NodeRegistryAuthV1{}, errors.New(
			"placer: referenced registry auth is not supported without a configured Provider materializer",
		)
	default:
		return routesync.NodeRegistryAuthV1{}, fmt.Errorf("placer: unsupported registry auth type %q", keyType)
	}
}
