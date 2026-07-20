package orch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

// NodeKeyMaterialResolver resolves Provider references on the node side of the
// authenticated node-link. Inline delivery does not use this interface.
type NodeKeyMaterialResolver interface {
	ResolveKey(context.Context, string) (string, error)
	ResolveRegistryAuth(context.Context, string) (string, error)
}

func (o *Orchestrator) SetNodeKeyMaterialResolver(resolver NodeKeyMaterialResolver) {
	o.keyResolver = resolver
}

func (o *Orchestrator) putClusterKeyLease(
	ctx context.Context,
	wire routesync.NodeKeyLeaseV1,
) (routesync.NodeKeyLeaseRefV1, error) {
	if err := wire.Validate(); err != nil {
		return routesync.NodeKeyLeaseRefV1{}, err
	}
	if wire.ExpiresUnix <= time.Now().Unix() {
		return routesync.NodeKeyLeaseRefV1{}, errors.New("cluster: key lease is already expired")
	}
	authKey, err := o.resolveNodeKeyMaterial(ctx, wire.AuthKey)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve AuthKey: %w", err)
	}
	manifestKey, err := o.resolveNodeKeyMaterial(ctx, wire.ManifestKey)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve ManifestKey: %w", err)
	}
	registryAuth, err := o.resolveNodeRegistryAuth(ctx, wire.RegistryAuth)
	if err != nil {
		return routesync.NodeKeyLeaseRefV1{}, fmt.Errorf("cluster: resolve registry auth: %w", err)
	}
	if _, err := o.st.PutKeyLease(ctx, store.KeyLease{
		Group: wire.Group, AuthKey: authKey, ManifestKey: manifestKey,
		RegistryAuth: registryAuth, Label: "cluster", ExpiresUnix: wire.ExpiresUnix,
	}); err != nil {
		return routesync.NodeKeyLeaseRefV1{}, err
	}
	ref := routesync.NodeKeyLeaseRefV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: wire.Group,
		AuthKeyFingerprint:     wire.AuthKey.Fingerprint,
		ManifestKeyFingerprint: wire.ManifestKey.Fingerprint,
	}
	return ref, ref.Validate()
}

func (o *Orchestrator) resolveNodeKeyMaterial(
	ctx context.Context,
	material routesync.NodeKeyMaterialV1,
) (string, error) {
	switch material.Type {
	case routesync.KeyMaterialInline:
		return material.Value, nil
	case routesync.KeyMaterialRef:
		if o.keyResolver == nil {
			return "", errors.New("Provider key reference resolver is not configured")
		}
		value, err := o.keyResolver.ResolveKey(ctx, material.Ref)
		if err != nil {
			return "", err
		}
		fingerprint, err := keyFingerprint("resolved key", value)
		if err != nil {
			return "", err
		}
		if fingerprint != material.Fingerprint {
			return "", errors.New("resolved key fingerprint mismatch")
		}
		return value, nil
	default:
		return "", errors.New("unsupported key material type")
	}
}

func (o *Orchestrator) resolveNodeRegistryAuth(
	ctx context.Context,
	auth routesync.NodeRegistryAuthV1,
) (string, error) {
	switch auth.Type {
	case "":
		return "", nil
	case routesync.KeyMaterialInline:
		return auth.Value, nil
	case routesync.KeyMaterialRef:
		if o.keyResolver == nil {
			return "", errors.New("Provider registry auth resolver is not configured")
		}
		return o.keyResolver.ResolveRegistryAuth(ctx, auth.Ref)
	default:
		return "", errors.New("unsupported registry auth material type")
	}
}

func (o *Orchestrator) resolveByFingerprints(
	ctx context.Context,
	group, authFingerprint, manifestFingerprint string,
) (store.KeyLease, error) {
	if group == "" || authFingerprint == "" || manifestFingerprint == "" {
		return store.KeyLease{}, errors.New("cluster: group and both key fingerprints are required")
	}
	lease, found, err := o.st.KeyLeaseByFingerprints(ctx, group, authFingerprint, manifestFingerprint)
	if err != nil {
		return store.KeyLease{}, err
	}
	if !found {
		return store.KeyLease{}, fmt.Errorf("cluster: exact node key lease is absent or expired for group %q", group)
	}
	return lease, nil
}
