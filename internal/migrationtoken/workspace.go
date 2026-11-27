package migrationtoken

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

const workspacePrefix = "kwt1."

// Workspace tokens authenticate the entire disk graph. Separate the AEAD key
// by protocol and by both tenant credentials, even for tenants sharing a
// manifest key. The prefix is authenticated and cannot be changed to kmt1.
func workspaceKey(m KeyMaterial) ([]byte, error) {
	return workspaceKeyContext(context.Background(), m)
}

func workspaceKeyContext(ctx context.Context, m KeyMaterial) ([]byte, error) {
	api, ok := decodeCanonicalHex32(m.APISecret)
	if !ok {
		return nil, ErrInvalidKeyMaterial
	}
	defer clear(api)
	key, err := resolveManifest(ctx, m.ManifestKey)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("kuasar-workspace-token-v1\x00"))
	mac.Write(api)
	return mac.Sum(nil), nil
}

// ValidateWorkspaceDisk limits transfer to immutable, independently addressable
// layers. A mutable diff template or an owner-local path cannot cross nodes.
func ValidateWorkspaceDisk(d rtconfig.DiskV1) error {
	if d.Validate() != nil || d.Size == nil || d.Base == "" || d.DiffTemplate != "" {
		return ErrInvalidPayload
	}
	for _, raw := range append([]string{d.Base}, d.BaseFromRefs...) {
		ref, err := types.ParsePortableRef(raw)
		if err != nil || ref.Scheme == manifest.RefSchemeFile && ref.Digest == "" {
			return ErrInvalidPayload
		}
	}
	return nil
}

func SealWorkspace(m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	return SealWorkspaceContext(context.Background(), m, d)
}

// SealWorkspaceContext is SealWorkspace with cancellable ManifestKey resolution.
func SealWorkspaceContext(ctx context.Context, m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	return sealWorkspaceContext(ctx, rand.Reader, m, d)
}

func sealWorkspace(random io.Reader, m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	return sealWorkspaceContext(context.Background(), random, m, d)
}

func sealWorkspaceContext(ctx context.Context, random io.Reader, m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	if err := ValidateWorkspaceDisk(d); err != nil {
		return "", err
	}
	key, err := workspaceKeyContext(ctx, m)
	if err != nil {
		return "", err
	}
	defer clear(key)
	raw, err := json.Marshal(d)
	if err != nil {
		return "", ErrInvalidPayload
	}
	return sealEnvelope(random, workspacePrefix, key, raw, []byte(workspacePrefix))
}

func OpenWorkspace(m KeyMaterial, token string) (rtconfig.DiskV1, error) {
	return OpenWorkspaceContext(context.Background(), m, token)
}

// OpenWorkspaceContext is OpenWorkspace with cancellable ManifestKey resolution.
func OpenWorkspaceContext(ctx context.Context, m KeyMaterial, token string) (rtconfig.DiskV1, error) {
	var disk rtconfig.DiskV1
	key, err := workspaceKeyContext(ctx, m)
	if err != nil {
		return disk, err
	}
	defer clear(key)
	raw, err := openEnvelope(token, workspacePrefix, key, []byte(workspacePrefix))
	if err != nil {
		return disk, err
	}
	if json.Unmarshal(raw, &disk) != nil || ValidateWorkspaceDisk(disk) != nil {
		return rtconfig.DiskV1{}, ErrInvalidPayload
	}
	return disk, nil
}
