package migrationtoken

import (
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
	api, ok := decodeCanonicalHex32(m.APISecret)
	if !ok {
		return nil, ErrInvalidKeyMaterial
	}
	key, ok := decodeCanonicalHex32(m.ManifestKey)
	if !ok {
		return nil, ErrInvalidKeyMaterial
	}
	mac := hmac.New(sha256.New, key)
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
		if err != nil || ref.Scheme == manifest.RefSchemeFile && (ref.Location == "" || ref.DigestScheme != "manifest") {
			return ErrInvalidPayload
		}
	}
	return nil
}

func SealWorkspace(m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	return sealWorkspace(rand.Reader, m, d)
}
func sealWorkspace(random io.Reader, m KeyMaterial, d rtconfig.DiskV1) (string, error) {
	if err := ValidateWorkspaceDisk(d); err != nil {
		return "", err
	}
	key, err := workspaceKey(m)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return "", ErrInvalidPayload
	}
	return sealEnvelope(random, workspacePrefix, key, raw, []byte(workspacePrefix))
}

func OpenWorkspace(m KeyMaterial, token string) (rtconfig.DiskV1, error) {
	var disk rtconfig.DiskV1
	key, err := workspaceKey(m)
	if err != nil {
		return disk, err
	}
	raw, err := openEnvelope(token, workspacePrefix, key, []byte(workspacePrefix))
	if err != nil {
		return disk, err
	}
	if json.Unmarshal(raw, &disk) != nil || ValidateWorkspaceDisk(disk) != nil {
		return rtconfig.DiskV1{}, ErrInvalidPayload
	}
	return disk, nil
}
