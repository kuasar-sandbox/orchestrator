package routesync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const NodeKeyLeaseVersionV1 uint16 = 1

const (
	KeyMaterialInline = "inline"
	KeyMaterialRef    = "ref"
)

// NodeKeyMaterialV1 carries one node key domain over the authenticated
// node-link. Inline values never enter consensus state; references are resolved
// by the node before it durably ACKs the lease.
type NodeKeyMaterialV1 struct {
	Type        string `json:"type"`
	Value       string `json:"value,omitempty"`
	Ref         string `json:"ref,omitempty"`
	Fingerprint string `json:"fingerprint"`
}

func (m NodeKeyMaterialV1) validate(name string) error {
	if !validNodeKeyFingerprint(m.Fingerprint) {
		return fmt.Errorf("routesync: %s fingerprint must be 24 lowercase hexadecimal characters", name)
	}
	switch m.Type {
	case KeyMaterialInline:
		if m.Value == "" || m.Ref != "" {
			return fmt.Errorf("routesync: inline %s requires only a value", name)
		}
		raw, err := hex.DecodeString(m.Value)
		if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != m.Value {
			return fmt.Errorf("routesync: inline %s must be a canonical 32-byte hexadecimal key", name)
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:12]) != m.Fingerprint {
			return fmt.Errorf("routesync: inline %s fingerprint does not match its value", name)
		}
	case KeyMaterialRef:
		if m.Ref == "" || m.Value != "" {
			return fmt.Errorf("routesync: referenced %s requires only a ref", name)
		}
	default:
		return fmt.Errorf("routesync: %s type must be inline or ref", name)
	}
	return nil
}

// NodeRegistryAuthV1 is optional pull authorization delivered with a group key
// lease. It is a capability or reference, never part of Route/Build Raft state.
type NodeRegistryAuthV1 struct {
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
	Ref   string `json:"ref,omitempty"`
}

func (a NodeRegistryAuthV1) validate() error {
	if a.Type == "" && a.Value == "" && a.Ref == "" {
		return nil
	}
	switch a.Type {
	case KeyMaterialInline:
		if a.Value == "" || a.Ref != "" {
			return errors.New("routesync: inline registry auth requires only a value")
		}
	case KeyMaterialRef:
		if a.Ref == "" || a.Value != "" {
			return errors.New("routesync: referenced registry auth requires only a ref")
		}
	default:
		return errors.New("routesync: registry auth type must be inline or ref")
	}
	return nil
}

// NodeKeyLeaseV1 is the exact, TTL-bounded key bundle a node must durably install
// and ACK before it can accept admission for the group.
type NodeKeyLeaseV1 struct {
	Version      uint16             `json:"version"`
	Group        string             `json:"group"`
	KeyRevision  uint64             `json:"key_revision"`
	AuthKey      NodeKeyMaterialV1  `json:"auth_key"`
	ManifestKey  NodeKeyMaterialV1  `json:"manifest_key"`
	RegistryAuth NodeRegistryAuthV1 `json:"registry_auth,omitempty"`
	ExpiresUnix  int64              `json:"expires_unix"`
}

func (l NodeKeyLeaseV1) Validate() error {
	if l.Version != NodeKeyLeaseVersionV1 || l.Group == "" || l.KeyRevision == 0 || l.ExpiresUnix <= 0 {
		return errors.New("routesync: key lease requires version, group, key revision, and positive expiry")
	}
	if err := l.AuthKey.validate("AuthKey"); err != nil {
		return err
	}
	if err := l.ManifestKey.validate("ManifestKey"); err != nil {
		return err
	}
	if l.AuthKey.Fingerprint == l.ManifestKey.Fingerprint {
		return errors.New("routesync: AuthKey and ManifestKey must use separate key material")
	}
	return l.RegistryAuth.validate()
}

// NodeKeyLeaseRefV1 identifies the exact lease removed by key_drop. A stale drop
// cannot remove a newly rotated lease with different fingerprints.
type NodeKeyLeaseRefV1 struct {
	Version                uint16 `json:"version"`
	Group                  string `json:"group"`
	KeyRevision            uint64 `json:"key_revision"`
	AuthKeyFingerprint     string `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	RegistryAuthDigest     string `json:"registry_auth_digest"`
}

func (r NodeKeyLeaseRefV1) Validate() error {
	if r.Version != NodeKeyLeaseVersionV1 || r.Group == "" || r.KeyRevision == 0 ||
		!validNodeKeyFingerprint(r.AuthKeyFingerprint) ||
		!validNodeKeyFingerprint(r.ManifestKeyFingerprint) || !validNodeDigest(r.RegistryAuthDigest) {
		return errors.New("routesync: invalid key lease reference")
	}
	if r.AuthKeyFingerprint == r.ManifestKeyFingerprint {
		return errors.New("routesync: AuthKey and ManifestKey must use separate key material")
	}
	return nil
}

func (l NodeKeyLeaseV1) Ref() (NodeKeyLeaseRefV1, error) {
	if err := l.Validate(); err != nil {
		return NodeKeyLeaseRefV1{}, err
	}
	raw, err := json.Marshal(struct {
		Type  string `json:"type"`
		Value string `json:"value,omitempty"`
		Ref   string `json:"ref,omitempty"`
	}{Type: l.RegistryAuth.Type, Value: l.RegistryAuth.Value, Ref: l.RegistryAuth.Ref})
	if err != nil {
		return NodeKeyLeaseRefV1{}, err
	}
	digest := sha256.Sum256(append([]byte("kuasar-registry-auth-v1\x00"), raw...))
	ref := NodeKeyLeaseRefV1{
		Version: NodeKeyLeaseVersionV1, Group: l.Group, KeyRevision: l.KeyRevision,
		AuthKeyFingerprint: l.AuthKey.Fingerprint, ManifestKeyFingerprint: l.ManifestKey.Fingerprint,
		RegistryAuthDigest: hex.EncodeToString(digest[:]),
	}
	return ref, ref.Validate()
}

func validNodeKeyFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 12 && hex.EncodeToString(decoded) == value
}

func validNodeDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}
