//go:build fdb && cgo

package fdbdriver

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func (s *Store) keyPairScope(nodeID string) fdb.Key { return s.key("key-pair", nodeID) }
func (s *Store) keyPairKey(nodeID, fingerprint string) fdb.Key {
	return mainKey(s.keyPairScope(nodeID), fingerprint)
}
func (s *Store) PutKeyPair(ctx context.Context, nodeID string, pair metadata.NodeKeyPair) (metadata.Versioned[metadata.NodeKeyPair], error) {
	if err := validateParts(nodeID); err != nil {
		return metadata.Versioned[metadata.NodeKeyPair]{}, err
	}
	normalized, err := metadata.NormalizeKeyPair(pair)
	if err != nil {
		return metadata.Versioned[metadata.NodeKeyPair]{}, err
	}
	var out metadata.NodeKeyPair
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardLive(m, nodeID); err != nil {
			return false, err
		}
		current, err := getTyped[metadata.NodeKeyPair](m, s.keyPairKey(nodeID, normalized.APISecretFingerprint))
		if err != nil {
			return false, err
		}
		out = normalized
		if current.Exists {
			if !metadata.SameKeyPairMaterial(current.Value, normalized) {
				return false, metadata.ErrOwnershipConflict
			}
			if out.AckedExpiresUnix == 0 {
				out.AckedExpiresUnix = current.Value.AckedExpiresUnix
			}
		}
		return true, putJSON(m.tr, s.keyPairKey(nodeID, out.APISecretFingerprint), out)
	})
	if err != nil {
		return metadata.Versioned[metadata.NodeKeyPair]{}, err
	}
	return metadata.Versioned[metadata.NodeKeyPair]{Exists: true, Version: version, Value: out}, nil
}
func (s *Store) AckKeyPair(ctx context.Context, nodeID string, pair metadata.NodeKeyPair) (bool, error) {
	if err := validateParts(nodeID); err != nil {
		return false, err
	}
	normalized, err := metadata.NormalizeKeyPair(pair)
	if err != nil {
		return false, err
	}
	if normalized.ExpiresUnix <= 0 {
		return false, nil
	}
	matched := false
	_, err = s.mutate(ctx, func(m *mutation) (bool, error) {
		matched = false
		current, err := getTyped[metadata.NodeKeyPair](m, s.keyPairKey(nodeID, normalized.APISecretFingerprint))
		if err != nil || !current.Exists {
			return false, err
		}
		if current.Value.ExpiresUnix != normalized.ExpiresUnix || !metadata.SameKeyPairMaterial(current.Value, normalized) {
			return false, nil
		}
		matched = true
		if current.Value.AckedExpiresUnix >= normalized.ExpiresUnix {
			return false, nil
		}
		current.Value.AckedExpiresUnix = normalized.ExpiresUnix
		return true, putJSON(m.tr, s.keyPairKey(nodeID, normalized.APISecretFingerprint), current.Value)
	})
	return matched && err == nil, err
}
func (s *Store) DropKeyPair(ctx context.Context, nodeID, fingerprint string, expected metadata.Version) error {
	if err := validateParts(nodeID); err != nil {
		return err
	}
	if !metadata.ValidFingerprint(fingerprint) {
		return invalidRecord("key pair fingerprint")
	}
	return s.removeOwner(ctx, s.keyPairKey(nodeID, fingerprint), expected)
}
func (s *Store) ListKeyPairs(ctx context.Context, nodeID string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeKeyPair]], error) {
	if err := validateParts(nodeID); err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeKeyPair]]{}, err
	}
	page, err := listTyped[metadata.NodeKeyPair](ctx, s, s.keyPairScope(nodeID), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeKeyPair]]{}, err
	}
	for _, item := range page.Items {
		if _, err = metadata.NormalizeKeyPair(item.Value); err != nil {
			return metadata.Page[metadata.Versioned[metadata.NodeKeyPair]]{}, err
		}
	}
	return page, nil
}
