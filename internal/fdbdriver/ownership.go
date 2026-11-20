//go:build fdb && cgo

package fdbdriver

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func (s *Store) sandboxOwnerScope(nodeID string) fdb.Key { return s.key("sandbox-owner", nodeID) }
func (s *Store) sandboxOwnerKey(nodeID, sandboxID string) fdb.Key {
	return mainKey(s.sandboxOwnerScope(nodeID), sandboxID)
}
func (s *Store) buildOwnerScope(nodeID string) fdb.Key { return s.key("build-owner", nodeID) }
func (s *Store) buildOwnerKey(nodeID, buildID string) fdb.Key {
	return mainKey(s.buildOwnerScope(nodeID), buildID)
}
func (s *Store) ensureSandbox(m *mutation, nodeID string, ref metadata.NodeSandboxRef) error {
	current, err := getTyped[metadata.NodeSandboxRef](m, s.sandboxOwnerKey(nodeID, ref.NodeSandboxID))
	if err != nil {
		return err
	}
	if current.Exists && current.Value != ref {
		return metadata.ErrOwnershipConflict
	}
	return putJSON(m.tr, s.sandboxOwnerKey(nodeID, ref.NodeSandboxID), ref)
}
func (s *Store) ensureBuild(m *mutation, nodeID string, ref metadata.NodeBuildRef) error {
	current, err := getTyped[metadata.NodeBuildRef](m, s.buildOwnerKey(nodeID, ref.BuildID))
	if err != nil {
		return err
	}
	if current.Exists && current.Value != ref {
		if current.Value.Group != ref.Group || current.Value.BuildID != ref.BuildID || current.Value.TemplateID != "" {
			return metadata.ErrOwnershipConflict
		}
		build, err := getTyped[metadata.BuildRecord](m, s.buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID}))
		if err != nil {
			return err
		}
		if !build.Exists || build.Value.NodeID != nodeID || build.Value.TemplateID != ref.TemplateID {
			return metadata.ErrOwnershipConflict
		}
	}
	return putJSON(m.tr, s.buildOwnerKey(nodeID, ref.BuildID), ref)
}
func (s *Store) clearMatchingSandbox(m *mutation, nodeID string, ref metadata.NodeSandboxRef) (bool, error) {
	key := s.sandboxOwnerKey(nodeID, ref.NodeSandboxID)
	current, err := getTyped[metadata.NodeSandboxRef](m, key)
	if err != nil {
		return false, err
	}
	if !current.Exists || current.Value != ref {
		return false, nil
	}
	clearRecord(m.tr, key)
	return true, nil
}
func (s *Store) clearMatchingBuild(m *mutation, nodeID string, ref metadata.NodeBuildRef) (bool, error) {
	key := s.buildOwnerKey(nodeID, ref.BuildID)
	current, err := getTyped[metadata.NodeBuildRef](m, key)
	if err != nil {
		return false, err
	}
	if !current.Exists || current.Value != ref {
		return false, nil
	}
	clearRecord(m.tr, key)
	return true, nil
}
func (s *Store) ReadSandboxOwner(ctx context.Context, nodeID, sandboxID string) (out metadata.Versioned[metadata.NodeSandboxRef], err error) {
	if err = validateParts(nodeID, sandboxID); err != nil {
		return
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		var e error
		out, e = readTyped[metadata.NodeSandboxRef](tr, s.sandboxOwnerKey(nodeID, sandboxID))
		if e == nil && out.Exists {
			e = metadata.ValidateSandboxRef(out.Value)
			if out.Value.NodeSandboxID != sandboxID {
				return invalidRecord("ownership key mismatch")
			}
		}
		return e
	})
	return
}
func (s *Store) ReadBuildOwner(ctx context.Context, nodeID, buildID string) (out metadata.Versioned[metadata.NodeBuildRef], err error) {
	if err = validateParts(nodeID, buildID); err != nil {
		return
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		var e error
		out, e = readTyped[metadata.NodeBuildRef](tr, s.buildOwnerKey(nodeID, buildID))
		if e == nil && out.Exists && (out.Value.BuildID != buildID || out.Value.Group == "") {
			return invalidRecord("build ownership key")
		}
		return e
	})
	return
}
func (s *Store) EnsureSandboxOwner(ctx context.Context, nodeID string, ref metadata.NodeSandboxRef) error {
	if err := validateParts(nodeID, ref.NodeSandboxID); err != nil {
		return err
	}
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardLive(m, nodeID); err != nil {
			return false, err
		}
		return true, s.ensureSandbox(m, nodeID, ref)
	})
	return err
}
func (s *Store) EnsureBuildOwner(ctx context.Context, nodeID string, ref metadata.NodeBuildRef) error {
	if err := validateParts(nodeID, ref.Group, ref.BuildID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardLive(m, nodeID); err != nil {
			return false, err
		}
		return true, s.ensureBuild(m, nodeID, ref)
	})
	return err
}
func (s *Store) removeOwner(ctx context.Context, key fdb.Key, expected metadata.Version) error {
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := m.expect(key, expected)
		if err != nil || !current.exists {
			return false, err
		}
		clearRecord(m.tr, key)
		return true, nil
	})
	return err
}
func (s *Store) RemoveSandboxOwner(ctx context.Context, nodeID, sandboxID string, expected metadata.Version) error {
	if err := validateParts(nodeID, sandboxID); err != nil {
		return err
	}
	return s.removeOwner(ctx, s.sandboxOwnerKey(nodeID, sandboxID), expected)
}
func (s *Store) RemoveBuildOwner(ctx context.Context, nodeID, buildID string, expected metadata.Version) error {
	if err := validateParts(nodeID, buildID); err != nil {
		return err
	}
	return s.removeOwner(ctx, s.buildOwnerKey(nodeID, buildID), expected)
}
func (s *Store) ListSandboxOwners(ctx context.Context, nodeID string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]], error) {
	if err := validateParts(nodeID); err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]]{}, err
	}
	page, err := listTyped[metadata.NodeSandboxRef](ctx, s, s.sandboxOwnerScope(nodeID), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]]{}, err
	}
	for _, item := range page.Items {
		if err = metadata.ValidateSandboxRef(item.Value); err != nil {
			return metadata.Page[metadata.Versioned[metadata.NodeSandboxRef]]{}, err
		}
	}
	return page, nil
}
func (s *Store) ListBuildOwners(ctx context.Context, nodeID string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeBuildRef]], error) {
	if err := validateParts(nodeID); err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeBuildRef]]{}, err
	}
	page, err := listTyped[metadata.NodeBuildRef](ctx, s, s.buildOwnerScope(nodeID), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeBuildRef]]{}, err
	}
	for _, item := range page.Items {
		if item.Value.Group == "" || item.Value.BuildID == "" {
			return metadata.Page[metadata.Versioned[metadata.NodeBuildRef]]{}, invalidRecord("build ownership identity")
		}
	}
	return page, nil
}
