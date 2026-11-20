//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (s *Store) buildScope(group string) fdb.Key { return s.key("builds", group) }
func (s *Store) buildKey(key metadata.BuildKey) fdb.Key {
	return mainKey(s.buildScope(key.Group), key.BuildID)
}
func (s *Store) templateScope(group, templateID string) fdb.Key {
	return s.key("build-template", group, templateID)
}
func (s *Store) buildIndexKey(v metadata.BuildRecord) fdb.Key {
	return mainKey(s.templateScope(v.Group, v.TemplateID), v.BuildID)
}
func (s *Store) putBuild(m *mutation, current metadata.Versioned[metadata.BuildRecord], value metadata.BuildRecord) error {
	if current.Exists && current.Value.TemplateID != "" && current.Value.TemplateID != value.TemplateID {
		clearRecord(m.tr, s.buildIndexKey(current.Value))
	}
	if value.TemplateID != "" {
		if err := putJSON(m.tr, s.buildIndexKey(value), metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}); err != nil {
			return err
		}
	}
	return putJSON(m.tr, s.buildKey(metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}), value)
}
func (s *Store) clearBuild(m *mutation, value metadata.BuildRecord) {
	clearRecord(m.tr, s.buildKey(metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}))
	if value.TemplateID != "" {
		clearRecord(m.tr, s.buildIndexKey(value))
	}
}
func (s *Store) ReadBuild(ctx context.Context, key metadata.BuildKey) (out metadata.Versioned[metadata.BuildRecord], err error) {
	if err = validateParts(key.Group, key.BuildID); err != nil {
		return
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		var e error
		out, e = readTyped[metadata.BuildRecord](tr, s.buildKey(key))
		if e == nil && out.Exists && (out.Value.Group != key.Group || out.Value.BuildID != key.BuildID) {
			return invalidRecord("build key mismatch")
		}
		return e
	})
	return
}
func (s *Store) CompareAndPutBuild(ctx context.Context, key metadata.BuildKey, expected metadata.Version, value metadata.BuildRecord) (metadata.Version, error) {
	if err := validateParts(key.Group, key.BuildID); err != nil {
		return metadata.Version{}, err
	}
	if value.Group != key.Group || value.BuildID != key.BuildID {
		return metadata.Version{}, invalidRecord("build key mismatch")
	}
	if err := metadata.ValidateBuild(value); err != nil {
		return metadata.Version{}, err
	}
	return s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.buildKey(key), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.BuildRecord](raw)
		if err != nil {
			return false, err
		}
		return true, s.putBuild(m, current, value)
	})
}
func (s *Store) DeleteBuild(ctx context.Context, key metadata.BuildKey, expected metadata.Version) error {
	if err := validateParts(key.Group, key.BuildID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.buildKey(key), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.BuildRecord](raw)
		if err != nil || !current.Exists {
			return false, err
		}
		s.clearBuild(m, current.Value)
		return true, nil
	})
	return err
}
func (s *Store) CommitRegistrationIntent(ctx context.Context, value metadata.BuildRecord, ref metadata.NodeBuildRef) (metadata.Version, error) {
	if err := metadata.ValidateBuild(value); err != nil {
		return metadata.Version{}, err
	}
	if err := validateParts(value.Group, value.BuildID, value.NodeID, ref.TemplateID); err != nil {
		return metadata.Version{}, err
	}
	if value.State != metadata.BuildStarting || ref.Group != value.Group || ref.BuildID != value.BuildID || ref.TemplateID != value.TemplateID {
		return metadata.Version{}, invalidRecord("build intent ownership")
	}
	return s.mutate(ctx, func(m *mutation) (bool, error) {
		if _, err := m.expect(s.buildKey(metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}), metadata.Version{}); err != nil {
			return false, err
		}
		if err := s.guardLive(m, value.NodeID); err != nil {
			return false, err
		}
		if err := s.ensureBuild(m, value.NodeID, ref); err != nil {
			return false, err
		}
		return true, s.putBuild(m, metadata.Versioned[metadata.BuildRecord]{}, value)
	})
}
func (s *Store) AcceptBuildRegistration(ctx context.Context, key metadata.BuildKey, nodeID, templateID string, target *types.BuildTarget) (metadata.Versioned[metadata.BuildRecord], error) {
	if err := validateParts(key.Group, key.BuildID, nodeID, templateID); err != nil {
		return metadata.Versioned[metadata.BuildRecord]{}, err
	}
	var out metadata.Versioned[metadata.BuildRecord]
	var changed bool
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := getTyped[metadata.BuildRecord](m, s.buildKey(key))
		if err != nil {
			return false, err
		}
		next, write, err := metadata.MergeBuildAccepted(current, nodeID, templateID, target)
		if err != nil {
			return false, err
		}
		out = metadata.Versioned[metadata.BuildRecord]{Exists: true, Version: current.Version, Value: next}
		changed = write
		if !write {
			return false, nil
		}
		return true, s.putBuild(m, current, next)
	})
	if err != nil {
		return metadata.Versioned[metadata.BuildRecord]{}, err
	}
	if changed {
		out.Version = version
	}
	return out, nil
}
func (s *Store) EnsureRegistrationOwnership(ctx context.Context, key metadata.BuildKey, nodeID, templateID string) error {
	if err := validateParts(key.Group, key.BuildID, nodeID, templateID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := getTyped[metadata.BuildRecord](m, s.buildKey(key))
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value.NodeID != nodeID || current.Value.TemplateID != templateID {
			return false, metadata.ErrConflict
		}
		if err = s.guardLive(m, nodeID); err != nil {
			return false, err
		}
		return true, s.ensureBuild(m, nodeID, metadata.NodeBuildRef{Group: key.Group, BuildID: key.BuildID, TemplateID: templateID})
	})
	return err
}
func (s *Store) RejectRegistrationIntent(ctx context.Context, key metadata.BuildKey, nodeID, templateID string) error {
	if err := validateParts(key.Group, key.BuildID, nodeID, templateID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := getTyped[metadata.BuildRecord](m, s.buildKey(key))
		if err != nil {
			return false, err
		}
		if current.Exists && (current.Value.NodeID != nodeID || current.Value.TemplateID != templateID || current.Value.State != metadata.BuildStarting || current.Value.RegistrationTargetSet) {
			return false, metadata.ErrConflict
		}
		if current.Exists {
			s.clearBuild(m, current.Value)
		}
		cleared, err := s.clearMatchingBuild(m, nodeID, metadata.NodeBuildRef{Group: key.Group, BuildID: key.BuildID, TemplateID: templateID})
		return current.Exists || cleared, err
	})
	return err
}
func (s *Store) ApplyBuildReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeBuildRef, event routesync.BuildEvent) error {
	nodeID := registration.NodeID
	if err := validateParts(nodeID, ref.Group, ref.BuildID); err != nil {
		return err
	}
	if event.BuildID != ref.BuildID || (ref.TemplateID != "" && ref.TemplateID != event.TemplateID) {
		return nil
	}
	switch metadata.BuildState(event.State) {
	case metadata.BuildRegistered, metadata.BuildWaiting, metadata.BuildBuilding, metadata.BuildReady, metadata.BuildError:
	default:
		return nil
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, registration); err != nil {
			return false, err
		}
		owner, err := getTyped[metadata.NodeBuildRef](m, s.buildOwnerKey(nodeID, ref.BuildID))
		if err != nil {
			return false, err
		}
		if !owner.Exists || owner.Value != ref {
			return false, nil
		}
		current, err := getTyped[metadata.BuildRecord](m, s.buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID}))
		if err != nil || !current.Exists {
			return false, err
		}
		next := current.Value
		if next.NodeID != nodeID || (event.TemplateID != "" && types.IsTransientID(next.TemplateID) && next.TemplateID != event.TemplateID) {
			return false, nil
		}
		next.State = metadata.BuildState(event.State)
		next.Reason = event.Reason
		if event.PersistID != "" {
			next.PersistID = event.PersistID
		}
		if types.IsTransientID(event.TemplateID) {
			next.TemplateID = event.TemplateID
		}
		if next.RegistrationTargetSet {
			next.RegistrationImageRepo = ""
			next.RegistrationRegistryAuth = ""
		}
		return true, s.putBuild(m, current, next)
	})
	return err
}
func (s *Store) DeleteBuildProjection(ctx context.Context, registration metadata.Registration, ref metadata.NodeBuildRef) error {
	nodeID := registration.NodeID
	if err := validateParts(nodeID, ref.Group, ref.BuildID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, registration); err != nil {
			return false, err
		}
		owner, err := getTyped[metadata.NodeBuildRef](m, s.buildOwnerKey(nodeID, ref.BuildID))
		if err != nil || !owner.Exists {
			return false, err
		}
		if owner.Value != ref {
			return false, nil
		}
		current, err := getTyped[metadata.BuildRecord](m, s.buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID}))
		if err != nil {
			return false, err
		}
		if current.Exists && ref.TemplateID != "" && current.Value.TemplateID != ref.TemplateID {
			return false, nil
		}
		if current.Exists && current.Value.NodeID == nodeID {
			if current.Value.State == metadata.BuildStarting {
				return false, nil
			}
			s.clearBuild(m, current.Value)
		}
		clearRecord(m.tr, s.buildOwnerKey(nodeID, ref.BuildID))
		return true, nil
	})
	return err
}
func (s *Store) ListBuilds(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.BuildRecord]], error) {
	if err := validateParts(group); err != nil {
		return metadata.Page[metadata.Versioned[metadata.BuildRecord]]{}, err
	}
	page, err := listTyped[metadata.BuildRecord](ctx, s, s.buildScope(group), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.BuildRecord]]{}, err
	}
	for _, item := range page.Items {
		if item.Value.Group != group || item.Value.BuildID == "" {
			return metadata.Page[metadata.Versioned[metadata.BuildRecord]]{}, invalidRecord("build listing identity")
		}
	}
	return page, nil
}
func (s *Store) ListBuildsByTemplate(ctx context.Context, group, templateID string, cursor metadata.Cursor, limit int) (out metadata.Page[metadata.Versioned[metadata.BuildRecord]], err error) {
	if err = validateParts(group, templateID); err != nil {
		return
	}
	if limit < 1 || limit > maxPageRows {
		return out, invalidRecord("page limit must be 1..512")
	}
	scope := s.templateScope(group, templateID)
	last, err := decodeCursor(scope, cursor)
	if err != nil {
		return out, err
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		out = metadata.Page[metadata.Versioned[metadata.BuildRecord]]{}
		bounds := rangeFor(scope)
		begin := fdb.FirstGreaterOrEqual(scope)
		if last != nil {
			begin = fdb.FirstGreaterThan(last)
		}
		rows := tr.GetRange(fdb.SelectorRange{Begin: begin, End: fdb.FirstGreaterOrEqual(bounds.End)}, fdb.RangeOptions{Limit: limit + 1, Mode: fdb.StreamingModeSmall}).Iterator()
		used := 0
		var previous fdb.Key
		for rows.Advance() {
			row, err := rows.Get()
			if err != nil {
				return err
			}
			if len(out.Items) == limit {
				out.Next = encodeCursor(scope, previous)
				break
			}
			index, err := decodeRecord(tr, row.Key, row.Value)
			if err != nil {
				return err
			}
			pointer, err := typed[metadata.BuildKey](index)
			if err != nil {
				return err
			}
			if pointer.Value.Group != group {
				return invalidRecord("build index scope")
			}
			key := s.buildKey(pointer.Value)
			raw, err := tr.Get(key).Get()
			if err != nil {
				return err
			}
			if _, err = recordStamp(raw); err != nil {
				return err
			}
			if raw == nil {
				return invalidRecord("missing build index target")
			}
			size := binary.BigEndian.Uint64(raw[11:19])
			if len(out.Items) > 0 && uint64(used)+size > maxPageBytes {
				out.Next = encodeCursor(scope, previous)
				break
			}
			record, err := decodeRecord(tr, key, raw)
			if err != nil {
				return err
			}
			value, err := typed[metadata.BuildRecord](record)
			if err != nil {
				return err
			}
			if !value.Exists || value.Value.TemplateID != templateID {
				return invalidRecord("build index target")
			}
			out.Items = append(out.Items, value)
			used += len(record.payload)
			previous = row.Key
		}
		return nil
	})
	return out, err
}
