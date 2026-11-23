package metastore

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func (s *Store) buildScope(group string) records.Key { return s.key("builds", group) }
func (s *Store) buildKey(key metadata.BuildKey) records.Key {
	return mainKey(s.buildScope(key.Group), key.BuildID)
}
func (s *Store) templateScope(group, templateID string) records.Key {
	return s.key("build-template", group, templateID)
}
func (s *Store) buildIndexKey(v metadata.BuildRecord) records.Key {
	return mainKey(s.templateScope(v.Group, v.TemplateID), v.BuildID)
}
func (s *Store) putBuild(m *mutation, current metadata.Versioned[metadata.BuildRecord], value metadata.BuildRecord) error {
	if err := s.retainObjectAuth(m, buildRecordAuth(value), value.GroupVersion); err != nil {
		return err
	}
	if current.Exists && current.Value.GroupVersion > 0 && (value.GroupVersion == 0 || current.Value.APISecretFingerprint != value.APISecretFingerprint) {
		if err := s.releaseObjectAuth(m, buildRecordAuth(current.Value), current.Value.GroupVersion); err != nil {
			return err
		}
	}
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
func (s *Store) clearBuild(m *mutation, value metadata.BuildRecord) error {
	if err := s.releaseObjectAuth(m, buildRecordAuth(value), value.GroupVersion); err != nil {
		return err
	}
	clearRecord(m.tr, s.buildKey(metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}))
	if value.TemplateID != "" {
		clearRecord(m.tr, s.buildIndexKey(value))
	}
	return nil
}
func (s *Store) ReadBuild(ctx context.Context, key metadata.BuildKey) (out metadata.Versioned[metadata.BuildRecord], err error) {
	if err = validateParts(key.Group, key.BuildID); err != nil {
		return
	}
	err = s.read(ctx, func(tr *records.Tx) error {
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
		if err := s.clearBuild(m, current.Value); err != nil {
			return false, err
		}
		return true, nil
	})
	return err
}
func (s *Store) CommitRegistrationIntent(ctx context.Context, value metadata.BuildRecord, ref metadata.NodeBuildRef) (metadata.Version, error) {
	return s.commitBuildIntent(ctx, value, ref, nil)
}
func (s *Store) CommitGroupRegistrationIntent(ctx context.Context, value metadata.BuildRecord, ref metadata.NodeBuildRef, selected metadata.GroupAdmission) (metadata.Version, error) {
	return s.commitBuildIntent(ctx, value, ref, &selected)
}
func (s *Store) commitBuildIntent(ctx context.Context, value metadata.BuildRecord, ref metadata.NodeBuildRef, selected *metadata.GroupAdmission) (metadata.Version, error) {
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
		if selected != nil {
			if ref != metadata.BuildRef(value) || value.RegistrationImageRepo != "" || value.RegistrationRegistryAuth != "" {
				return false, invalidRecord("Group Build intent inputs")
			}
			if err := s.guardGroupAdmission(m, *selected, value.Group, value.NodeID, value.APISecretFingerprint, value.GroupVersion, "", ""); err != nil {
				return false, err
			}
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
	return s.acceptBuildRegistration(ctx, key, nodeID, templateID, target, nil)
}
func (s *Store) AcceptGroupBuildRegistration(ctx context.Context, registration metadata.Registration, key metadata.BuildKey, templateID string, target *types.BuildTarget) (metadata.Versioned[metadata.BuildRecord], error) {
	return s.acceptBuildRegistration(ctx, key, registration.NodeID, templateID, target, &registration)
}
func (s *Store) acceptBuildRegistration(ctx context.Context, key metadata.BuildKey, nodeID, templateID string, target *types.BuildTarget, registration *metadata.Registration) (metadata.Versioned[metadata.BuildRecord], error) {
	if err := validateParts(key.Group, key.BuildID, nodeID, templateID); err != nil {
		return metadata.Versioned[metadata.BuildRecord]{}, err
	}
	var out metadata.Versioned[metadata.BuildRecord]
	var changed bool
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if registration != nil {
			if err := s.guardRegistration(m, *registration); err != nil {
				return false, err
			}
		}
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

func (s *Store) RetireRejectedGroupBuild(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeBuildRef) error {
	if !expected.IsCommitted() || ref.GroupVersion <= 0 {
		return invalidRecord("rejected Group Build")
	}
	if err := validateParts(registration.NodeID, ref.Group, ref.BuildID, ref.TemplateID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, registration); err != nil {
			return false, err
		}
		raw, err := m.expect(s.buildKey(metadata.BuildKey{Group: ref.Group, BuildID: ref.BuildID}), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.BuildRecord](raw)
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value.NodeID != registration.NodeID || current.Value.State != metadata.BuildStarting || current.Value.RegistrationTargetSet || metadata.BuildRef(current.Value) != ref {
			return false, metadata.ErrConflict
		}
		// Every replay refreshes this owner before sending. Its original intent
		// version is therefore proof that no second delivery was authorized.
		raw, err = m.expect(s.buildOwnerKey(registration.NodeID, ref.BuildID), expected)
		if err != nil {
			return false, err
		}
		owner, err := typed[metadata.NodeBuildRef](raw)
		if err != nil {
			return false, err
		}
		if !owner.Exists || owner.Value != ref {
			return false, metadata.ErrConflict
		}
		if err := s.clearBuild(m, current.Value); err != nil {
			return false, err
		}
		_, err = s.clearMatchingBuild(m, registration.NodeID, ref)
		return true, err
	})
	return err
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
		ref := metadata.NodeBuildRef{Group: key.Group, BuildID: key.BuildID, TemplateID: templateID}
		if current.Value.GroupVersion > 0 {
			ref = metadata.BuildRef(current.Value)
		}
		return true, s.ensureBuild(m, nodeID, ref)
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
			if err := s.clearBuild(m, current.Value); err != nil {
				return false, err
			}
		}
		ref := metadata.NodeBuildRef{Group: key.Group, BuildID: key.BuildID, TemplateID: templateID}
		if current.Exists && current.Value.GroupVersion > 0 {
			ref = metadata.BuildRef(current.Value)
		}
		cleared, err := s.clearMatchingBuild(m, nodeID, ref)
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
			if err := s.clearBuild(m, current.Value); err != nil {
				return false, err
			}
		}
		return s.clearMatchingBuild(m, nodeID, ref)
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
	err = s.read(ctx, func(tr *records.Tx) error {
		out = metadata.Page[metadata.Versioned[metadata.BuildRecord]]{}
		page, err := tr.Scan(scope, last, pageLimit(limit))
		if err != nil {
			return err
		}
		used := 0
		var previous records.Key
		for _, item := range page.Items {
			pointer, err := typed[metadata.BuildKey](wrapRecord(item.Value))
			if err != nil {
				return err
			}
			if pointer.Value.Group != group {
				return invalidRecord("build index scope")
			}
			target, err := tr.Get(s.buildKey(pointer.Value))
			if err != nil {
				return err
			}
			if !target.Exists {
				return invalidRecord("missing build index target")
			}
			if len(out.Items) > 0 && used+len(target.Payload) > maxPageBytes {
				out.Next = encodeCursor(scope, previous)
				return nil
			}
			value, err := typed[metadata.BuildRecord](wrapRecord(target))
			if err != nil {
				return err
			}
			if value.Value.Group != group || value.Value.TemplateID != templateID || value.Value.BuildID != pointer.Value.BuildID {
				return invalidRecord("build index target identity")
			}
			out.Items = append(out.Items, value)
			used += len(target.Payload)
			previous = item.Key
		}
		if page.More && previous != nil {
			out.Next = encodeCursor(scope, previous)
		}
		return nil
	})
	return
}
