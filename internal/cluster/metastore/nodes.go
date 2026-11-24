package metastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func (s *Store) guardReap(m *mutation, claim metadata.ReapClaim) error {
	if !claim.ProfileVersion.IsCommitted() || !claim.Fence.IsCommitted() {
		return metadata.ErrConflict
	}
	profile, err := m.expect(s.profileKey(claim.NodeID), claim.ProfileVersion)
	if err != nil {
		return err
	}
	if !profile.exists {
		return metadata.ErrConflict
	}
	current, err := getTyped[lifecycle](m, s.lifecycleKey(claim.NodeID))
	if err != nil {
		return err
	}
	if !current.Exists || !current.Value.Reaping || current.Value.NodeID != claim.NodeID || current.Value.LinkOwner != claim.LinkOwner || lifeFence(current) != claim.Fence {
		return metadata.ErrConflict
	}
	marker, err := m.expect(s.reapingKey(claim.NodeID), claim.ProfileVersion)
	if err != nil {
		return err
	}
	if !marker.exists {
		return metadata.ErrConflict
	}
	return nil
}
func nodeSnapshot(profile metadata.Versioned[metadata.NodeProfileRecord], life metadata.Versioned[lifecycle]) (metadata.NodeSnapshot, error) {
	if profile.Exists != life.Exists {
		return metadata.NodeSnapshot{}, invalidRecord("profile lifecycle mismatch")
	}
	if !profile.Exists {
		return metadata.NodeSnapshot{}, nil
	}
	if profile.Value.NodeID != life.Value.NodeID || profile.Value.LinkOwner != life.Value.LinkOwner || !lifeFence(life).IsCommitted() {
		return metadata.NodeSnapshot{}, invalidRecord("profile registration identity")
	}
	return metadata.NodeSnapshot{Exists: true, Version: profile.Version, Value: profile.Value, Registration: metadata.Registration{NodeID: life.Value.NodeID, LinkOwner: life.Value.LinkOwner, Fence: lifeFence(life)}, Reaping: life.Value.Reaping}, nil
}
func (s *Store) ReadNodeProfile(ctx context.Context, nodeID string) (out metadata.NodeSnapshot, err error) {
	if err = validateParts(nodeID); err != nil {
		return
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		profile, err := readTyped[metadata.NodeProfileRecord](tr, s.profileKey(nodeID))
		if err != nil {
			return err
		}
		life, err := readTyped[lifecycle](tr, s.lifecycleKey(nodeID))
		if err != nil {
			return err
		}
		out, err = nodeSnapshot(profile, life)
		return err
	})
	return
}
func validateProfile(value metadata.NodeProfileRecord) error {
	if err := validateParts(value.NodeID, value.LinkOwner); err != nil {
		return err
	}
	if value.Capacity < 0 {
		return invalidRecord("negative node capacity")
	}
	return nil
}
func (s *Store) Register(ctx context.Context, expected metadata.Version, value metadata.NodeProfileRecord) (metadata.NodeSnapshot, error) {
	if err := validateProfile(value); err != nil {
		return metadata.NodeSnapshot{}, err
	}
	value.State = metadata.NodeLive
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if _, err := m.expect(s.profileKey(value.NodeID), expected); err != nil {
			return false, err
		}
		if _, err := m.get(s.lifecycleKey(value.NodeID)); err != nil {
			return false, err
		}
		if err := putJSON(m.tr, s.profileKey(value.NodeID), value); err != nil {
			return false, err
		}
		if err := putJSON(m.tr, s.lifecycleKey(value.NodeID), lifecycle{NodeID: value.NodeID, LinkOwner: value.LinkOwner}); err != nil {
			return false, err
		}
		clearRecord(m.tr, s.reapingKey(value.NodeID))
		return true, s.putCatalog(m.tr, value)
	})
	if err != nil {
		return metadata.NodeSnapshot{}, err
	}
	return metadata.NodeSnapshot{Exists: true, Version: version, Value: value, Registration: metadata.Registration{NodeID: value.NodeID, LinkOwner: value.LinkOwner, Fence: version}}, nil
}
func (s *Store) UpdateProfile(ctx context.Context, registration metadata.Registration, expected metadata.Version, value metadata.NodeProfileRecord) (metadata.Version, error) {
	if err := validateProfile(value); err != nil {
		return metadata.Version{}, err
	}
	if registration.NodeID != value.NodeID || registration.LinkOwner != value.LinkOwner || !registration.Fence.IsCommitted() {
		return metadata.Version{}, metadata.ErrConflict
	}
	return s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.profileKey(value.NodeID), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.NodeProfileRecord](raw)
		if err != nil || !current.Exists {
			if err == nil {
				err = metadata.ErrConflict
			}
			return false, err
		}
		life, err := getTyped[lifecycle](m, s.lifecycleKey(value.NodeID))
		if err != nil {
			return false, err
		}
		if !life.Exists || life.Value.Reaping || life.Value.NodeID != registration.NodeID || life.Value.LinkOwner != registration.LinkOwner || lifeFence(life) != registration.Fence {
			return false, metadata.ErrConflict
		}
		value.State = metadata.NodeLive
		if err = putJSON(m.tr, s.profileKey(value.NodeID), value); err != nil {
			return false, err
		}
		old, _ := json.Marshal(projectNode(current.Value))
		next, _ := json.Marshal(projectNode(value))
		if !bytes.Equal(old, next) {
			if err = s.putCatalog(m.tr, value); err != nil {
				return false, err
			}
		}
		return true, nil
	})
}
func (s *Store) BeginReap(ctx context.Context, nodeID string, expected metadata.Version) (metadata.ReapClaim, error) {
	if err := validateParts(nodeID); err != nil {
		return metadata.ReapClaim{}, err
	}
	var claim metadata.ReapClaim
	var already bool
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.profileKey(nodeID), expected)
		if err != nil {
			return false, err
		}
		profile, err := typed[metadata.NodeProfileRecord](raw)
		if err != nil {
			return false, err
		}
		life, err := getTyped[lifecycle](m, s.lifecycleKey(nodeID))
		if err != nil {
			return false, err
		}
		if !profile.Exists || !life.Exists || profile.Value.NodeID != nodeID || life.Value.NodeID != nodeID {
			return false, metadata.ErrConflict
		}
		claim = metadata.ReapClaim{NodeID: nodeID, LinkOwner: life.Value.LinkOwner, ProfileVersion: profile.Version, Fence: lifeFence(life)}
		already = life.Value.Reaping
		if already {
			return false, nil
		}
		next := life.Value
		next.Fence = lifeFence(life)
		next.Reaping = true
		profile.Value.State = metadata.NodeDead
		if err = putJSON(m.tr, s.profileKey(nodeID), profile.Value); err != nil {
			return false, err
		}
		if err = putJSON(m.tr, s.lifecycleKey(nodeID), next); err != nil {
			return false, err
		}
		if err = putJSON(m.tr, s.reapingKey(nodeID), next); err != nil {
			return false, err
		}
		s.clearCatalog(m.tr, nodeID)
		return true, nil
	})
	if err != nil {
		return metadata.ReapClaim{}, err
	}
	if !already {
		claim.ProfileVersion = version
	}
	return claim, nil
}
func (s *Store) CleanupSandbox(ctx context.Context, claim metadata.ReapClaim, ref metadata.Versioned[metadata.NodeSandboxRef], expectedRoute metadata.Version, mayReapReserved bool) error {
	if err := metadata.ValidateSandboxRef(ref.Value); err != nil {
		return err
	}
	if !ref.Exists || !ref.Version.IsCommitted() {
		return metadata.ErrConflict
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardReap(m, claim); err != nil {
			return false, err
		}
		raw, err := m.expect(s.sandboxOwnerKey(claim.NodeID, ref.Value.NodeSandboxID), ref.Version)
		if err != nil {
			return false, err
		}
		actual, err := typed[metadata.NodeSandboxRef](raw)
		if err != nil {
			return false, err
		}
		if !actual.Exists || actual.Value != ref.Value {
			return false, metadata.ErrConflict
		}
		key := s.routeKey(metadata.RouteKey{Group: ref.Value.Group, RouteKey: ref.Value.RouteKey})
		raw, err = m.expect(key, expectedRoute)
		if err != nil {
			return false, err
		}
		route, err := typed[metadata.SandboxRecord](raw)
		if err != nil {
			return false, err
		}
		if route.Exists && route.Value.NodeID == claim.NodeID && metadata.SandboxRef(route.Value) == ref.Value {
			switch route.Value.State {
			case metadata.StateDead, metadata.StateDeleting:
				return false, nil
			case metadata.StateReserved:
				if !mayReapReserved {
					return false, nil
				}
				if err := s.clearRoute(m, metadata.RouteKey{Group: ref.Value.Group, RouteKey: ref.Value.RouteKey}); err != nil {
					return false, err
				}
			case metadata.StateReady, metadata.StatePaused, metadata.StatePausing:
				if err := s.clearRoute(m, metadata.RouteKey{Group: ref.Value.Group, RouteKey: ref.Value.RouteKey}); err != nil {
					return false, err
				}
			}
		}
		return s.clearMatchingSandbox(m, claim.NodeID, ref.Value)
	})
	return err
}
func (s *Store) CleanupBuild(ctx context.Context, claim metadata.ReapClaim, ref metadata.Versioned[metadata.NodeBuildRef], expectedBuild metadata.Version) error {
	if !ref.Exists || !ref.Version.IsCommitted() {
		return metadata.ErrConflict
	}
	if err := validateParts(ref.Value.Group, ref.Value.BuildID, claim.NodeID); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardReap(m, claim); err != nil {
			return false, err
		}
		raw, err := m.expect(s.buildOwnerKey(claim.NodeID, ref.Value.BuildID), ref.Version)
		if err != nil {
			return false, err
		}
		owner, err := typed[metadata.NodeBuildRef](raw)
		if err != nil {
			return false, err
		}
		if !owner.Exists || owner.Value != ref.Value {
			return false, metadata.ErrConflict
		}
		key := s.buildKey(metadata.BuildKey{Group: ref.Value.Group, BuildID: ref.Value.BuildID})
		raw, err = m.expect(key, expectedBuild)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.BuildRecord](raw)
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value.NodeID != claim.NodeID || current.Value.Group != ref.Value.Group || current.Value.BuildID != ref.Value.BuildID || (ref.Value.TemplateID != "" && current.Value.TemplateID != ref.Value.TemplateID) {
			return s.clearMatchingBuild(m, claim.NodeID, ref.Value)
		}
		switch current.Value.State {
		case metadata.BuildStarting, metadata.BuildRegistered, metadata.BuildWaiting, metadata.BuildBuilding:
			next := current.Value
			next.State = metadata.BuildError
			next.Reason = "node disconnected"
			next.RegistrationImageRepo = ""
			next.RegistrationRegistryAuth = ""
			return true, s.putBuild(m, current, next)
		default:
			return false, nil
		}
	})
	return err
}
func (s *Store) CleanupKeyPair(ctx context.Context, claim metadata.ReapClaim, pair metadata.Versioned[metadata.NodeKeyPair]) error {
	if !pair.Exists || !pair.Version.IsCommitted() {
		return metadata.ErrConflict
	}
	if !metadata.ValidFingerprint(pair.Value.APISecretFingerprint) {
		return invalidRecord("key pair fingerprint")
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardReap(m, claim); err != nil {
			return false, err
		}
		key := s.keyPairKey(claim.NodeID, pair.Value.APISecretFingerprint)
		raw, err := m.expect(key, pair.Version)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.NodeKeyPair](raw)
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value != pair.Value {
			return false, metadata.ErrConflict
		}
		clearRecord(m.tr, key)
		return true, nil
	})
	return err
}
func (s *Store) FinalizeReap(ctx context.Context, claim metadata.ReapClaim) (bool, error) {
	removed := false
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		removed = false
		if err := s.guardReap(m, claim); err != nil {
			return false, err
		}
		for _, scope := range []records.Key{s.sandboxOwnerScope(claim.NodeID), s.buildOwnerScope(claim.NodeID), s.keyPairScope(claim.NodeID), s.groupNodeScope(claim.NodeID)} {
			rows, err := m.tr.Scan(scope, nil, pageLimit(1))
			if err != nil {
				return false, err
			}
			if len(rows.Items) > 0 {
				return false, nil
			}
		}
		clearRecord(m.tr, s.profileKey(claim.NodeID))
		clearRecord(m.tr, s.lifecycleKey(claim.NodeID))
		clearRecord(m.tr, s.reapingKey(claim.NodeID))
		clearRecord(m.tr, s.groupPublicationKey(claim.NodeID))
		clearRecord(m.tr, s.groupProgressKey(claim.NodeID))
		s.clearCatalog(m.tr, claim.NodeID)
		removed = true
		return true, nil
	})
	return removed && err == nil, err
}
func (s *Store) ListReaping(ctx context.Context, cursor metadata.Cursor, limit int) (metadata.Page[metadata.ReapClaim], error) {
	values, err := listTyped[lifecycle](ctx, s, s.key("node-reaping"), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.ReapClaim]{}, err
	}
	out := metadata.Page[metadata.ReapClaim]{Next: values.Next}
	for _, value := range values.Items {
		if !value.Value.Reaping || !value.Value.Fence.IsCommitted() {
			return metadata.Page[metadata.ReapClaim]{}, invalidRecord("reaping marker")
		}
		out.Items = append(out.Items, metadata.ReapClaim{NodeID: value.Value.NodeID, LinkOwner: value.Value.LinkOwner, ProfileVersion: value.Version, Fence: value.Value.Fence})
	}
	return out, nil
}

func (s *Store) ListExpiredNodeProfiles(ctx context.Context, cutoffUnix int64, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]], error) {
	values, err := listTyped[metadata.NodeProfileRecord](ctx, s, s.key("node-profile"), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]]{}, err
	}
	out := metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]]{Next: values.Next}
	for _, value := range values.Items {
		if value.Value.State == metadata.NodeLive && value.Value.LastHeartbeatUnix > 0 && value.Value.LastHeartbeatUnix <= cutoffUnix {
			out.Items = append(out.Items, value)
		}
	}
	return out, nil
}

func bucket(nodeID string) byte { return sha256.Sum256([]byte(nodeID))[0] }
func projectNode(value metadata.NodeProfileRecord) metadata.NodeListEntry {
	return metadata.NodeListEntry{NodeID: value.NodeID, Labels: value.Labels, Capacity: value.Capacity, BuildRegistrationCapacity: value.BuildRegistrationCapacity, BuildExecutionCapacity: value.BuildExecutionCapacity, APIEndpoint: value.APIEndpoint, DataEndpoint: value.DataEndpoint, RuntimeDigest: value.RuntimeDigest, Draining: value.Draining}
}
func (s *Store) catalogScope(b byte) records.Key { return s.key("node-catalog", string([]byte{b})) }
func (s *Store) catalogKey(nodeID string) records.Key {
	return mainKey(s.catalogScope(bucket(nodeID)), nodeID)
}
func (s *Store) catalogTokenKey(b byte) records.Key {
	return s.key("node-catalog-token", string([]byte{b}))
}
func (s *Store) putCatalog(tr *records.Tx, value metadata.NodeProfileRecord) error {
	if err := putJSON(tr, s.catalogKey(value.NodeID), projectNode(value)); err != nil {
		return err
	}
	return tr.Put(s.catalogTokenKey(bucket(value.NodeID)), nil)
}
func (s *Store) clearCatalog(tr *records.Tx, nodeID string) {
	clearRecord(tr, s.catalogKey(nodeID))
	_ = tr.Put(s.catalogTokenKey(bucket(nodeID)), nil)
}
