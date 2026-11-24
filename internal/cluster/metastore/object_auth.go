package metastore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

// References belong to individual route/ownership facts. The bounded identity
// is an encoding detail, not a tenant-wide refcount or an authorization token.
func objectAuthReference(group, fingerprint, kind string, identity ...string) metadata.GroupAuthReference {
	raw, _ := json.Marshal(identity)
	digest := sha256.Sum256(raw)
	return metadata.GroupAuthReference{Group: group, AuthRef: fingerprint, Kind: kind, ID: hex.EncodeToString(digest[:])}
}

func (s *Store) retainObjectAuth(m *mutation, ref metadata.GroupAuthReference, revision int64) error {
	if revision == 0 {
		return nil
	} // retained records from before Group activation
	auth, err := getTyped[metadata.GroupAuth](m, s.groupAuthKey(ref.Group, ref.AuthRef))
	if err != nil {
		return err
	}
	if !auth.Exists {
		return metadata.ErrOwnershipConflict
	}
	return s.retainGroupAuth(m, ref, auth.Value.APISecret)
}

func (s *Store) releaseObjectAuth(m *mutation, ref metadata.GroupAuthReference, revision int64) error {
	if revision == 0 {
		return nil
	}
	return s.releaseGroupAuth(m, ref)
}

func sandboxRouteAuth(r metadata.SandboxRecord) metadata.GroupAuthReference {
	return objectAuthReference(r.Group, r.APISecretFingerprint, "sandbox", "route", r.RouteKey)
}
func sandboxOwnerAuth(node string, r metadata.NodeSandboxRef) metadata.GroupAuthReference {
	return objectAuthReference(r.Group, r.APISecretFingerprint, "sandbox", "owner", node, r.NodeSandboxID)
}
func buildRecordAuth(r metadata.BuildRecord) metadata.GroupAuthReference {
	return objectAuthReference(r.Group, r.APISecretFingerprint, "build", "record", r.BuildID)
}
func buildOwnerAuth(node string, r metadata.NodeBuildRef) metadata.GroupAuthReference {
	return objectAuthReference(r.Group, r.APISecretFingerprint, "build", "owner", node, r.BuildID)
}

func (s *Store) putRoute(m *mutation, value metadata.SandboxRecord) error {
	key := s.routeKey(metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey})
	current, err := getTyped[metadata.SandboxRecord](m, key)
	if err != nil {
		return err
	}
	if err := s.retainObjectAuth(m, sandboxRouteAuth(value), value.GroupVersion); err != nil {
		return err
	}
	if current.Exists && current.Value.GroupVersion > 0 && (value.GroupVersion == 0 || current.Value.APISecretFingerprint != value.APISecretFingerprint) {
		if err := s.releaseObjectAuth(m, sandboxRouteAuth(current.Value), current.Value.GroupVersion); err != nil {
			return err
		}
	}
	return putJSON(m.tr, key, value)
}

func (s *Store) clearRoute(m *mutation, key metadata.RouteKey) error {
	current, err := getTyped[metadata.SandboxRecord](m, s.routeKey(key))
	if err != nil || !current.Exists {
		return err
	}
	if err := s.releaseObjectAuth(m, sandboxRouteAuth(current.Value), current.Value.GroupVersion); err != nil {
		return err
	}
	clearRecord(m.tr, s.routeKey(key))
	return nil
}
