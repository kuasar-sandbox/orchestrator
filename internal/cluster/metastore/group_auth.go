package metastore

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func (s *Store) groupAuthKey(group, authRef string) records.Key {
	return s.key("group-auth", group, authRef)
}
func (s *Store) groupAuthReferenceKey(ref metadata.GroupAuthReference) records.Key {
	return s.groupAuthReferenceScope(ref.Group, ref.AuthRef).Child(ref.Kind).Child(ref.ID)
}

func (s *Store) groupAuthReferenceScope(group, authRef string) records.Key {
	return s.key("group-auth-reference", group, authRef)
}

func validGroupAuth(authRef, secret string) bool {
	if !metadata.ValidFingerprint(authRef) || !metadata.ValidFingerprint(secret) {
		return false
	}
	raw, err := hex.DecodeString(secret)
	if err != nil {
		return false
	}
	defer clear(raw)
	fp := sha256.Sum256(raw)
	return authRef == hex.EncodeToString(fp[:])
}

func validateGroupAuthReference(ref metadata.GroupAuthReference) error {
	if err := nodegroup.ValidateName(ref.Group); err != nil {
		return err
	}
	if !metadata.ValidFingerprint(ref.AuthRef) {
		return invalidRecord("Group auth fingerprint")
	}
	if ref.Kind != "support" && ref.Kind != "sandbox" && ref.Kind != "build" {
		return invalidRecord("Group auth reference kind")
	}
	return validateParts(ref.ID)
}

// retainGroupAuth runs inside the support/object transaction. There is no
// independent cache write that can outlive a rejected business mutation.
func (s *Store) retainGroupAuth(m *mutation, ref metadata.GroupAuthReference, secret string) error {
	if err := validateGroupAuthReference(ref); err != nil {
		return err
	}
	if !validGroupAuth(ref.AuthRef, secret) {
		return invalidRecord("Group authentication material")
	}
	current, err := getTyped[metadata.GroupAuth](m, s.groupAuthKey(ref.Group, ref.AuthRef))
	if err != nil {
		return err
	}
	if current.Exists && (current.Value.Group != ref.Group || current.Value.AuthRef != ref.AuthRef || !hmac.Equal([]byte(current.Value.APISecret), []byte(secret))) {
		return metadata.ErrOwnershipConflict
	}
	existing, err := getTyped[metadata.GroupAuthReference](m, s.groupAuthReferenceKey(ref))
	if err != nil {
		return err
	}
	if existing.Exists {
		if existing.Value != ref || !current.Exists {
			return invalidRecord("Group authentication reference")
		}
		return nil
	}
	if !current.Exists {
		current.Value = metadata.GroupAuth{Group: ref.Group, AuthRef: ref.AuthRef, APISecret: secret}
		if err := putJSON(m.tr, s.groupAuthKey(ref.Group, ref.AuthRef), current.Value); err != nil {
			return err
		}
	}
	return putJSON(m.tr, s.groupAuthReferenceKey(ref), ref)
}

func (s *Store) releaseGroupAuth(m *mutation, ref metadata.GroupAuthReference) error {
	if err := validateGroupAuthReference(ref); err != nil {
		return err
	}
	existing, err := getTyped[metadata.GroupAuthReference](m, s.groupAuthReferenceKey(ref))
	if err != nil || !existing.Exists {
		return err
	}
	if existing.Value != ref {
		return invalidRecord("Group authentication reference identity")
	}
	current, err := getTyped[metadata.GroupAuth](m, s.groupAuthKey(ref.Group, ref.AuthRef))
	if err != nil {
		return err
	}
	if !current.Exists || current.Value.Group != ref.Group || current.Value.AuthRef != ref.AuthRef {
		return invalidRecord("Group authentication reference material")
	}
	clearRecord(m.tr, s.groupAuthReferenceKey(ref))
	// A bounded serializable prefix probe sees this transaction's deletion.
	// Concurrent retains conflict with final reclamation, while unrelated new
	// references never update one shared per-tenant counter.
	remaining, err := m.tr.Scan(s.groupAuthReferenceScope(ref.Group, ref.AuthRef), nil, pageLimit(1))
	if err != nil {
		return err
	}
	if len(remaining.Items) == 0 {
		clearRecord(m.tr, s.groupAuthKey(ref.Group, ref.AuthRef))
	}
	return nil
}

func (s *Store) ReadGroupAuth(ctx context.Context, group, authRef string) (out metadata.Versioned[metadata.GroupAuth], err error) {
	if err = nodegroup.ValidateName(group); err != nil {
		return
	}
	if !metadata.ValidFingerprint(authRef) {
		return out, invalidRecord("Group auth fingerprint")
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		var err error
		out, err = readTyped[metadata.GroupAuth](tr, s.groupAuthKey(group, authRef))
		if err == nil && out.Exists && (out.Value.Group != group || out.Value.AuthRef != authRef || !validGroupAuth(out.Value.AuthRef, out.Value.APISecret)) {
			return invalidRecord("Group authentication identity")
		}
		return err
	})
	return
}

// The number of authentication roots is independent of object reference count.
// Page roots, including those retained solely by existing objects, without
// copying object records or trusting the token's short candidate fingerprint.
func (s *Store) ListGroupAuth(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.GroupAuth]], error) {
	if err := nodegroup.ValidateName(group); err != nil {
		return metadata.Page[metadata.Versioned[metadata.GroupAuth]]{}, err
	}
	page, err := listTyped[metadata.GroupAuth](ctx, s, s.key("group-auth", group), cursor, limit)
	if err != nil {
		return page, err
	}
	for _, auth := range page.Items {
		if auth.Value.Group != group || !validGroupAuth(auth.Value.AuthRef, auth.Value.APISecret) {
			return metadata.Page[metadata.Versioned[metadata.GroupAuth]]{}, invalidRecord("Group authentication identity")
		}
	}
	return page, nil
}
