package metastore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
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

// ListGroupAuthCandidates uses the existing ordered full-fingerprint keys. All
// fingerprints are fixed-width lower-case hex components, below the record
// codec's hashing threshold, so their physical and logical order agree. No
// secondary index or migration of retained authentication roots is needed.
//
// A page reads at most limit roots. Once a different prefix is reached, the
// search ends; even an absent candidate reads at most one unrelated page.
// Candidate-fingerprint collisions remain paginated, never truncated or trusted.
func (s *Store) ListGroupAuthCandidates(ctx context.Context, group, candidate string, cursor metadata.Cursor, limit int) (out metadata.Page[metadata.Versioned[metadata.GroupAuth]], err error) {
	if err = nodegroup.ValidateName(group); err != nil {
		return
	}
	if len(candidate) != apikey.CandidateFingerprintLen*2 || !metadata.ValidFingerprint(candidate+strings.Repeat("0", (apikey.FullFingerprintLen-apikey.CandidateFingerprintLen)*2)) {
		return out, invalidRecord("Group auth candidate fingerprint")
	}
	if limit < 1 || limit > metadata.MaxGroupSupportBatchRows {
		return out, invalidRecord("Group auth candidate page limit must be 1..128")
	}
	scope := s.key("group-auth", group)
	last, err := decodeCursor(scope, cursor)
	if err != nil {
		return out, err
	}
	if last == nil {
		// Scan's lower bound is exclusive. NUL padding sorts before every hex
		// suffix, including a legitimate fingerprint ending in forty zeroes.
		last = scope.Child(candidate + strings.Repeat("\x00", (apikey.FullFingerprintLen-apikey.CandidateFingerprintLen)*2))
	} else if ref, err := groupAuthKeyFingerprint(scope, last); err != nil || !strings.HasPrefix(ref, candidate) {
		return out, invalidRecord("Group auth candidate cursor")
	}
	err = s.read(ctx, func(tr *records.Tx) error {
		out = metadata.Page[metadata.Versioned[metadata.GroupAuth]]{}
		page, err := scanGroupAuthCandidates(tr, scope, last, candidate, limit)
		if err != nil {
			return err
		}
		for _, item := range page.Items {
			auth, err := typed[metadata.GroupAuth](wrapRecord(item.Value))
			if err != nil {
				return err
			}
			if auth.Value.Group != group || !bytes.Equal(item.Key, s.groupAuthKey(group, auth.Value.AuthRef)) || !validGroupAuth(auth.Value.AuthRef, auth.Value.APISecret) {
				return invalidRecord("Group authentication identity")
			}
			out.Items = append(out.Items, auth)
		}
		if page.More && len(page.Items) > 0 {
			out.Next = encodeCursor(scope, page.Items[len(page.Items)-1].Key)
		}
		return nil
	})
	return
}

func groupAuthKeyFingerprint(scope, key records.Key) (string, error) {
	if len(key) != len(scope)+4+apikey.FullFingerprintLen*2 {
		return "", invalidRecord("Group auth key identity")
	}
	ref := string(key[len(scope)+4:])
	if !metadata.ValidFingerprint(ref) || !bytes.Equal(key, scope.Child(ref)) {
		return "", invalidRecord("Group auth key identity")
	}
	return ref, nil
}

// Keep range selection separate from authentication-material validation: the
// first key outside the candidate range ends the page without decoding its JSON.
func scanGroupAuthCandidates(tr *records.Tx, scope, after records.Key, candidate string, limit int) (records.Page, error) {
	page, err := tr.Scan(scope, after, pageLimit(limit))
	if err != nil {
		return records.Page{}, err
	}
	for i, item := range page.Items {
		ref, err := groupAuthKeyFingerprint(scope, item.Key)
		if err != nil {
			return records.Page{}, err
		}
		if !strings.HasPrefix(ref, candidate) {
			page.Items = page.Items[:i]
			page.More = false
			break
		}
	}
	return page, nil
}
