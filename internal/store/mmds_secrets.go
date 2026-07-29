package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MMDSSecretValue is one named value inside a sandbox's decrypted MMDS secret
// blob. Body is opaque bytes, base64-encoded so the payload stays valid JSON
// regardless of content (secret values are not required to be valid UTF-8,
// unlike Phase 1's static route data).
type MMDSSecretValue struct {
	BodyBase64  string `json:"body_base64"`
	ContentType string `json:"content_type"`
	ExpiresUnix int64  `json:"expires_unix,omitempty"`
}

// MMDSSecretBlob is the full decrypted per-sandbox blob: one row in
// sandbox_mmds_secrets holds exactly one of these, keyed by name inside. This
// is also the exact wire shape of routesync.RouteEntry.MMDSSecrets, so the
// proxy master's RPC handler (cmd/node-ctl/proxy.go) unmarshals this same
// type from whatever it received over routesync, without a second copy of
// the JSON tags to keep in sync. Revision is populated only by
// GetMMDSSecretBlob (the mutate path never marshals it into ciphertext --
// it's synced state, not persisted state, so it isn't part of the AAD).
type MMDSSecretBlob struct {
	Version  int                        `json:"version"`
	Revision int64                      `json:"revision,omitempty"`
	Values   map[string]MMDSSecretValue `json:"values"`
}

// mmdsSecretAAD binds a sandbox's encrypted MMDS secret blob to its owner,
// its specification state, and the revision being written/read, so a copied
// ciphertext cannot be substituted into another sandbox, another specification
// state, or another revision.
func mmdsSecretAAD(sandboxID, configDigest string, revision int64) []byte {
	return []byte(fmt.Sprintf("mmds_secret:%s:%s:%d", sandboxID, configDigest, revision))
}

// rawGetMMDSSecretRow reads sid's raw (still-encrypted) row. found=false
// means no row exists yet -- equivalent to every name being at revision 0.
func (s *Store) rawGetMMDSSecretRow(ctx context.Context, sid string) (configDigest string, revision int64, ciphertext string, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `
SELECT config_digest, revision, secret_ciphertext FROM sandbox_mmds_secrets WHERE sandbox_id=?`, sid)
	err = row.Scan(&configDigest, &revision, &ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, "", false, nil
	}
	if err != nil {
		return "", 0, "", false, fmt.Errorf("store: get mmds secret row for %s: %w", sid, err)
	}
	return configDigest, revision, ciphertext, true, nil
}

func (s *Store) decryptMMDSSecretPayload(sid, configDigest, ciphertext string, revision int64) (MMDSSecretBlob, error) {
	payload := MMDSSecretBlob{Version: 1, Values: map[string]MMDSSecretValue{}}
	if ciphertext == "" {
		return payload, nil
	}
	plaintext, err := s.box.DecryptAAD(ciphertext, mmdsSecretAAD(sid, configDigest, revision))
	if err != nil {
		return MMDSSecretBlob{}, fmt.Errorf("store: decrypt mmds secrets for %s: %w", sid, err)
	}
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		return MMDSSecretBlob{}, fmt.Errorf("store: decode mmds secrets for %s: %w", sid, err)
	}
	if payload.Values == nil {
		payload.Values = map[string]MMDSSecretValue{}
	}
	return payload, nil
}

// mutateMMDSSecrets applies mutate to sid's decrypted secret-value map and
// persists the result under a fresh revision, CAS-guarded against concurrent
// writers (8-attempt bound, no backoff). mutate returns changed=false to
// signal a no-op (e.g. deleting an already-absent name) -- mutateMMDSSecrets
// then returns the current revision without writing anything, so a
// no-op delete never bumps the revision or touches the row.
//
// The INSERT ... ON CONFLICT ... WHERE form handles both "no row yet" (plain
// insert) and "CAS-guarded update" (the WHERE clause on the conflict action
// fails to match a revision that moved since our read, so SQLite silently
// skips the update and RowsAffected is 0) in one statement -- unlike Phase
// 1-adjacent per-row schemas, there is no insert-at-Create step here, so the
// first-ever PUT for a sandbox has no existing row to UPDATE.
func (s *Store) mutateMMDSSecrets(ctx context.Context, sid, configDigest string, mutate func(values map[string]MMDSSecretValue) (changed bool)) (int64, error) {
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		curDigest, revision, ciphertext, found, err := s.rawGetMMDSSecretRow(ctx, sid)
		if err != nil {
			return 0, err
		}
		var payload MMDSSecretBlob
		if found {
			payload, err = s.decryptMMDSSecretPayload(sid, curDigest, ciphertext, revision)
			if err != nil {
				return 0, err
			}
		} else {
			payload = MMDSSecretBlob{Version: 1, Values: map[string]MMDSSecretValue{}}
		}
		if !mutate(payload.Values) {
			return revision, nil
		}
		newRevision := revision + 1
		plaintext, err := json.Marshal(payload)
		if err != nil {
			return 0, fmt.Errorf("store: encode mmds secrets for %s: %w", sid, err)
		}
		newCiphertext, err := s.box.EncryptAAD(plaintext, mmdsSecretAAD(sid, configDigest, newRevision))
		if err != nil {
			return 0, fmt.Errorf("store: encrypt mmds secrets for %s: %w", sid, err)
		}
		res, err := s.db.ExecContext(ctx, `
INSERT INTO sandbox_mmds_secrets (sandbox_id, config_digest, revision, secret_ciphertext, updated_unix)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(sandbox_id) DO UPDATE SET
  config_digest=excluded.config_digest,
  revision=excluded.revision,
  secret_ciphertext=excluded.secret_ciphertext,
  updated_unix=excluded.updated_unix
WHERE sandbox_mmds_secrets.revision=?`,
			sid, configDigest, newRevision, newCiphertext, time.Now().Unix(), revision)
		if err != nil {
			return 0, fmt.Errorf("store: persist mmds secrets for %s: %w", sid, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return newRevision, nil
		}
		// Lost the CAS race to a concurrent mutation on the same sandbox; retry.
	}
	return 0, fmt.Errorf("store: mutate mmds secrets for %s: too much contention after %d attempts", sid, maxAttempts)
}

// SetMMDSSecret upserts one named value into sid's secret blob (creating the
// row if absent), CAS-guarded. configDigest is the sandbox's current
// canonical MMDS specification digest (sandboxcfg.MMDSConfigDigest), bound into
// the encryption AAD. contentType=="" defaults to application/octet-stream.
func (s *Store) SetMMDSSecret(ctx context.Context, sid, configDigest, name string, value []byte, contentType string, expiresUnix int64) (int64, error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return s.mutateMMDSSecrets(ctx, sid, configDigest, func(values map[string]MMDSSecretValue) bool {
		values[name] = MMDSSecretValue{
			BodyBase64:  base64.StdEncoding.EncodeToString(value),
			ContentType: contentType,
			ExpiresUnix: expiresUnix,
		}
		return true
	})
}

// ClearMMDSSecret removes one named value from sid's secret blob (other names
// are untouched). A name that was never configured (no row, or the row
// exists but never had this name set) is a no-op: returns the current
// revision (0 if no row exists) and no error -- the caller (the admin API)
// treats DELETE as idempotent.
func (s *Store) ClearMMDSSecret(ctx context.Context, sid, configDigest, name string) (int64, error) {
	return s.mutateMMDSSecrets(ctx, sid, configDigest, func(values map[string]MMDSSecretValue) bool {
		if _, ok := values[name]; !ok {
			return false
		}
		delete(values, name)
		return true
	})
}

// GetMMDSSecretBlob returns sid's decrypted, canonical secrets JSON (the
// exact wire value for routesync.RouteEntry.MMDSSecrets) and its revision.
// found=false means no row exists yet (equivalent to every specified secret
// being at revision 0 / never configured).
func (s *Store) GetMMDSSecretBlob(ctx context.Context, sid string) (blobJSON string, revision int64, found bool, err error) {
	configDigest, revision, ciphertext, found, err := s.rawGetMMDSSecretRow(ctx, sid)
	if err != nil {
		return "", 0, false, err
	}
	if !found {
		return "", 0, false, nil
	}
	payload, err := s.decryptMMDSSecretPayload(sid, configDigest, ciphertext, revision)
	if err != nil {
		return "", 0, false, err
	}
	payload.Revision = revision
	blob, err := json.Marshal(payload)
	if err != nil {
		return "", 0, false, fmt.Errorf("store: encode mmds secrets blob for %s: %w", sid, err)
	}
	return string(blob), revision, true, nil
}

// GetMMDSSecretValue resolves one name out of sid's secret blob. present
// distinguishes "configured" from "never configured, revoked, or expired";
// revision lets the caller tell "never configured" (revision==0) apart from
// "revoked" (revision>0, present=false) -- only the former is worth a bounded
// wait. An expired value (ExpiresUnix>0 and in the past) is reported exactly
// like a revoked one: present=false with the row's real (nonzero) revision,
// so a caller never retries waiting for it to reappear.
func (s *Store) GetMMDSSecretValue(ctx context.Context, sid, name string) (value MMDSSecretValue, present bool, revision int64, err error) {
	configDigest, revision, ciphertext, found, err := s.rawGetMMDSSecretRow(ctx, sid)
	if err != nil {
		return MMDSSecretValue{}, false, 0, err
	}
	if !found {
		return MMDSSecretValue{}, false, 0, nil
	}
	payload, err := s.decryptMMDSSecretPayload(sid, configDigest, ciphertext, revision)
	if err != nil {
		return MMDSSecretValue{}, false, revision, err
	}
	v, ok := payload.Values[name]
	if ok && MMDSSecretExpired(v) {
		return MMDSSecretValue{}, false, revision, nil
	}
	return v, ok, revision, nil
}

// MMDSSecretExpired reports whether v's ExpiresUnix has already passed.
// ExpiresUnix==0 means "never expires". Exported so every reader of a
// store.MMDSSecretBlob -- not just this package's own GetMMDSSecretValue --
// applies the exact same expiry rule; cmd/node-ctl/proxy.go's mmdsRPCHandler
// (external mode, reading a synced MMDSSecretBlob rather than calling
// GetMMDSSecretValue directly) is the other caller.
func MMDSSecretExpired(v MMDSSecretValue) bool {
	return v.ExpiresUnix > 0 && v.ExpiresUnix <= time.Now().Unix()
}
