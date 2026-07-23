package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Backend type tags stored in sandbox_mmds_endpoints.backend_type. Mirrors
// mmdscfg.BackendStore/BackendRelay as independent local constants so this
// (foundational, widely depended-on) package does not import the
// metadata-parsing layer.
const (
	MMDSBackendStore = "store"
	MMDSBackendRelay = "relay"
)

// ErrMMDSEndpointNotFound is returned by the store/relay-auth mutation and
// read methods when (sandbox_id,name) has no declared endpoint.
var ErrMMDSEndpointNotFound = errors.New("store: mmds endpoint not found")

// ErrMMDSEndpointWrongBackend is returned when a store mutation targets a
// relay-backed endpoint or vice versa.
var ErrMMDSEndpointWrongBackend = errors.New("store: mmds endpoint has a different backend type")

// MMDSEndpoint is one immutable endpoint definition as declared at sandbox
// Create — inserted at revision 0, value_present=false (Create never carries
// a secret value; that arrives later via the admin API).
type MMDSEndpoint struct {
	Name             string
	Path             string
	BackendType      string // MMDSBackendStore | MMDSBackendRelay
	PublicConfigJSON string // validated non-secrets, e.g. {"url":...,"auth_header_name":...}; "" -> "{}"
}

// MMDSEndpointStatus is the redacted, guest/admin-safe view of one endpoint's
// current value state — never includes plaintext or ciphertext: the Sandbox
// GET representation exposes only name/path/backend_type/configured/
// revision/expired.
type MMDSEndpointStatus struct {
	Name        string
	Path        string
	BackendType string
	Configured  bool
	Revision    int64
	Expired     bool
}

// MMDSStoreValue is a decrypted value + its metadata, used by the endpoint-
// serving authority (internal/mmdsauth) to answer guest GETs. Never returned
// by anything admin-API-facing — the admin API only ever sees
// MMDSEndpointStatus.
type MMDSStoreValue struct {
	Value       []byte
	ContentType string
	ExpiresUnix int64
	Revision    int64
	Present     bool
}

const (
	mmdsRecordStore     = "store"      // AAD record-type tag for a store value
	mmdsRecordRelayAuth = "relay_auth" // AAD record-type tag for a relay auth value
)

// mmdsAAD builds the AEAD associated data binding a secret ciphertext to the
// exact row/revision it belongs to: record type + sandbox id + name + backend
// type + revision. A ciphertext sealed under one combination fails to open
// under any other, so it cannot be replayed into a different endpoint, a
// different backend type, or an earlier/later revision even if raw DB rows
// were copied or swapped.
func mmdsAAD(recordType, sandboxID, name, backendType string, revision int64) []byte {
	return []byte(fmt.Sprintf("mmds:%s:%s:%s:%s:%d", recordType, sandboxID, name, backendType, revision))
}

func emptyJSONDefault(s string) string {
	if s == "" {
		return "{}"
	}
	return s
}

// PutWithMMDSEndpoints upserts sb (identical to Put) and, when endpoints is
// non-empty, atomically inserts the given revision-0 endpoint rows in the
// same transaction: the sandbox row and its endpoint declarations succeed or
// fail together.
//
// launch is shared between Create and resume (Connect); on resume, callers
// MUST pass a nil/empty endpoints slice. This function never deletes or
// replaces existing endpoint rows for sb.ID — only ever inserts new ones —
// so a resume call leaves prior endpoint definitions completely untouched:
// ordinary later sandbox upserts never replace immutable endpoint
// definitions.
func (s *Store) PutWithMMDSEndpoints(ctx context.Context, sb *types.Sandbox, endpoints []MMDSEndpoint) error {
	hash, enc, err := s.encField(sb.ManifestKey)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: put %s: begin tx: %w", sb.ID, err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit has succeeded

	if _, err := tx.ExecContext(ctx, `
INSERT INTO sandboxes (id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,
  floatingip,vswitch_port,inner_ip,port_mac,manifest_key_hash,manifest_key_enc,snapshot_ref,
  envd_access_token,traffic_access_token,metadata_json,env_json,created_unix)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET
  template_id=excluded.template_id, state=excluded.state, deadline_unix=excluded.deadline_unix,
  run_dir=excluded.run_dir, base_dir=excluded.base_dir, run_id=excluded.run_id, envd_uds=excluded.envd_uds,
  ci_uds=excluded.ci_uds, floatingip=excluded.floatingip, vswitch_port=excluded.vswitch_port,
  inner_ip=excluded.inner_ip, port_mac=excluded.port_mac,
  manifest_key_hash=excluded.manifest_key_hash, manifest_key_enc=excluded.manifest_key_enc,
  snapshot_ref=excluded.snapshot_ref, envd_access_token=excluded.envd_access_token,
  traffic_access_token=excluded.traffic_access_token,
  metadata_json=excluded.metadata_json, env_json=excluded.env_json`,
		sb.ID, sb.TemplateID, string(sb.State), sb.DeadlineUnix, sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS,
		sb.CiUDS, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC, hash, enc, sb.SnapshotRef,
		sb.EnvdAccessToken, sb.TrafficAccessToken, mj(sb.Metadata), mj(sb.Env), sb.CreatedUnix); err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}

	now := time.Now().Unix()
	for _, ep := range endpoints {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO sandbox_mmds_endpoints
  (sandbox_id,name,path,backend_type,public_config_json,secret_ciphertext,content_type,expires_unix,revision,value_present,created_unix,updated_unix)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			sb.ID, ep.Name, ep.Path, ep.BackendType, emptyJSONDefault(ep.PublicConfigJSON), "", "", int64(0), int64(0), 0, now, now); err != nil {
			return fmt.Errorf("store: put %s: insert mmds endpoint %q: %w", sb.ID, ep.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: put %s: commit: %w", sb.ID, err)
	}
	return nil
}

// getMMDSEndpointRevision reads the current backend_type+revision for
// (sid,name), the CAS basis for a mutation.
func (s *Store) getMMDSEndpointRevision(ctx context.Context, sid, name string) (backendType string, revision int64, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT backend_type, revision FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name=?`, sid, name)
	if serr := row.Scan(&backendType, &revision); serr != nil {
		if errors.Is(serr, sql.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, serr
	}
	return backendType, revision, true, nil
}

// mutateMMDSSecret is the shared implementation behind
// Set/ClearMMDSStoreValue and Set/ClearMMDSRelayAuth: last-write-wins from
// the caller's perspective, implemented as a revision-guarded compare-and-
// swap so the new ciphertext's AAD (which embeds the new revision) always
// matches what actually lands in the row — a plain read-then-write could
// otherwise race a concurrent mutation and persist a ciphertext whose AAD
// revision doesn't match the row's real revision. plaintext==nil clears the
// value (DELETE); non-nil (including empty) sets it (PUT). Returns the new
// revision on success, surfaced up through orch to configsock's admin audit
// log (operation, revision, and result).
func (s *Store) mutateMMDSSecret(ctx context.Context, sid, name, wantBackend, recordType string, plaintext []byte, contentType string, expiresUnix int64) (int64, error) {
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		backendType, revision, found, err := s.getMMDSEndpointRevision(ctx, sid, name)
		if err != nil {
			return 0, fmt.Errorf("store: mutate mmds %s for %s/%s: %w", recordType, sid, name, err)
		}
		if !found {
			return 0, ErrMMDSEndpointNotFound
		}
		if backendType != wantBackend {
			return 0, ErrMMDSEndpointWrongBackend
		}

		newRevision := revision + 1
		var ciphertext string
		valuePresent := 0
		if plaintext != nil {
			if ciphertext, err = s.box.EncryptAAD(plaintext, mmdsAAD(recordType, sid, name, backendType, newRevision)); err != nil {
				return 0, fmt.Errorf("store: encrypt mmds %s for %s/%s: %w", recordType, sid, name, err)
			}
			valuePresent = 1
		}

		res, err := s.db.ExecContext(ctx, `
UPDATE sandbox_mmds_endpoints
SET secret_ciphertext=?, content_type=?, expires_unix=?, revision=?, value_present=?, updated_unix=?
WHERE sandbox_id=? AND name=? AND revision=?`,
			ciphertext, contentType, expiresUnix, newRevision, valuePresent, time.Now().Unix(),
			sid, name, revision)
		if err != nil {
			return 0, fmt.Errorf("store: update mmds %s for %s/%s: %w", recordType, sid, name, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return newRevision, nil
		}
		// Lost the CAS race to a concurrent mutation on the same endpoint; retry.
	}
	return 0, fmt.Errorf("store: update mmds %s for %s/%s: too much contention after %d attempts", recordType, sid, name, maxAttempts)
}

// SetMMDSStoreValue replaces the complete value for a store-backend endpoint.
// An omitted contentType resets to application/octet-stream.
func (s *Store) SetMMDSStoreValue(ctx context.Context, sid, name string, value []byte, contentType string, expiresUnix int64) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return s.mutateMMDSSecret(ctx, sid, name, MMDSBackendStore, mmdsRecordStore, value, contentType, expiresUnix)
}

// ClearMMDSStoreValue clears the value, Content-Type, and expiration for a
// store-backend endpoint and increments its revision.
func (s *Store) ClearMMDSStoreValue(ctx context.Context, sid, name string) (int64, error) {
	return s.mutateMMDSSecret(ctx, sid, name, MMDSBackendStore, mmdsRecordStore, nil, "", 0)
}

// SetMMDSRelayAuth replaces the complete auth value for a relay-backend
// endpoint.
func (s *Store) SetMMDSRelayAuth(ctx context.Context, sid, name string, value []byte) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return s.mutateMMDSSecret(ctx, sid, name, MMDSBackendRelay, mmdsRecordRelayAuth, value, "", 0)
}

// ClearMMDSRelayAuth clears the auth value for a relay-backend endpoint and
// increments its revision.
func (s *Store) ClearMMDSRelayAuth(ctx context.Context, sid, name string) (int64, error) {
	return s.mutateMMDSSecret(ctx, sid, name, MMDSBackendRelay, mmdsRecordRelayAuth, nil, "", 0)
}

// GetMMDSEndpointPublicConfig returns an endpoint's immutable, non-secret
// declared config (e.g. the relay URL + auth header name, as JSON) and its
// backend type. found=false when (sandbox_id,name) has no declared endpoint.
func (s *Store) GetMMDSEndpointPublicConfig(ctx context.Context, sid, name string) (publicConfigJSON, backendType string, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT public_config_json, backend_type FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name=?`, sid, name)
	if serr := row.Scan(&publicConfigJSON, &backendType); serr != nil {
		if errors.Is(serr, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, serr
	}
	return publicConfigJSON, backendType, true, nil
}

// MMDSEndpointFull is one endpoint's complete state — declaration plus its
// current decrypted value, if any — the source data for external-mode MMDS
// sync (routesync.MmdsEndpointEntry). Never exposed via the
// admin API; only ever used on the authenticated local sync stream.
type MMDSEndpointFull struct {
	SandboxID        string
	Name             string
	Path             string
	BackendType      string
	PublicConfigJSON string
	Revision         int64
	ValuePresent     bool
	ContentType      string
	ExpiresUnix      int64
	SecretPlaintext  []byte
}

// scanMMDSEndpointFull scans one sandbox_mmds_endpoints row (in the fixed
// column order both RangeMMDSEndpoints and GetMMDSEndpointFull use) and
// decrypts its value, if present.
func (s *Store) scanMMDSEndpointFull(row interface{ Scan(...any) error }) (MMDSEndpointFull, error) {
	var e MMDSEndpointFull
	var ciphertext string
	var valuePresent int
	if err := row.Scan(&e.SandboxID, &e.Name, &e.Path, &e.BackendType, &e.PublicConfigJSON,
		&ciphertext, &e.ContentType, &e.ExpiresUnix, &e.Revision, &valuePresent); err != nil {
		return MMDSEndpointFull{}, err
	}
	e.ValuePresent = valuePresent != 0
	if e.ValuePresent && ciphertext != "" {
		recordType := mmdsRecordStore
		if e.BackendType == MMDSBackendRelay {
			recordType = mmdsRecordRelayAuth
		}
		plain, err := s.box.DecryptAAD(ciphertext, mmdsAAD(recordType, e.SandboxID, e.Name, e.BackendType, e.Revision))
		if err != nil {
			return MMDSEndpointFull{}, fmt.Errorf("store: decrypt mmds value for %s/%s: %w", e.SandboxID, e.Name, err)
		}
		e.SecretPlaintext = plain
	}
	return e, nil
}

const mmdsEndpointFullCols = `sandbox_id,name,path,backend_type,public_config_json,secret_ciphertext,content_type,expires_unix,revision,value_present`

// GetMMDSEndpointFull returns one endpoint's complete state (declaration +
// decrypted current value). found=false when (sandbox_id,name) has no
// declared endpoint.
func (s *Store) GetMMDSEndpointFull(ctx context.Context, sid, name string) (MMDSEndpointFull, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+mmdsEndpointFullCols+` FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name=?`, sid, name)
	e, err := s.scanMMDSEndpointFull(row)
	if errors.Is(err, sql.ErrNoRows) {
		return MMDSEndpointFull{}, false, nil
	}
	if err != nil {
		return MMDSEndpointFull{}, false, err
	}
	return e, true, nil
}

// RangeMMDSEndpoints streams every currently-declared endpoint across every
// sandbox, each carrying its decrypted current value (if configured),
// through fn — the full-scan source for external-mode MMDS sync. Like
// store.RangeByState, fn MUST be read-only with respect to the store while
// the cursor is open.
func (s *Store) RangeMMDSEndpoints(ctx context.Context, fn func(MMDSEndpointFull) error) error {
	rows, err := s.db.QueryContext(ctx, `SELECT `+mmdsEndpointFullCols+` FROM sandbox_mmds_endpoints ORDER BY sandbox_id ASC, name ASC`)
	if err != nil {
		return fmt.Errorf("store: range mmds endpoints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		e, err := s.scanMMDSEndpointFull(rows)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}

// getMMDSSecret reads and decrypts the current value for (sid,name), scoped
// to wantBackend. found=false when the endpoint is unknown OR belongs to a
// different backend type (the caller asked the wrong question, not "it has
// no value").
func (s *Store) getMMDSSecret(ctx context.Context, sid, name, wantBackend, recordType string) (MMDSStoreValue, bool, error) {
	var v MMDSStoreValue
	var backendType, ciphertext, contentType string
	var valuePresent int
	row := s.db.QueryRowContext(ctx, `
SELECT backend_type, secret_ciphertext, content_type, expires_unix, revision, value_present
FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND name=?`, sid, name)
	if err := row.Scan(&backendType, &ciphertext, &contentType, &v.ExpiresUnix, &v.Revision, &valuePresent); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MMDSStoreValue{}, false, nil
		}
		return MMDSStoreValue{}, false, fmt.Errorf("store: get mmds %s for %s/%s: %w", recordType, sid, name, err)
	}
	if backendType != wantBackend {
		return MMDSStoreValue{}, false, nil
	}
	v.ContentType = contentType
	v.Present = valuePresent != 0
	if !v.Present || ciphertext == "" {
		return v, true, nil
	}
	plain, err := s.box.DecryptAAD(ciphertext, mmdsAAD(recordType, sid, name, backendType, v.Revision))
	if err != nil {
		return MMDSStoreValue{}, false, fmt.Errorf("store: decrypt mmds %s for %s/%s: %w", recordType, sid, name, err)
	}
	v.Value = plain
	return v, true, nil
}

// GetMMDSStoreValue returns the decrypted current value for a store-backend
// endpoint. found=false when the endpoint is unknown or is not store-backed.
func (s *Store) GetMMDSStoreValue(ctx context.Context, sid, name string) (MMDSStoreValue, bool, error) {
	return s.getMMDSSecret(ctx, sid, name, MMDSBackendStore, mmdsRecordStore)
}

// GetMMDSRelayAuth returns the decrypted current relay auth value.
// found=false when the endpoint is unknown or is not relay-backed.
func (s *Store) GetMMDSRelayAuth(ctx context.Context, sid, name string) (MMDSStoreValue, bool, error) {
	return s.getMMDSSecret(ctx, sid, name, MMDSBackendRelay, mmdsRecordRelayAuth)
}

// MMDSEndpointByPath is the guest-facing dispatch lookup: the exact
// (name,backend_type) declared for (sandbox_id,path), or found=false if no
// endpoint owns that exact path for that sandbox.
func (s *Store) MMDSEndpointByPath(ctx context.Context, sid, path string) (name, backendType string, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT name, backend_type FROM sandbox_mmds_endpoints WHERE sandbox_id=? AND path=?`, sid, path)
	if serr := row.Scan(&name, &backendType); serr != nil {
		if errors.Is(serr, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, serr
	}
	return name, backendType, true, nil
}

// ListMMDSEndpointStatus returns the redacted status of every endpoint
// declared for sid, ordered by name — the shape the Sandbox GET
// representation and the admin API expose (never plaintext/ciphertext).
func (s *Store) ListMMDSEndpointStatus(ctx context.Context, sid string) ([]MMDSEndpointStatus, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT name, path, backend_type, value_present, revision, expires_unix
FROM sandbox_mmds_endpoints WHERE sandbox_id=? ORDER BY name ASC`, sid)
	if err != nil {
		return nil, fmt.Errorf("store: list mmds endpoints for %s: %w", sid, err)
	}
	defer rows.Close()
	now := time.Now().Unix()
	var out []MMDSEndpointStatus
	for rows.Next() {
		var st MMDSEndpointStatus
		var valuePresent int
		var expiresUnix int64
		if err := rows.Scan(&st.Name, &st.Path, &st.BackendType, &valuePresent, &st.Revision, &expiresUnix); err != nil {
			return nil, err
		}
		st.Configured = valuePresent != 0
		st.Expired = expiresUnix > 0 && now >= expiresUnix
		out = append(out, st)
	}
	return out, rows.Err()
}
