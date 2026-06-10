// Package store persists sandbox/build records in a node-local sqlite database.
// modernc.org/sqlite is a pure-Go driver (CGO_ENABLED=0). Per-tenant manifest
// keys are stored AES-256-GCM-encrypted (secretbox); a non-unique
// manifest_key_hash index (= apikey.Fingerprint) enables O(1) matching, with the
// full key recovered by decrypt for HMAC verification.
package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
	_ "modernc.org/sqlite"
)

type Store struct {
	db  *sql.DB
	box *secretbox.Box
}

const schema = `
CREATE TABLE IF NOT EXISTS sandboxes (
  id                   TEXT PRIMARY KEY,
  template_id          TEXT NOT NULL,
  state                TEXT NOT NULL,
  deadline_unix        INTEGER NOT NULL DEFAULT 0,
  run_dir              TEXT NOT NULL,
  base_dir             TEXT NOT NULL,
  envd_uds             TEXT NOT NULL DEFAULT '',
  ci_uds               TEXT NOT NULL DEFAULT '',
  floatingip           TEXT NOT NULL DEFAULT '',
  vswitch_port         TEXT NOT NULL DEFAULT '',
  inner_ip             TEXT NOT NULL DEFAULT '',
  port_mac             TEXT NOT NULL DEFAULT '',
  manifest_key_hash    TEXT NOT NULL,
  manifest_key_enc     TEXT NOT NULL,
  snapshot_ref         TEXT NOT NULL DEFAULT '',
  envd_access_token    TEXT NOT NULL DEFAULT '',
  traffic_access_token TEXT NOT NULL DEFAULT '',
  metadata_json        TEXT NOT NULL DEFAULT '{}',
  env_json             TEXT NOT NULL DEFAULT '{}',
  created_unix         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sandboxes_state ON sandboxes(state);
CREATE INDEX IF NOT EXISTS idx_sandboxes_mkhash ON sandboxes(manifest_key_hash);

CREATE TABLE IF NOT EXISTS builds (
  build_id          TEXT PRIMARY KEY,
  template_id       TEXT NOT NULL,
  persist_id        TEXT NOT NULL DEFAULT '',
  manifest_key_hash TEXT NOT NULL,
  manifest_key_enc  TEXT NOT NULL,
  profile           TEXT NOT NULL,
  kind              TEXT NOT NULL,
  from_image        TEXT NOT NULL DEFAULT '',
  start_cmd         TEXT NOT NULL DEFAULT '',
  status            TEXT NOT NULL,
  reason            TEXT NOT NULL DEFAULT '',
  names_json        TEXT NOT NULL DEFAULT '[]',
  aliases_json      TEXT NOT NULL DEFAULT '[]',
  created_unix      INTEGER NOT NULL,
  registry_auth_enc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_builds_status ON builds(status);
CREATE INDEX IF NOT EXISTS idx_builds_mkhash ON builds(manifest_key_hash);

CREATE TABLE IF NOT EXISTS manifest_keys (
  key_hash          TEXT NOT NULL,
  key_enc           TEXT NOT NULL,
  label             TEXT NOT NULL DEFAULT '',
  created_unix      INTEGER NOT NULL,
  expires_unix      INTEGER NOT NULL DEFAULT 0,
  registry_auth_enc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_manifest_keys_hash ON manifest_keys(key_hash);
`

// Open opens (creating if needed) the sqlite store with the encryption box used
// for manifest keys at rest. The file should be 0600.
func Open(path string, box *secretbox.Box) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	return &Store{db: db, box: box}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ManifestKeyHash is hex(apikey.Fingerprint(rawKey)) — the non-unique match index
// equal to the fingerprint embedded in api keys. Exported so callers can derive a
// match key from an api key's fp without re-deriving the hash scheme.
func ManifestKeyHash(manifestKeyHex string) (string, error) {
	raw, err := hex.DecodeString(manifestKeyHex)
	if err != nil {
		return "", fmt.Errorf("store: manifest key not hex: %w", err)
	}
	return hex.EncodeToString(apikey.Fingerprint(raw)), nil
}

// encField returns the (hash, ciphertext) for a manifest key (hex) at rest.
func (s *Store) encField(manifestKeyHex string) (hash, enc string, err error) {
	if hash, err = ManifestKeyHash(manifestKeyHex); err != nil {
		return "", "", err
	}
	enc, err = s.box.EncryptString(manifestKeyHex)
	return hash, enc, err
}

func mj(m map[string]string) string {
	if m == nil {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func uj(s string) map[string]string {
	m := map[string]string{}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

func mjs(s []string) string {
	if s == nil {
		return "[]"
	}
	b, _ := json.Marshal(s)
	return string(b)
}

func ujs(s string) []string {
	var v []string
	_ = json.Unmarshal([]byte(s), &v)
	return v
}

// --- sandboxes ---

// Put upserts a sandbox record (manifest key encrypted + hashed).
func (s *Store) Put(ctx context.Context, sb *types.Sandbox) error {
	hash, enc, err := s.encField(sb.ManifestKey)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO sandboxes (id,template_id,state,deadline_unix,run_dir,base_dir,envd_uds,ci_uds,
  floatingip,vswitch_port,inner_ip,port_mac,manifest_key_hash,manifest_key_enc,snapshot_ref,
  envd_access_token,traffic_access_token,metadata_json,env_json,created_unix)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET
  template_id=excluded.template_id, state=excluded.state, deadline_unix=excluded.deadline_unix,
  run_dir=excluded.run_dir, base_dir=excluded.base_dir, envd_uds=excluded.envd_uds,
  ci_uds=excluded.ci_uds, floatingip=excluded.floatingip, vswitch_port=excluded.vswitch_port,
  inner_ip=excluded.inner_ip, port_mac=excluded.port_mac,
  manifest_key_hash=excluded.manifest_key_hash, manifest_key_enc=excluded.manifest_key_enc,
  snapshot_ref=excluded.snapshot_ref, envd_access_token=excluded.envd_access_token,
  traffic_access_token=excluded.traffic_access_token,
  metadata_json=excluded.metadata_json, env_json=excluded.env_json`,
		sb.ID, sb.TemplateID, string(sb.State), sb.DeadlineUnix, sb.RunDir, sb.BaseDir, sb.EnvdUDS,
		sb.CiUDS, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC, hash, enc, sb.SnapshotRef,
		sb.EnvdAccessToken, sb.TrafficAccessToken, mj(sb.Metadata), mj(sb.Env), sb.CreatedUnix)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}
	return nil
}

var cols = `id,template_id,state,deadline_unix,run_dir,base_dir,envd_uds,ci_uds,floatingip,
  vswitch_port,inner_ip,port_mac,manifest_key_hash,manifest_key_enc,snapshot_ref,
  envd_access_token,traffic_access_token,metadata_json,env_json,created_unix`

func (s *Store) scan(row interface{ Scan(...any) error }) (*types.Sandbox, error) {
	var sb types.Sandbox
	var st, meta, env, mkHash, mkEnc string
	if err := row.Scan(&sb.ID, &sb.TemplateID, &st, &sb.DeadlineUnix, &sb.RunDir, &sb.BaseDir,
		&sb.EnvdUDS, &sb.CiUDS, &sb.FloatingIP, &sb.VswitchPort, &sb.InnerIP, &sb.PortMAC,
		&mkHash, &mkEnc, &sb.SnapshotRef, &sb.EnvdAccessToken, &sb.TrafficAccessToken,
		&meta, &env, &sb.CreatedUnix); err != nil {
		return nil, err
	}
	mk, err := s.box.DecryptString(mkEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt manifest key for %s: %w", sb.ID, err)
	}
	sb.ManifestKey = mk
	sb.State = types.State(st)
	sb.Metadata, sb.Env = uj(meta), uj(env)
	return &sb, nil
}

// Get returns the sandbox or (nil, nil) if not found.
func (s *Store) Get(ctx context.Context, id string) (*types.Sandbox, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+cols+` FROM sandboxes WHERE id=?`, id)
	sb, err := s.scan(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get %s: %w", id, err)
	}
	return sb, nil
}

// List returns sandboxes ordered by id (cursor pagination). ownerHash != "" scopes
// the result to a manifest_key_hash (a fast, non-unique pre-filter — the caller
// still verifies the api key per row); "" returns all (internal callers).
func (s *Store) List(ctx context.Context, state, ownerHash string, limit int, cursor string) ([]*types.Sandbox, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q := `SELECT ` + cols + ` FROM sandboxes`
	var args []any
	var conds []string
	if state != "" {
		conds = append(conds, "state=?")
		args = append(args, state)
	}
	if ownerHash != "" {
		conds = append(conds, "manifest_key_hash=?")
		args = append(args, ownerHash)
	}
	if cursor != "" {
		conds = append(conds, "id > ?")
		args = append(args, cursor)
	}
	for i, c := range conds {
		if i == 0 {
			q += " WHERE "
		} else {
			q += " AND "
		}
		q += c
	}
	q += " ORDER BY id ASC LIMIT ?"
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("store: list: %w", err)
	}
	defer rows.Close()
	var out []*types.Sandbox
	for rows.Next() {
		sb, err := s.scan(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, sb)
	}
	next := ""
	if len(out) > limit {
		next = out[limit-1].ID
		out = out[:limit]
	}
	return out, next, rows.Err()
}

// ListByState returns all sandboxes in a state (used by the reaper/reconciler).
func (s *Store) ListByState(ctx context.Context, state types.State) ([]*types.Sandbox, error) {
	out, _, err := s.List(ctx, string(state), "", 1000, "")
	return out, err
}

func (s *Store) SetState(ctx context.Context, id string, st types.State) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=? WHERE id=?`, string(st), id)
	return err
}

func (s *Store) SetDeadline(ctx context.Context, id string, unix int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET deadline_unix=? WHERE id=?`, unix, id)
	return err
}

func (s *Store) SetSnapshotRef(ctx context.Context, id, ref string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET snapshot_ref=? WHERE id=?`, ref, id)
	return err
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sandboxes WHERE id=?`, id)
	return err
}

// --- builds (also the template registry) ---

var buildCols = `build_id,template_id,persist_id,manifest_key_hash,manifest_key_enc,profile,kind,
  from_image,start_cmd,status,reason,names_json,aliases_json,created_unix,registry_auth_enc`

func (s *Store) scanBuild(row interface{ Scan(...any) error }) (*types.Build, error) {
	var b types.Build
	var profile, kind, status, names, aliases, mkHash, mkEnc, raEnc string
	if err := row.Scan(&b.BuildID, &b.TemplateID, &b.PersistID, &mkHash, &mkEnc, &profile, &kind,
		&b.FromImage, &b.StartCmd, &status, &b.Reason, &names, &aliases, &b.CreatedUnix, &raEnc); err != nil {
		return nil, err
	}
	mk, err := s.box.DecryptString(mkEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt manifest key for build %s: %w", b.BuildID, err)
	}
	b.ManifestKey = mk
	if raEnc != "" {
		if b.RegistryAuth, err = s.box.DecryptString(raEnc); err != nil {
			return nil, fmt.Errorf("store: decrypt registry auth for build %s: %w", b.BuildID, err)
		}
	}
	b.Profile, b.Kind, b.Status = types.Profile(profile), types.Kind(kind), types.BuildState(status)
	b.Names, b.Aliases = ujs(names), ujs(aliases)
	return &b, nil
}

// PutBuild upserts a build record (manifest key + registry auth encrypted at rest).
func (s *Store) PutBuild(ctx context.Context, b *types.Build) error {
	hash, enc, err := s.encField(b.ManifestKey)
	if err != nil {
		return fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	var raEnc string
	if b.RegistryAuth != "" {
		if raEnc, err = s.box.EncryptString(b.RegistryAuth); err != nil {
			return fmt.Errorf("store: put build %s: encrypt registry auth: %w", b.BuildID, err)
		}
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO builds (build_id,template_id,persist_id,manifest_key_hash,manifest_key_enc,profile,kind,
  from_image,start_cmd,status,reason,names_json,aliases_json,created_unix,registry_auth_enc)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(build_id) DO UPDATE SET
  template_id=excluded.template_id, persist_id=excluded.persist_id,
  manifest_key_hash=excluded.manifest_key_hash, manifest_key_enc=excluded.manifest_key_enc,
  profile=excluded.profile, kind=excluded.kind, from_image=excluded.from_image,
  start_cmd=excluded.start_cmd, status=excluded.status, reason=excluded.reason,
  names_json=excluded.names_json, aliases_json=excluded.aliases_json,
  registry_auth_enc=excluded.registry_auth_enc`,
		b.BuildID, b.TemplateID, b.PersistID, hash, enc, string(b.Profile), string(b.Kind),
		b.FromImage, b.StartCmd, string(b.Status), b.Reason, mjs(b.Names), mjs(b.Aliases), b.CreatedUnix, raEnc)
	if err != nil {
		return fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	return nil
}

// GetBuild returns the build or (nil, nil) if not found.
func (s *Store) GetBuild(ctx context.Context, buildID string) (*types.Build, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE build_id=?`, buildID)
	b, err := s.scanBuild(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get build %s: %w", buildID, err)
	}
	return b, nil
}

// BuildsByStatus returns builds in a given state (used by the builder pool + ListTemplates).
func (s *Store) BuildsByStatus(ctx context.Context, status types.BuildState) ([]*types.Build, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds WHERE status=? ORDER BY created_unix ASC`, string(status))
	if err != nil {
		return nil, fmt.Errorf("store: builds by status: %w", err)
	}
	defer rows.Close()
	var out []*types.Build
	for rows.Next() {
		b, err := s.scanBuild(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// CASBuildStatus atomically moves a build from one status to another, returning
// whether it won the transition (lets multiple pool workers race for a build).
func (s *Store) CASBuildStatus(ctx context.Context, buildID string, from, to types.BuildState) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE builds SET status=? WHERE build_id=? AND status=?`,
		string(to), buildID, string(from))
	if err != nil {
		return false, fmt.Errorf("store: cas build %s: %w", buildID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// --- manifest_keys (the create/build allowlist) ---

// ManifestKeyInfo is a non-secret view of a manifest_keys row (no key material).
type ManifestKeyInfo struct {
	Hash        string // hex fingerprint (24 chars)
	Label       string
	CreatedUnix int64
	ExpiresUnix int64 // 0 = never expires
}

// AddManifestKey inserts a manifest key (hex) into the allowlist, or refreshes it if
// already present (dedup by decrypt-compare, since the short hash is non-unique).
// ttlSec>0 sets expiry to now+ttlSec; ttlSec<=0 means never expires. registryAuthJSON
// (a docker config.json; "" = leave) is the tenant's default pull credentials, stored
// encrypted. Re-adding refreshes the expiry (and the label / registry auth if newly
// given). Returns whether a NEW row was inserted (false = refreshed an existing one).
func (s *Store) AddManifestKey(ctx context.Context, manifestKeyHex, label string, ttlSec int64, registryAuthJSON string) (bool, error) {
	hash, enc, err := s.encField(manifestKeyHex)
	if err != nil {
		return false, err
	}
	var expires int64
	if ttlSec > 0 {
		expires = time.Now().Unix() + ttlSec
	}
	var raEnc string
	if registryAuthJSON != "" {
		if raEnc, err = s.box.EncryptString(registryAuthJSON); err != nil {
			return false, err
		}
	}
	rid, found, err := s.findManifestKeyRow(ctx, manifestKeyHex, hash)
	if err != nil {
		return false, err
	}
	if found { // re-add: always refresh expiry; update label / registry auth only if given
		if _, err := s.db.ExecContext(ctx, `UPDATE manifest_keys SET expires_unix=? WHERE rowid=?`, expires, rid); err != nil {
			return false, err
		}
		if label != "" {
			if _, err := s.db.ExecContext(ctx, `UPDATE manifest_keys SET label=? WHERE rowid=?`, label, rid); err != nil {
				return false, err
			}
		}
		if registryAuthJSON != "" {
			if _, err := s.db.ExecContext(ctx, `UPDATE manifest_keys SET registry_auth_enc=? WHERE rowid=?`, raEnc, rid); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO manifest_keys (key_hash,key_enc,label,created_unix,expires_unix,registry_auth_enc) VALUES (?,?,?,?,?,?)`,
		hash, enc, label, time.Now().Unix(), expires, raEnc); err != nil {
		return false, fmt.Errorf("store: add manifest key: %w", err)
	}
	return true, nil
}

// RegistryAuthForKey returns the tenant's stored registry auth (docker config.json),
// or "" if none. Matched by decrypt-compare (the hash is non-unique).
func (s *Store) RegistryAuthForKey(ctx context.Context, manifestKeyHex string) (string, error) {
	hash, err := ManifestKeyHash(manifestKeyHex)
	if err != nil {
		return "", err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT key_enc,registry_auth_enc FROM manifest_keys WHERE key_hash=?`, hash)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var keyEnc, raEnc string
		if err := rows.Scan(&keyEnc, &raEnc); err != nil {
			return "", err
		}
		if mk, err := s.box.DecryptString(keyEnc); err == nil && mk == manifestKeyHex {
			if raEnc == "" {
				return "", nil
			}
			return s.box.DecryptString(raEnc)
		}
	}
	return "", rows.Err()
}

// findManifestKeyRow returns the rowid of the manifest_keys row whose decrypted key
// equals manifestKeyHex (the hash is non-unique, so decrypt-compare is authoritative).
func (s *Store) findManifestKeyRow(ctx context.Context, manifestKeyHex, hash string) (int64, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rowid,key_enc FROM manifest_keys WHERE key_hash=?`, hash)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var rid int64
		var enc string
		if err := rows.Scan(&rid, &enc); err != nil {
			return 0, false, err
		}
		if mk, err := s.box.DecryptString(enc); err == nil && mk == manifestKeyHex {
			return rid, true, nil
		}
	}
	return 0, false, rows.Err()
}

// HasManifestKey reports whether the manifest key (hex) is in the allowlist.
func (s *Store) HasManifestKey(ctx context.Context, manifestKeyHex string) (bool, error) {
	keys, err := s.AllowedManifestKeysByHash(ctx, mustHash(manifestKeyHex))
	if err != nil {
		return false, err
	}
	for _, k := range keys {
		if k == manifestKeyHex {
			return true, nil
		}
	}
	return false, nil
}

// RemoveManifestKey deletes matching manifest-key rows; returns the count removed.
func (s *Store) RemoveManifestKey(ctx context.Context, manifestKeyHex string) (int, error) {
	hash, err := ManifestKeyHash(manifestKeyHex)
	if err != nil {
		return 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT rowid,key_enc FROM manifest_keys WHERE key_hash=?`, hash)
	if err != nil {
		return 0, err
	}
	var victims []int64
	for rows.Next() {
		var rid int64
		var enc string
		if err := rows.Scan(&rid, &enc); err != nil {
			rows.Close()
			return 0, err
		}
		if mk, err := s.box.DecryptString(enc); err == nil && mk == manifestKeyHex {
			victims = append(victims, rid)
		}
	}
	rows.Close()
	for _, rid := range victims {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM manifest_keys WHERE rowid=?`, rid); err != nil {
			return 0, err
		}
	}
	return len(victims), nil
}

// ListManifestKeys returns the allowlist as non-secret fingerprints + labels.
func (s *Store) ListManifestKeys(ctx context.Context) ([]ManifestKeyInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key_hash,label,created_unix,expires_unix FROM manifest_keys ORDER BY created_unix ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ManifestKeyInfo
	for rows.Next() {
		var mi ManifestKeyInfo
		if err := rows.Scan(&mi.Hash, &mi.Label, &mi.CreatedUnix, &mi.ExpiresUnix); err != nil {
			return nil, err
		}
		out = append(out, mi)
	}
	return out, rows.Err()
}

// AllowedManifestKeysByHash returns the decrypted, NON-EXPIRED manifest keys (hex) in
// the allowlist whose hash matches — the candidate set the caller verifies an api key
// against for create/build authorization. Expired rows (expires_unix>0 && <now) are
// excluded (treated as not allowlisted); they linger until PruneExpiredManifestKeys.
func (s *Store) AllowedManifestKeysByHash(ctx context.Context, hash string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key_enc FROM manifest_keys WHERE key_hash=? AND (expires_unix=0 OR expires_unix>?)`,
		hash, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var enc string
		if err := rows.Scan(&enc); err != nil {
			return nil, err
		}
		if mk, err := s.box.DecryptString(enc); err == nil {
			out = append(out, mk)
		}
	}
	return out, rows.Err()
}

// PruneExpiredManifestKeys deletes expired allowlist rows (expires_unix>0 && <now).
// Lazy cleanup called opportunistically by the reaper; expired keys are already
// excluded from authorization by AllowedManifestKeysByHash.
func (s *Store) PruneExpiredManifestKeys(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM manifest_keys WHERE expires_unix>0 AND expires_unix<?`, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// mustHash is HasManifestKey's helper; an invalid hex key yields a hash that
// matches nothing (callers re-validate the key elsewhere).
func mustHash(manifestKeyHex string) string {
	h, err := ManifestKeyHash(manifestKeyHex)
	if err != nil {
		return ""
	}
	return h
}
