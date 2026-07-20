// Package store persists sandbox/build records in a node-local sqlite database.
// modernc.org/sqlite is a pure-Go driver (CGO_ENABLED=0). AuthKey and
// ManifestKey are separate AES-256-GCM-encrypted fields; only AuthKey indexes
// participate in API ownership checks.
package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	_ "modernc.org/sqlite"
)

type Store struct {
	db        *sql.DB
	box       *secretbox.Box
	eventWake chan struct{}
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

const schema = `
CREATE TABLE IF NOT EXISTS sandboxes (
  id                   TEXT PRIMARY KEY,
  template_id          TEXT NOT NULL,
  state                TEXT NOT NULL,
  deadline_unix        INTEGER NOT NULL DEFAULT 0,
  run_dir              TEXT NOT NULL,
  base_dir             TEXT NOT NULL,
  run_id               TEXT NOT NULL DEFAULT '',
  envd_uds             TEXT NOT NULL DEFAULT '',
  ci_uds               TEXT NOT NULL DEFAULT '',
  floatingip           TEXT NOT NULL DEFAULT '',
  vswitch_port         TEXT NOT NULL DEFAULT '',
  inner_ip             TEXT NOT NULL DEFAULT '',
  port_mac             TEXT NOT NULL DEFAULT '',
  auth_key_hash        TEXT NOT NULL,
  auth_key_enc         TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_sandboxes_authhash ON sandboxes(auth_key_hash);

CREATE TABLE IF NOT EXISTS builds (
  build_id          TEXT PRIMARY KEY,
  template_id       TEXT NOT NULL,
  persist_id        TEXT NOT NULL DEFAULT '',
  auth_key_hash     TEXT NOT NULL,
  auth_key_enc      TEXT NOT NULL,
  manifest_key_hash TEXT NOT NULL,
  manifest_key_enc  TEXT NOT NULL,
  profile           TEXT NOT NULL,
  cpu_count         INTEGER NOT NULL,
  memory_mb         INTEGER NOT NULL,
  kind              TEXT NOT NULL,
  from_image        TEXT NOT NULL DEFAULT '',
  from_template     TEXT NOT NULL DEFAULT '',
  start_cmd         TEXT NOT NULL DEFAULT '',
  ready_cmd         TEXT NOT NULL DEFAULT '',
  steps_json        TEXT NOT NULL DEFAULT '[]',
  status            TEXT NOT NULL,
  reason            TEXT NOT NULL DEFAULT '',
  run_id            TEXT NOT NULL DEFAULT '',
  names_json        TEXT NOT NULL DEFAULT '[]',
  aliases_json      TEXT NOT NULL DEFAULT '[]',
	  created_unix      INTEGER NOT NULL,
	  registry_auth_enc TEXT NOT NULL DEFAULT '',
	  metadata_json     TEXT NOT NULL DEFAULT '{}',
	  builder_json      TEXT NOT NULL DEFAULT '{}'
	);
CREATE INDEX IF NOT EXISTS idx_builds_status ON builds(status);
CREATE INDEX IF NOT EXISTS idx_builds_authhash ON builds(auth_key_hash);

CREATE TABLE IF NOT EXISTS key_leases (
  group_name        TEXT NOT NULL,
  auth_key_hash     TEXT NOT NULL,
  auth_key_enc      TEXT NOT NULL,
  manifest_key_hash TEXT NOT NULL,
  manifest_key_enc  TEXT NOT NULL,
  label             TEXT NOT NULL DEFAULT '',
  created_unix      INTEGER NOT NULL,
  expires_unix      INTEGER NOT NULL DEFAULT 0,
  registry_auth_enc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_key_leases_auth ON key_leases(auth_key_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_key_leases_exact
  ON key_leases(group_name, auth_key_hash, manifest_key_hash);

-- Pre-cutover storage used only by the legacy cluster path. Phase 5 removes
-- this table together with that path at the atomic cutover.
CREATE TABLE IF NOT EXISTS manifest_keys (
  key_hash          TEXT NOT NULL,
  key_enc           TEXT NOT NULL,
  label             TEXT NOT NULL DEFAULT '',
  created_unix      INTEGER NOT NULL,
  expires_unix      INTEGER NOT NULL DEFAULT 0,
  registry_auth_enc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_manifest_keys_hash ON manifest_keys(key_hash);

CREATE TABLE IF NOT EXISTS cluster_identity (
  singleton       INTEGER PRIMARY KEY CHECK (singleton = 1),
  node_id         TEXT NOT NULL UNIQUE,
  enrollment_id   TEXT NOT NULL UNIQUE,
  node_epoch      BLOB NOT NULL CHECK (length(node_epoch) = 8),
  session_seq     BLOB NOT NULL CHECK (length(session_seq) = 8),
  boot_id         TEXT NOT NULL,
  data_endpoint   TEXT NOT NULL,
  updated_unix    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS node_workflows (
  object_kind          INTEGER NOT NULL,
  object_id            TEXT NOT NULL,
  group_name           TEXT NOT NULL,
  route_key            TEXT NOT NULL DEFAULT '',
  node_id              TEXT NOT NULL,
  node_epoch           BLOB NOT NULL CHECK (length(node_epoch) = 8),
  data_endpoint        TEXT NOT NULL,
  normalized_demand    BLOB NOT NULL,
  demand_digest        TEXT NOT NULL,
  dispatch_spec        BLOB NOT NULL,
  dispatch_spec_digest TEXT NOT NULL,
  provider_policy_version TEXT NOT NULL,
  opaque_binding       TEXT NOT NULL,
  binding_digest       TEXT NOT NULL,
  build_demand_json    TEXT NOT NULL DEFAULT '{}',
  admission_state      TEXT NOT NULL,
  result                TEXT NOT NULL,
  reason                TEXT NOT NULL DEFAULT '',
  reservation_token     TEXT NOT NULL DEFAULT '',
  queue_sequence        BLOB NOT NULL CHECK (length(queue_sequence) = 8),
  resource_claimed      INTEGER NOT NULL DEFAULT 0,
  object_state          TEXT NOT NULL DEFAULT '',
  event_seq             BLOB NOT NULL CHECK (length(event_seq) = 8),
  acked_event_seq       BLOB NOT NULL CHECK (length(acked_event_seq) = 8),
  latest_event_json     TEXT NOT NULL DEFAULT '',
  workflow_finalized    INTEGER NOT NULL DEFAULT 0,
  created_unix          INTEGER NOT NULL,
  updated_unix          INTEGER NOT NULL,
  PRIMARY KEY (object_kind, object_id)
);
CREATE INDEX IF NOT EXISTS idx_node_workflows_build_queue
  ON node_workflows(object_kind, admission_state, queue_sequence);
CREATE INDEX IF NOT EXISTS idx_node_workflows_outbox
  ON node_workflows(node_id, node_epoch, event_seq, acked_event_seq);
`

// Open opens (creating if needed) the sqlite store with the encryption box used
// for node secrets at rest. The file should be 0600.
func Open(path string, box *secretbox.Box) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	if err := ensureColumn(ctx, db, "builds", "builder_json", `TEXT NOT NULL DEFAULT '{}'`); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	if err := ensureColumn(ctx, db, "sandboxes", "run_id", `TEXT NOT NULL DEFAULT ''`); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	if err := ensureColumn(ctx, db, "builds", "run_id", `TEXT NOT NULL DEFAULT ''`); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	return &Store{db: db, box: box, eventWake: make(chan struct{}, 1)}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func ensureColumn(ctx context.Context, db *sql.DB, table, name, spec string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, pk int
		var col, typ string
		var def sql.NullString
		if err := rows.Scan(&cid, &col, &typ, &notNull, &def, &pk); err != nil {
			return err
		}
		if col == name {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+name+" "+spec)
	return err
}

func keyHash(name, keyHex string) (string, error) {
	raw, err := hex.DecodeString(keyHex)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != keyHex {
		return "", fmt.Errorf("store: %s must be a canonical 32-byte hexadecimal key", name)
	}
	return hex.EncodeToString(apikey.Fingerprint(raw)), nil
}

func AuthKeyHash(authKeyHex string) (string, error) {
	return keyHash("AuthKey", authKeyHex)
}

func ManifestKeyHash(manifestKeyHex string) (string, error) {
	return keyHash("ManifestKey", manifestKeyHex)
}

func (s *Store) encKeyField(name, keyHex string) (hash, enc string, err error) {
	if hash, err = keyHash(name, keyHex); err != nil {
		return "", "", err
	}
	enc, err = s.box.EncryptString(keyHex)
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

func mb(o types.BuildOptions) string {
	b, _ := json.Marshal(o)
	if len(b) == 0 || string(b) == "null" {
		return "{}"
	}
	return string(b)
}

func ub(s string) types.BuildOptions {
	var o types.BuildOptions
	_ = json.Unmarshal([]byte(s), &o)
	return o
}

// --- sandboxes ---

// Put upserts a Sandbox with separate encrypted AuthKey and ManifestKey fields.
func (s *Store) Put(ctx context.Context, sb *types.Sandbox) error {
	return s.putSandbox(ctx, s.db, sb)
}

func (s *Store) putSandbox(ctx context.Context, exec sqlExecutor, sb *types.Sandbox) error {
	authHash, authEnc, err := s.encKeyField("AuthKey", sb.AuthKey)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}
	manifestHash, manifestEnc, err := s.encKeyField("ManifestKey", sb.ManifestKey)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}
	_, err = exec.ExecContext(ctx, `
INSERT INTO sandboxes (id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,
  floatingip,vswitch_port,inner_ip,port_mac,auth_key_hash,auth_key_enc,manifest_key_hash,manifest_key_enc,snapshot_ref,
  envd_access_token,traffic_access_token,metadata_json,env_json,created_unix)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET
  template_id=excluded.template_id, state=excluded.state, deadline_unix=excluded.deadline_unix,
  run_dir=excluded.run_dir, base_dir=excluded.base_dir, run_id=excluded.run_id, envd_uds=excluded.envd_uds,
  ci_uds=excluded.ci_uds, floatingip=excluded.floatingip, vswitch_port=excluded.vswitch_port,
  inner_ip=excluded.inner_ip, port_mac=excluded.port_mac,
  auth_key_hash=excluded.auth_key_hash, auth_key_enc=excluded.auth_key_enc,
  manifest_key_hash=excluded.manifest_key_hash, manifest_key_enc=excluded.manifest_key_enc,
  snapshot_ref=excluded.snapshot_ref, envd_access_token=excluded.envd_access_token,
  traffic_access_token=excluded.traffic_access_token,
  metadata_json=excluded.metadata_json, env_json=excluded.env_json`,
		sb.ID, sb.TemplateID, string(sb.State), sb.DeadlineUnix, sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS,
		sb.CiUDS, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC, authHash, authEnc, manifestHash, manifestEnc, sb.SnapshotRef,
		sb.EnvdAccessToken, sb.TrafficAccessToken, mj(sb.Metadata), mj(sb.Env), sb.CreatedUnix)
	if err != nil {
		return fmt.Errorf("store: put %s: %w", sb.ID, err)
	}
	return nil
}

var cols = `id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,floatingip,
  vswitch_port,inner_ip,port_mac,auth_key_hash,auth_key_enc,manifest_key_hash,manifest_key_enc,snapshot_ref,
  envd_access_token,traffic_access_token,metadata_json,env_json,created_unix`

func (s *Store) scan(row interface{ Scan(...any) error }) (*types.Sandbox, error) {
	var sb types.Sandbox
	var st, meta, env, authHash, authEnc, manifestHash, manifestEnc string
	if err := row.Scan(&sb.ID, &sb.TemplateID, &st, &sb.DeadlineUnix, &sb.RunDir, &sb.BaseDir,
		&sb.RunID, &sb.EnvdUDS, &sb.CiUDS, &sb.FloatingIP, &sb.VswitchPort, &sb.InnerIP, &sb.PortMAC,
		&authHash, &authEnc, &manifestHash, &manifestEnc, &sb.SnapshotRef, &sb.EnvdAccessToken, &sb.TrafficAccessToken,
		&meta, &env, &sb.CreatedUnix); err != nil {
		return nil, err
	}
	authKey, err := s.box.DecryptString(authEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt AuthKey for %s: %w", sb.ID, err)
	}
	manifestKey, err := s.box.DecryptString(manifestEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt ManifestKey for %s: %w", sb.ID, err)
	}
	sb.AuthKey, sb.ManifestKey = authKey, manifestKey
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
// the result to an auth_key_hash (a fast, non-unique pre-filter — the caller
// still verifies the api key per row); "" returns all (internal callers).
func (s *Store) List(ctx context.Context, state, ownerHash string, limit int, cursor string) ([]*types.Sandbox, string, error) {
	if limit <= 0 {
		limit = 100 // default page
	}
	if limit > 1000 {
		limit = 1000 // clamp to the max page (not down to the default)
	}
	q := `SELECT ` + cols + ` FROM sandboxes`
	var args []any
	var conds []string
	if state != "" {
		conds = append(conds, "state=?")
		args = append(args, state)
	}
	if ownerHash != "" {
		conds = append(conds, "auth_key_hash=?")
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

// RangeByState calls fn for every sandbox in state, streaming rows (bounded
// memory, no limit/cursor — the reaper / reconcile / route snapshot all need the
// complete set, which the old paginated ListByState silently truncated at 1000).
// fn MUST be read-only with respect to the store: collect what to act on and act
// after Range returns (the read cursor is open for the whole scan; writing to the
// same table mid-scan is only safe under WAL + a multi-conn pool, so the
// read-only contract keeps callers correct regardless). A non-nil fn error stops
// iteration and is returned.
func (s *Store) RangeByState(ctx context.Context, state types.State, fn func(*types.Sandbox) error) error {
	rows, err := s.db.QueryContext(ctx, `SELECT `+cols+` FROM sandboxes WHERE state=? ORDER BY id ASC`, string(state))
	if err != nil {
		return fmt.Errorf("store: range by state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		sb, err := s.scan(rows)
		if err != nil {
			return err
		}
		if err := fn(sb); err != nil {
			return err
		}
	}
	return rows.Err()
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

var buildCols = `build_id,template_id,persist_id,auth_key_hash,auth_key_enc,manifest_key_hash,manifest_key_enc,profile,cpu_count,memory_mb,kind,
  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,metadata_json,builder_json`

func (s *Store) scanBuild(row interface{ Scan(...any) error }) (*types.Build, error) {
	var b types.Build
	var profile, kind, status, names, aliases, authHash, authEnc, manifestHash, manifestEnc, raEnc, steps, meta, builder string
	if err := row.Scan(&b.BuildID, &b.TemplateID, &b.PersistID, &authHash, &authEnc, &manifestHash, &manifestEnc, &profile, &b.CPUCount, &b.MemoryMB, &kind,
		&b.FromImage, &b.FromTemplate, &b.StartCmd, &b.ReadyCmd, &steps, &status, &b.Reason, &b.RunID, &names, &aliases, &b.CreatedUnix, &raEnc, &meta, &builder); err != nil {
		return nil, err
	}
	b.Metadata = uj(meta)
	b.Builder = ub(builder)
	if steps != "" && steps != "[]" {
		if err := json.Unmarshal([]byte(steps), &b.Steps); err != nil {
			return nil, fmt.Errorf("store: build %s steps: %w", b.BuildID, err)
		}
	}
	authKey, err := s.box.DecryptString(authEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt AuthKey for build %s: %w", b.BuildID, err)
	}
	manifestKey, err := s.box.DecryptString(manifestEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt ManifestKey for build %s: %w", b.BuildID, err)
	}
	b.AuthKey, b.ManifestKey = authKey, manifestKey
	if raEnc != "" {
		if b.RegistryAuth, err = s.box.DecryptString(raEnc); err != nil {
			return nil, fmt.Errorf("store: decrypt registry auth for build %s: %w", b.BuildID, err)
		}
	}
	b.Profile, b.Kind, b.Status = types.Profile(profile), types.Kind(kind), types.BuildState(status)
	b.Names, b.Aliases = ujs(names), ujs(aliases)
	return &b, nil
}

// PutBuild upserts a Build with separate encrypted key domains and resource ceiling.
func (s *Store) PutBuild(ctx context.Context, b *types.Build) error {
	return s.putBuild(ctx, s.db, b)
}

func (s *Store) putBuild(ctx context.Context, exec sqlExecutor, b *types.Build) error {
	authHash, authEnc, err := s.encKeyField("AuthKey", b.AuthKey)
	if err != nil {
		return fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	manifestHash, manifestEnc, err := s.encKeyField("ManifestKey", b.ManifestKey)
	if err != nil {
		return fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	if b.CPUCount <= 0 || b.MemoryMB <= 0 {
		return fmt.Errorf("store: put build %s: positive CPU/memory ceiling required", b.BuildID)
	}
	var raEnc string
	if b.RegistryAuth != "" {
		if raEnc, err = s.box.EncryptString(b.RegistryAuth); err != nil {
			return fmt.Errorf("store: put build %s: encrypt registry auth: %w", b.BuildID, err)
		}
	}
	stepsJSON := "[]"
	if len(b.Steps) > 0 {
		sj, jerr := json.Marshal(b.Steps)
		if jerr != nil {
			return fmt.Errorf("store: put build %s: steps: %w", b.BuildID, jerr)
		}
		stepsJSON = string(sj)
	}
	_, err = exec.ExecContext(ctx, `
	INSERT INTO builds (build_id,template_id,persist_id,auth_key_hash,auth_key_enc,manifest_key_hash,manifest_key_enc,profile,cpu_count,memory_mb,kind,
	  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,metadata_json,builder_json)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(build_id) DO UPDATE SET
	  template_id=excluded.template_id, persist_id=excluded.persist_id,
	  auth_key_hash=excluded.auth_key_hash, auth_key_enc=excluded.auth_key_enc,
	  manifest_key_hash=excluded.manifest_key_hash, manifest_key_enc=excluded.manifest_key_enc,
	  profile=excluded.profile, cpu_count=excluded.cpu_count, memory_mb=excluded.memory_mb,
	  kind=excluded.kind, from_image=excluded.from_image,
	  from_template=excluded.from_template, start_cmd=excluded.start_cmd,
	  ready_cmd=excluded.ready_cmd, steps_json=excluded.steps_json,
	  status=excluded.status, reason=excluded.reason, run_id=excluded.run_id,
	  names_json=excluded.names_json, aliases_json=excluded.aliases_json,
	  registry_auth_enc=excluded.registry_auth_enc, metadata_json=excluded.metadata_json,
	  builder_json=excluded.builder_json`,
		b.BuildID, b.TemplateID, b.PersistID, authHash, authEnc, manifestHash, manifestEnc, string(b.Profile), b.CPUCount, b.MemoryMB, string(b.Kind),
		b.FromImage, b.FromTemplate, b.StartCmd, b.ReadyCmd, stepsJSON, string(b.Status), b.Reason, b.RunID, mjs(b.Names), mjs(b.Aliases), b.CreatedUnix, raEnc, mj(b.Metadata), mb(b.Builder))
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

// GetBuildByTemplateID looks a build up by its (transient) template id — the
// handle the files endpoint receives (GET /templates/{tid}/files/{hash}), which
// carries no build id. The transient template id is a per-build uuidv7, so this
// is unique. Returns nil when unknown.
func (s *Store) GetBuildByTemplateID(ctx context.Context, templateID string) (*types.Build, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE template_id=?`, templateID)
	b, err := s.scanBuild(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get build by template %s: %w", templateID, err)
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

// SetBuildRunID records the systemd runner assigned to a build without
// rewriting status or other fields that may have changed since admission.
func (s *Store) SetBuildRunID(ctx context.Context, buildID, runID string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE builds SET run_id=? WHERE build_id=?`, runID, buildID)
	if err != nil {
		return fmt.Errorf("store: set build %s run id: %w", buildID, err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("store: set build %s run id: build not found", buildID)
	}
	return nil
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
