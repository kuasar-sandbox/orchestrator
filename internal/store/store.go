// Package store persists sandbox/build records in a node-local sqlite database.
// modernc.org/sqlite is a pure-Go driver (CGO_ENABLED=0). Tenant API, manifest,
// and sandbox service credentials are stored AES-256-GCM-encrypted (secretbox).
// Tenant-root complete SHA-256 fingerprints are indexed; the API-secret
// fingerprint prefix embedded in an API key is only a candidate selector, and
// callers must still verify the MAC.
package store

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	_ "modernc.org/sqlite"
)

type Store struct {
	db  *sql.DB
	box *secretbox.Box
}

const schema = `
CREATE TABLE IF NOT EXISTS sandboxes (
  id                   TEXT PRIMARY KEY,
  profile              TEXT NOT NULL,
  cluster_group        TEXT NOT NULL DEFAULT '',
  cluster_route_key    TEXT NOT NULL DEFAULT '',
  stable_id           TEXT NOT NULL DEFAULT '',
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
  api_secret_hash      TEXT NOT NULL,
  api_secret_enc       TEXT NOT NULL,
  manifest_key_hash    TEXT NOT NULL,
  manifest_key_enc     TEXT NOT NULL,
  resume_source_kind   TEXT NOT NULL DEFAULT '',
  resume_source_ref    TEXT NOT NULL DEFAULT '',
  auto_pause_memory    INTEGER NOT NULL DEFAULT 1,
  launch_mode          TEXT NOT NULL DEFAULT '',
  service_secret_enc       TEXT NOT NULL,
  envd_access_token_enc    TEXT NOT NULL,
  traffic_access_token_enc TEXT NOT NULL,
  forward_access_token_enc TEXT NOT NULL,
  metadata_json        TEXT NOT NULL DEFAULT '{}',
  env_json             TEXT NOT NULL DEFAULT '{}',
  created_unix         INTEGER NOT NULL,
  dead_unix            INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sandboxes_state ON sandboxes(state);
CREATE INDEX IF NOT EXISTS idx_sandboxes_ashash ON sandboxes(api_secret_hash);
CREATE INDEX IF NOT EXISTS idx_sandboxes_ascandidate ON sandboxes(substr(api_secret_hash,1,24));
CREATE INDEX IF NOT EXISTS idx_sandboxes_mkhash ON sandboxes(manifest_key_hash);

CREATE TABLE IF NOT EXISTS builds (
  build_id          TEXT PRIMARY KEY,
  template_id       TEXT NOT NULL,
  persist_id        TEXT NOT NULL DEFAULT '',
  api_secret_hash   TEXT NOT NULL,
  api_secret_enc    TEXT NOT NULL,
  manifest_key_hash TEXT NOT NULL,
  manifest_key_enc  TEXT NOT NULL,
  profile           TEXT NOT NULL,
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
  registration_image_repo TEXT NOT NULL DEFAULT '',
  registration_registry_auth_enc TEXT NOT NULL DEFAULT '',
  registration_mmds_routes_digest TEXT NOT NULL DEFAULT '',
  registration_mmds_values_digest TEXT NOT NULL DEFAULT '',
  registration_request_digest TEXT NOT NULL DEFAULT '',
  cluster_group       TEXT NOT NULL DEFAULT '',
  resources_cpu      INTEGER NOT NULL,
  resources_memory   INTEGER NOT NULL,
  resources_storage  INTEGER NOT NULL DEFAULT 0,
  metadata_json     TEXT NOT NULL DEFAULT '{}',
  builder_json      TEXT NOT NULL DEFAULT '{}',
  instance_config_enc TEXT NOT NULL,
  waiting_unix      INTEGER NOT NULL DEFAULT 0,
  waiting_sequence  INTEGER NOT NULL DEFAULT 0,
  execution_claimed INTEGER NOT NULL DEFAULT 0,
  execution_claimed_unix INTEGER NOT NULL DEFAULT 0,
  enforcement_status TEXT NOT NULL DEFAULT '',
  phase              TEXT NOT NULL DEFAULT '',
  phase_sandbox_id   TEXT NOT NULL DEFAULT '',
  runtime_vswitch_port TEXT NOT NULL DEFAULT '',
  runtime_floating_ip TEXT NOT NULL DEFAULT '',
  runtime_port_mac TEXT NOT NULL DEFAULT '',
  runtime_envd_access_token_enc TEXT NOT NULL DEFAULT '',
  runtime_prepare_json TEXT NOT NULL DEFAULT '',
  execution_result_json TEXT NOT NULL DEFAULT '',
  finished_unix INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_builds_status ON builds(status);
CREATE INDEX IF NOT EXISTS idx_builds_ashash ON builds(api_secret_hash);
CREATE INDEX IF NOT EXISTS idx_builds_ascandidate ON builds(substr(api_secret_hash,1,24));
CREATE INDEX IF NOT EXISTS idx_builds_mkhash ON builds(manifest_key_hash);

CREATE TABLE IF NOT EXISTS sandbox_mmds_route_secret_values (
  sandbox_id        TEXT PRIMARY KEY,
  routes_digest     TEXT NOT NULL,
  revision          INTEGER NOT NULL,
  ciphertext        TEXT NOT NULL,
  updated_unix      INTEGER NOT NULL,
  FOREIGN KEY (sandbox_id) REFERENCES sandboxes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS build_mmds_route_secret_values (
  build_id          TEXT PRIMARY KEY,
  routes_digest     TEXT NOT NULL,
  revision          INTEGER NOT NULL,
  ciphertext        TEXT NOT NULL,
  updated_unix      INTEGER NOT NULL,
  FOREIGN KEY (build_id) REFERENCES builds(build_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS manifest_keys (
  api_secret_hash   TEXT PRIMARY KEY,
  api_secret_enc    TEXT NOT NULL,
  manifest_key_hash TEXT NOT NULL,
  manifest_key_enc  TEXT NOT NULL,
  label             TEXT NOT NULL DEFAULT '',
  created_unix      INTEGER NOT NULL,
  expires_unix      INTEGER NOT NULL DEFAULT 0,
  registry_auth_enc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_manifest_keys_ascandidate ON manifest_keys(substr(api_secret_hash,1,24));
CREATE INDEX IF NOT EXISTS idx_manifest_keys_mkhash ON manifest_keys(manifest_key_hash);
`

// Open opens (creating if needed) the sqlite store with the encryption box used
// for tenant and sandbox credentials at rest. The file should be 0600.
func Open(path string, box *secretbox.Box) (*Store, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("store: resolve %s: %w", path, err)
	}
	query := url.Values{
		"_txlock": {"immediate"},
		"_pragma": {
			"busy_timeout(5000)",
			"journal_mode(WAL)",
			"foreign_keys(1)",
		},
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(absPath), RawQuery: query.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	// Build target/config persistence is an intentional pre-release schema cut.
	// Refuse a pre-#300 builds table before running any of the repository's
	// independent additive migrations: there is no safe plaintext backfill for
	// instance_config_enc and no legacy Build reader/writer path.
	if err := requireColumn(ctx, db, "builds", "instance_config_enc"); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureColumn(ctx, db, "builds", "runtime_prepare_json", "TEXT NOT NULL DEFAULT ''"); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureColumn(ctx, db, "builds", "registration_request_digest", "TEXT NOT NULL DEFAULT ''"); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureColumn(ctx, db, "sandboxes", "dead_unix", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensureColumn(ctx, db, "builds", "finished_unix", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		db.Close()
		return nil, err
	}
	// Existing terminal history receives a fresh retention window on the one-way
	// additive schema upgrade. New terminal transitions set these timestamps in
	// the same UPDATE that publishes the terminal state.
	for _, statement := range []string{
		`UPDATE sandboxes SET dead_unix=unixepoch() WHERE state='dead' AND dead_unix=0`,
		`UPDATE builds SET finished_unix=unixepoch() WHERE status IN ('ready','error') AND finished_unix=0`,
		`CREATE INDEX IF NOT EXISTS idx_sandboxes_dead_retention ON sandboxes(state,dead_unix)`,
		`CREATE INDEX IF NOT EXISTS idx_builds_terminal_retention ON builds(status,finished_unix)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: initialize terminal retention: %w", err)
		}
	}
	return &Store{db: db, box: box}, nil
}

func requireColumn(ctx context.Context, db *sql.DB, table, column string) error {
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pragma_table_info(?) WHERE name=?", table, column,
	).Scan(&count); err != nil {
		return fmt.Errorf("store: inspect %s schema: %w", table, err)
	}
	if count != 1 {
		return fmt.Errorf("store: incompatible pre-release %s schema: missing %s; recreate the database", table, column)
	}
	return nil
}

// ensureColumn performs the repository's additive SQLite compatibility
// upgrade. CREATE TABLE IF NOT EXISTS cannot add fields to an existing node DB.
func ensureColumn(ctx context.Context, db *sql.DB, table, column, definition string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return fmt.Errorf("store: inspect %s schema: %w", table, err)
	}
	found := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: inspect %s columns: %w", table, err)
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("store: inspect %s columns: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: close %s schema rows: %w", table, err)
	}
	if found {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+definition); err != nil {
		return fmt.Errorf("store: add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

const (
	secretHexLen              = sha256.Size * 2
	apiSecretCandidateHashLen = 12 * 2
)

var (
	hexSecretRE        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	hexCandidateHashRE = regexp.MustCompile(`^[0-9a-f]{24}$`)

	// ErrSandboxExists means an insert-only sandbox write found an existing
	// record with the same ID. The existing row is retained unchanged.
	ErrSandboxExists = errors.New("store: sandbox already exists")

	// ErrKeyPairConflict means an API-secret fingerprint is already bound to
	// different API-secret or manifest-key material. The existing row is retained.
	ErrKeyPairConflict = errors.New("store: API secret fingerprint is already bound to a different key pair")
)

// KeyPair is the tenant credential pair copied into newly created durable
// business records. APISecret authenticates API requests; ManifestKey protects
// manifest content. Neither field is a fingerprint.
type KeyPair struct {
	APISecret   string
	ManifestKey string
}

func secretHash(name, secretHex string) (string, error) {
	if len(secretHex) != secretHexLen || !hexSecretRE.MatchString(secretHex) {
		return "", fmt.Errorf("store: %s must be 64 lowercase hex characters", name)
	}
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		return "", fmt.Errorf("store: %s not hex: %w", name, err)
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}

// APISecretHash returns the complete SHA-256 fingerprint of a hex API secret.
func APISecretHash(apiSecretHex string) (string, error) {
	return secretHash("API secret", apiSecretHex)
}

// ManifestKeyHash returns the complete SHA-256 fingerprint of a hex manifest key.
func ManifestKeyHash(manifestKeyHex string) (string, error) {
	return secretHash("manifest key", manifestKeyHex)
}

func keyPairHashes(pair KeyPair) (apiHash, manifestHash string, err error) {
	apiHash, err = APISecretHash(pair.APISecret)
	if err != nil {
		return "", "", err
	}
	manifestHash, err = ManifestKeyHash(pair.ManifestKey)
	if err != nil {
		return "", "", err
	}
	return apiHash, manifestHash, nil
}

// encSecret returns the complete fingerprint and ciphertext for a hex secret.
func (s *Store) encSecret(name, secretHex string) (hash, enc string, err error) {
	if hash, err = secretHash(name, secretHex); err != nil {
		return "", "", err
	}
	enc, err = s.box.EncryptString(secretHex)
	return hash, enc, err
}

func equalSecretHex(a, b string) bool {
	aRaw, aErr := hex.DecodeString(a)
	bRaw, bErr := hex.DecodeString(b)
	return aErr == nil && bErr == nil && len(aRaw) == sha256.Size && len(bRaw) == sha256.Size && hmac.Equal(aRaw, bRaw)
}

func equalKeyPair(a, b KeyPair) bool {
	apiEqual := equalSecretHex(a.APISecret, b.APISecret)
	manifestEqual := equalSecretHex(a.ManifestKey, b.ManifestKey)
	return apiEqual && manifestEqual
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

type buildInstanceConfig struct {
	Env                map[string]string `json:"env,omitempty"`
	Secure             bool              `json:"secure,omitempty"`
	ServiceSecret      string            `json:"service_secret,omitempty"`
	EnvdAccessToken    string            `json:"envd_access_token,omitempty"`
	TrafficAccessToken string            `json:"traffic_access_token,omitempty"`
}

// --- sandboxes ---

const sandboxInsertSQL = `
	INSERT INTO sandboxes (id,profile,cluster_group,cluster_route_key,stable_id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,
	  floatingip,vswitch_port,inner_ip,port_mac,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,
	  resume_source_kind,resume_source_ref,auto_pause_memory,launch_mode,
	  service_secret_enc,envd_access_token_enc,traffic_access_token_enc,forward_access_token_enc,metadata_json,env_json,created_unix,dead_unix)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

const sandboxUpsertSQL = sandboxInsertSQL + `
ON CONFLICT(id) DO UPDATE SET
  template_id=excluded.template_id, state=excluded.state, deadline_unix=excluded.deadline_unix,
  run_dir=excluded.run_dir, base_dir=excluded.base_dir, run_id=excluded.run_id, envd_uds=excluded.envd_uds,
  ci_uds=excluded.ci_uds, floatingip=excluded.floatingip, vswitch_port=excluded.vswitch_port,
  inner_ip=excluded.inner_ip, port_mac=excluded.port_mac,
  resume_source_kind=excluded.resume_source_kind, resume_source_ref=excluded.resume_source_ref,
  auto_pause_memory=excluded.auto_pause_memory, launch_mode=excluded.launch_mode,
  metadata_json=excluded.metadata_json, env_json=excluded.env_json,
  dead_unix=excluded.dead_unix`

const sandboxInsertOnlySQL = sandboxInsertSQL + `
ON CONFLICT(id) DO NOTHING`

func (s *Store) prepareSandboxInsert(sb *types.Sandbox) ([]any, error) {
	if sb == nil {
		return nil, errors.New("sandbox is required")
	}
	if !types.ValidLocalSandboxID(sb.ID) {
		return nil, errors.New("invalid sandbox id")
	}
	if sb.State == types.StateDead && sb.DeadUnix == 0 {
		sb.DeadUnix = time.Now().Unix()
	}
	if err := validateSandboxLifecycle(sb); err != nil {
		return nil, err
	}
	clusterGroup, clusterRouteKey, err := sandboxIdentityColumns(sb)
	if err != nil {
		return nil, err
	}
	apiHash, apiEnc, err := s.encSecret("API secret", sb.APISecret)
	if err != nil {
		return nil, err
	}
	manifestHash, manifestEnc, err := s.encSecret("manifest key", sb.ManifestKey)
	if err != nil {
		return nil, err
	}
	if err := validateSandboxServiceCredentials(sb); err != nil {
		return nil, err
	}
	serviceSecretEnc, err := s.box.EncryptString(sb.ServiceSecret)
	if err != nil {
		return nil, fmt.Errorf("encrypt service secret: %w", err)
	}
	envdAccessTokenEnc, err := s.box.EncryptString(sb.EnvdAccessToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt envd access token: %w", err)
	}
	trafficAccessTokenEnc, err := s.box.EncryptString(sb.TrafficAccessToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt traffic access token: %w", err)
	}
	forwardAccessTokenEnc, err := s.box.EncryptString(sb.ForwardAccessToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt forward access token: %w", err)
	}
	return []any{
		sb.ID, string(sb.Profile), clusterGroup, clusterRouteKey, sb.StableIDValue,
		sb.TemplateID, string(sb.State), sb.DeadlineUnix, sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS,
		sb.CiUDS, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC, apiHash, apiEnc, manifestHash, manifestEnc,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.AutoPauseMemory, string(sb.LaunchMode),
		serviceSecretEnc, envdAccessTokenEnc, trafficAccessTokenEnc, forwardAccessTokenEnc,
		mj(sb.Metadata), mj(sb.Env), sb.CreatedUnix, sb.DeadUnix,
	}, nil
}

func validateSandboxLifecycle(sb *types.Sandbox) error {
	if sb == nil {
		return errors.New("sandbox is required")
	}
	if !sb.ResumeSource.Empty() && !sb.ResumeSource.Valid() {
		return fmt.Errorf("invalid resume source kind=%q ref=%q", sb.ResumeSource.Kind, sb.ResumeSource.Ref)
	}
	switch sb.State {
	case types.StatePaused:
		if !sb.ResumeSource.Valid() {
			return errors.New("paused sandbox requires a resume source")
		}
		if sb.LaunchMode != "" {
			return errors.New("paused sandbox must not have a launch mode")
		}
	case types.StateStarting:
		if sb.ResumeSource.Valid() {
			if sb.LaunchMode != types.LaunchCold && sb.LaunchMode != types.LaunchMemory {
				return errors.New("resuming sandbox requires cold or memory launch mode")
			}
			if sb.LaunchMode == types.LaunchMemory && sb.ResumeSource.Kind != types.ResumeSourceSnapshot {
				return errors.New("memory launch requires a snapshot resume source")
			}
			break
		}
		tmpl, err := types.ParseTemplateID(sb.TemplateID)
		if err != nil {
			return fmt.Errorf("fresh starting sandbox template: %w", err)
		}
		want, err := types.LaunchModeForTemplate(tmpl.Kind)
		if err != nil {
			return err
		}
		if sb.LaunchMode != want {
			return fmt.Errorf("fresh starting sandbox launch mode %q does not match template kind %q", sb.LaunchMode, tmpl.Kind)
		}
	case types.StateRunning, types.StateDeleting:
		if sb.LaunchMode != "" {
			return fmt.Errorf("%s sandbox must not have a launch mode", sb.State)
		}
	case types.StateDead:
		if sb.DeadUnix <= 0 {
			return errors.New("dead sandbox requires a terminal timestamp")
		}
		if sb.LaunchMode != "" {
			return errors.New("dead sandbox must not have a launch mode")
		}
		if sb.RunID != "" || sb.VswitchPort != "" || sb.FloatingIP != "" || sb.InnerIP != "" || sb.PortMAC != "" ||
			sb.RunDir != "" || sb.BaseDir != "" || sb.EnvdUDS != "" || sb.CiUDS != "" || !sb.ResumeSource.Empty() {
			return errors.New("dead sandbox must not retain runtime, path, network, or artifact ownership")
		}
	default:
		return fmt.Errorf("invalid sandbox state %q", sb.State)
	}
	if sb.State != types.StateDead && sb.DeadUnix != 0 {
		return fmt.Errorf("%s sandbox must not have a terminal timestamp", sb.State)
	}
	return nil
}

func sandboxWriteError(operation string, sb *types.Sandbox, err error) error {
	if sb == nil {
		return fmt.Errorf("store: %s: %w", operation, err)
	}
	return fmt.Errorf("store: %s %s: %w", operation, sb.ID, err)
}

// Put upserts a sandbox record. Profile, cluster identity, stable identity,
// tenant credential pair, and sandbox service credentials are written only by
// the initial insert; later lifecycle updates cannot rebind an existing sandbox.
func (s *Store) Put(ctx context.Context, sb *types.Sandbox) error {
	args, err := s.prepareSandboxInsert(sb)
	if err != nil {
		return sandboxWriteError("put", sb, err)
	}
	_, err = s.db.ExecContext(ctx, sandboxUpsertSQL, args...)
	if err != nil {
		return sandboxWriteError("put", sb, err)
	}
	return nil
}

// InsertSandbox writes a new sandbox record and returns ErrSandboxExists when
// the ID is already present. It never changes an existing row.
func (s *Store) InsertSandbox(ctx context.Context, sb *types.Sandbox) error {
	args, err := s.prepareSandboxInsert(sb)
	if err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	result, err := s.db.ExecContext(ctx, sandboxInsertOnlySQL, args...)
	if err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	if inserted == 0 {
		return sandboxWriteError("insert sandbox", sb, ErrSandboxExists)
	}
	return nil
}

// StartingResources is the host network ownership acquired by one launch before
// a runner is assigned. It is persisted separately from the immutable sandbox
// identity so concurrent deadline/lifecycle updates cannot be overwritten by a
// stale whole-row write.
type StartingResources struct {
	FloatingIP  string
	VswitchPort string
	InnerIP     string
	PortMAC     string
}

func sandboxUpdateChanged(operation, id string, result sql.Result) (bool, error) {
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: %s sandbox %s rows: %w", operation, id, err)
	}
	return n == 1, nil
}

// BeginResume atomically accepts a fully cleaned paused sandbox and restores
// the canonical runtime paths for its next incarnation. Runner, network, and
// RunDir ownership must already have been fenced, released, and exact-cleared;
// this CAS must never discard identities still needed by the paused finalizer.
func (s *Store) BeginResume(
	ctx context.Context,
	id string,
	deadlineUnix int64,
	mode types.LaunchMode,
	runDir, envdUDS, ciUDS string,
) (bool, error) {
	if mode != types.LaunchCold && mode != types.LaunchMemory {
		return false, fmt.Errorf("store: begin resume sandbox %s: invalid launch mode %q", id, mode)
	}
	if runDir == "" {
		return false, fmt.Errorf("store: begin resume sandbox %s: empty run dir", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, deadline_unix=?, launch_mode=?, run_dir=?, envd_uds=?, ci_uds=?
		 WHERE id=? AND state=?
		   AND run_id='' AND floatingip='' AND vswitch_port='' AND inner_ip='' AND port_mac=''
		   AND run_dir='' AND envd_uds='' AND ci_uds=''
		   AND resume_source_ref<>''
		   AND ((?=? AND resume_source_kind=?) OR
		        (?=? AND resume_source_kind IN (?,?)))`,
		string(types.StateStarting), deadlineUnix, string(mode), runDir, envdUDS, ciUDS,
		id, string(types.StatePaused),
		string(mode), string(types.LaunchMemory), string(types.ResumeSourceSnapshot),
		string(mode), string(types.LaunchCold), string(types.ResumeSourceSnapshot), string(types.ResumeSourceSandbox))
	if err != nil {
		return false, fmt.Errorf("store: begin resume sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("begin resume", id, result)
}

// SetStartingResources transfers freshly attached network ownership to the
// durable starting row only before any runner has been bound.
func (s *Store) SetStartingResources(ctx context.Context, id string, resources StartingResources) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET floatingip=?, vswitch_port=?, inner_ip=?, port_mac=?
		 WHERE id=? AND state=? AND run_id=''`,
		resources.FloatingIP, resources.VswitchPort, resources.InnerIP, resources.PortMAC,
		id, string(types.StateStarting))
	if err != nil {
		return false, fmt.Errorf("store: set starting resources sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("set starting resources", id, result)
}

// SetStartingResourcesForRun transfers attached network ownership only to the
// exact assigned starting runner. Restore launches bind a runner before task-
// local artifact preparation, so the older unassigned CAS is intentionally too
// weak for their final host preparation.
func (s *Store) SetStartingResourcesForRun(ctx context.Context, id, runID string, resources StartingResources) (bool, error) {
	if runID == "" {
		return false, fmt.Errorf("store: set starting resources sandbox %s: empty run id", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET floatingip=?, vswitch_port=?, inner_ip=?, port_mac=?
		 WHERE id=? AND state=? AND run_id=? AND vswitch_port=''`,
		resources.FloatingIP, resources.VswitchPort, resources.InnerIP, resources.PortMAC,
		id, string(types.StateStarting), runID)
	if err != nil {
		return false, fmt.Errorf("store: set exact-run starting resources sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("set exact-run starting resources", id, result)
}

// StartingTaskIdentity reads only the non-secret identity used to authenticate
// a sandbox task bootstrap. It deliberately does not select or decrypt tenant
// credentials.
func (s *Store) StartingTaskIdentity(ctx context.Context, id, runID string) (runDir string, found bool, err error) {
	if id == "" || runID == "" {
		return "", false, nil
	}
	err = s.db.QueryRowContext(ctx, `
		SELECT run_dir FROM sandboxes
		 WHERE id=? AND state=? AND run_id=?`, id, string(types.StateStarting), runID).Scan(&runDir)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: read starting task identity for sandbox %s: %w", id, err)
	}
	return runDir, true, nil
}

// BindStartingRunner is the runner-pool commit fence. It succeeds exactly once
// while the accepted launch still owns an unassigned starting row.
func (s *Store) BindStartingRunner(ctx context.Context, id, runID string) (bool, error) {
	if runID == "" {
		return false, fmt.Errorf("store: bind starting runner sandbox %s: empty run id", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET run_id=?
		 WHERE id=? AND state=? AND run_id=''`,
		runID, id, string(types.StateStarting))
	if err != nil {
		return false, fmt.Errorf("store: bind starting runner sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("bind starting runner", id, result)
}

// ResetStartingOwnershipForRecovery releases the persisted runner/network
// incarnation of an interrupted resume while preserving both the accepted
// starting state and its durable launch mode. A subsequent conductor restart
// can therefore retry the same cold or memory decision without re-resolving it
// from ResumeAuto.
func (s *Store) ResetStartingOwnershipForRecovery(ctx context.Context, id, expectedRunID string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET run_id='', floatingip='', vswitch_port='', inner_ip='', port_mac=''
		 WHERE id=? AND state=? AND run_id=?
		   AND resume_source_ref<>'' AND launch_mode IN (?,?)`,
		id, string(types.StateStarting), expectedRunID,
		string(types.LaunchCold), string(types.LaunchMemory))
	if err != nil {
		return false, fmt.Errorf("store: reset starting recovery ownership sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("reset starting recovery ownership", id, result)
}

// CommitStartingRunning commits a successfully initialized sandbox only while
// the exact runner bound by the assignment callback still owns it.
func (s *Store) CommitStartingRunning(ctx context.Context, id, runID string) (bool, error) {
	if runID == "" {
		return false, fmt.Errorf("store: commit starting running sandbox %s: empty run id", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET state=?, launch_mode=''
		 WHERE id=? AND state=? AND run_id=?`,
		string(types.StateRunning), id, string(types.StateStarting), runID)
	if err != nil {
		return false, fmt.Errorf("store: commit starting running sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("commit starting running", id, result)
}

// CommitRunningPaused publishes a completed artifact only while the exact
// runner that produced it still owns a running row. State and resume source are
// one atomic update so readers can never observe a partially committed pause.
func (s *Store) CommitRunningPaused(ctx context.Context, id, runID string, source types.ResumeSource) (bool, error) {
	if !source.Valid() {
		return false, fmt.Errorf("store: commit running paused sandbox %s: invalid resume source", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, resume_source_kind=?, resume_source_ref=?, launch_mode=''
		 WHERE id=? AND state=? AND run_id=?`,
		string(types.StatePaused), string(source.Kind), source.Ref, id, string(types.StateRunning), runID)
	if err != nil {
		return false, fmt.Errorf("store: commit running paused sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("commit running paused", id, result)
}

// ClearPausedRunner records successful stop/reset cleanup without discarding
// the exact runner identity before that cleanup has completed. A crash between
// CommitRunningPaused and this CAS therefore leaves enough durable ownership
// for restart reconciliation to retry safely.
func (s *Store) ClearPausedRunner(ctx context.Context, id, expectedRunID string) (bool, error) {
	if expectedRunID == "" {
		return false, fmt.Errorf("store: clear paused runner sandbox %s: empty run id", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET run_id=''
		 WHERE id=? AND state=? AND run_id=?`,
		id, string(types.StatePaused), expectedRunID)
	if err != nil {
		return false, fmt.Errorf("store: clear paused runner sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("clear paused runner", id, result)
}

// ClearPausedNetwork is the corresponding exact ownership fence for a
// successfully detached port. Clearing every derived address in the same CAS
// prevents a later resume from retaining a released/reused network identity.
func (s *Store) ClearPausedNetwork(ctx context.Context, id, expectedPort string) (bool, error) {
	if expectedPort == "" {
		return false, fmt.Errorf("store: clear paused network sandbox %s: empty port", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET floatingip='', vswitch_port='', inner_ip='', port_mac=''
		 WHERE id=? AND state=? AND vswitch_port=?`,
		id, string(types.StatePaused), expectedPort)
	if err != nil {
		return false, fmt.Errorf("store: clear paused network sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("clear paused network", id, result)
}

// ClearPausedRunDir records that the exact paused RunDir has been removed. Its
// UDS paths are part of the same ownership and are cleared atomically. Requiring
// empty runner/network fields prevents callers from advancing directory cleanup
// before the process and port fences have completed.
func (s *Store) ClearPausedRunDir(ctx context.Context, id, expectedRunDir, expectedEnvdUDS, expectedCiUDS string) (bool, error) {
	if expectedRunDir == "" {
		return false, fmt.Errorf("store: clear paused run dir sandbox %s: empty run dir", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET run_dir='', envd_uds='', ci_uds=''
		 WHERE id=? AND state=?
		   AND run_id='' AND floatingip='' AND vswitch_port='' AND inner_ip='' AND port_mac=''
		   AND run_dir=? AND envd_uds=? AND ci_uds=?`,
		id, string(types.StatePaused), expectedRunDir, expectedEnvdUDS, expectedCiUDS)
	if err != nil {
		return false, fmt.Errorf("store: clear paused run dir sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("clear paused run dir", id, result)
}

// BeginSandboxDelete is the durable acceptance fence for an explicit delete.
// It changes only lifecycle state (and clears the no-longer-actionable launch
// mode), preserving every exact runner, network, and path identity needed by a
// retrying node-local finalizer. The complete ownership tuple is part of the
// CAS so a stale API or node-link command cannot delete a newer incarnation.
func (s *Store) BeginSandboxDelete(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.ID == "" {
		return false, errors.New("store: begin sandbox delete requires a sandbox")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET state=?, launch_mode='', dead_unix=0
		 WHERE id=? AND state=? AND launch_mode=?
		   AND run_id=? AND floatingip=? AND vswitch_port=? AND inner_ip=? AND port_mac=?
		   AND run_dir=? AND base_dir=? AND envd_uds=? AND ci_uds=?
		   AND resume_source_kind=? AND resume_source_ref=? AND created_unix=?`,
		string(types.StateDeleting), sb.ID, string(sb.State), string(sb.LaunchMode),
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC,
		sb.RunDir, sb.BaseDir, sb.EnvdUDS, sb.CiUDS,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.CreatedUnix)
	if err != nil {
		return false, fmt.Errorf("store: begin sandbox delete %s: %w", sb.ID, err)
	}
	return sandboxUpdateChanged("begin delete", sb.ID, result)
}

// ClearDeletingNetwork records that the exact deleting Sandbox incarnation no
// longer owns its connector port. Every derived network field is cleared in
// the same full-owner CAS so a stale finalizer cannot clear a changed row.
func (s *Store) ClearDeletingNetwork(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.ID == "" || sb.State != types.StateDeleting || sb.VswitchPort == "" {
		return false, errors.New("store: clear deleting network requires a deleting sandbox with a port")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET floatingip='', vswitch_port='', inner_ip='', port_mac=''
		 WHERE id=? AND state=? AND launch_mode=?
		   AND run_id=? AND floatingip=? AND vswitch_port=? AND inner_ip=? AND port_mac=?
		   AND run_dir=? AND base_dir=? AND envd_uds=? AND ci_uds=?
		   AND resume_source_kind=? AND resume_source_ref=? AND created_unix=?`,
		sb.ID, string(types.StateDeleting), string(sb.LaunchMode),
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC,
		sb.RunDir, sb.BaseDir, sb.EnvdUDS, sb.CiUDS,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.CreatedUnix)
	if err != nil {
		return false, fmt.Errorf("store: clear deleting network sandbox %s: %w", sb.ID, err)
	}
	return sandboxUpdateChanged("clear deleting network", sb.ID, result)
}

// DeleteFinalizedSandbox hard-deletes only the exact deleting row whose local
// ownership the caller has already fenced and removed. Network fields may have
// been durably cleared after Detach; every remaining field stays unchanged so
// a process crash leaves sufficient information for startup reconciliation.
func (s *Store) DeleteFinalizedSandbox(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.ID == "" || sb.State != types.StateDeleting {
		return false, errors.New("store: finalize sandbox delete requires a deleting sandbox")
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM sandboxes
		 WHERE id=? AND state=? AND launch_mode=?
		   AND run_id=? AND floatingip=? AND vswitch_port=? AND inner_ip=? AND port_mac=?
		   AND run_dir=? AND base_dir=? AND envd_uds=? AND ci_uds=?
		   AND resume_source_kind=? AND resume_source_ref=? AND created_unix=?`,
		sb.ID, string(types.StateDeleting), string(sb.LaunchMode),
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC,
		sb.RunDir, sb.BaseDir, sb.EnvdUDS, sb.CiUDS,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.CreatedUnix)
	if err != nil {
		return false, fmt.Errorf("store: finalize sandbox delete %s: %w", sb.ID, err)
	}
	return sandboxUpdateChanged("finalize delete", sb.ID, result)
}

func (s *Store) RollbackStartingDead(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.State != types.StateStarting || !sb.ResumeSource.Empty() {
		return false, nil
	}
	return s.CommitSandboxDead(ctx, sb)
}

// CommitSandboxDead records non-delete terminal history only after the caller
// has fenced and removed every local resource. It accepts a starting launch
// failure or a running sandbox found without a live unit and clears all
// ownership atomically.
func (s *Store) CommitSandboxDead(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.ID == "" {
		return false, errors.New("store: commit sandbox dead requires a sandbox")
	}
	if sb.State != types.StateStarting && sb.State != types.StateRunning {
		return false, fmt.Errorf("store: commit sandbox %s dead from invalid state %q", sb.ID, sb.State)
	}
	if sb.State == types.StateStarting && !sb.ResumeSource.Empty() {
		return false, fmt.Errorf("store: commit resumed sandbox %s dead", sb.ID)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, launch_mode='', run_id='', floatingip='', vswitch_port='', inner_ip='', port_mac='',
		       run_dir='', base_dir='', envd_uds='', ci_uds='', resume_source_kind='', resume_source_ref='',
		       dead_unix=unixepoch()
		 WHERE id=? AND state=? AND launch_mode=?
		   AND run_id=? AND floatingip=? AND vswitch_port=? AND inner_ip=? AND port_mac=?
		   AND run_dir=? AND base_dir=? AND envd_uds=? AND ci_uds=?
		   AND resume_source_kind=? AND resume_source_ref=? AND created_unix=?`,
		string(types.StateDead), sb.ID, string(sb.State), string(sb.LaunchMode),
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC,
		sb.RunDir, sb.BaseDir, sb.EnvdUDS, sb.CiUDS,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.CreatedUnix)
	if err != nil {
		return false, fmt.Errorf("store: commit sandbox %s dead: %w", sb.ID, err)
	}
	return sandboxUpdateChanged("commit dead", sb.ID, result)
}

func (s *Store) RollbackStartingPaused(ctx context.Context, sb *types.Sandbox) (bool, error) {
	if sb == nil || sb.State != types.StateStarting || !sb.ResumeSource.Valid() {
		return false, nil
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, launch_mode='', run_id='', floatingip='', vswitch_port='', inner_ip='', port_mac='',
		       run_dir='', envd_uds='', ci_uds=''
		 WHERE id=? AND state=? AND launch_mode=?
		   AND run_id=? AND floatingip=? AND vswitch_port=? AND inner_ip=? AND port_mac=?
		   AND run_dir=? AND base_dir=? AND envd_uds=? AND ci_uds=?
		   AND resume_source_kind=? AND resume_source_ref=? AND created_unix=?
		   AND ((launch_mode=? AND resume_source_kind=?) OR
		        (launch_mode=? AND resume_source_kind IN (?,?)))`,
		string(types.StatePaused), sb.ID, string(types.StateStarting), string(sb.LaunchMode),
		sb.RunID, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC,
		sb.RunDir, sb.BaseDir, sb.EnvdUDS, sb.CiUDS,
		string(sb.ResumeSource.Kind), sb.ResumeSource.Ref, sb.CreatedUnix,
		string(types.LaunchMemory), string(types.ResumeSourceSnapshot),
		string(types.LaunchCold), string(types.ResumeSourceSnapshot), string(types.ResumeSourceSandbox))
	if err != nil {
		return false, fmt.Errorf("store: rollback resume starting sandbox %s to paused: %w", sb.ID, err)
	}
	return sandboxUpdateChanged("rollback resume starting to paused", sb.ID, result)
}

var cols = `id,profile,cluster_group,cluster_route_key,stable_id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,floatingip,
  vswitch_port,inner_ip,port_mac,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,
  resume_source_kind,resume_source_ref,auto_pause_memory,launch_mode,
  service_secret_enc,envd_access_token_enc,traffic_access_token_enc,forward_access_token_enc,metadata_json,env_json,created_unix,dead_unix`

func (s *Store) scan(row interface{ Scan(...any) error }) (*types.Sandbox, error) {
	var sb types.Sandbox
	var profile, clusterGroup, clusterRouteKey, st, meta, env, apiHash, apiEnc, manifestHash, manifestEnc string
	var resumeSourceKind, launchMode string
	var autoPauseMemory bool
	var serviceSecretEnc, envdAccessTokenEnc, trafficAccessTokenEnc, forwardAccessTokenEnc string
	if err := row.Scan(&sb.ID, &profile, &clusterGroup, &clusterRouteKey, &sb.StableIDValue,
		&sb.TemplateID, &st, &sb.DeadlineUnix, &sb.RunDir, &sb.BaseDir,
		&sb.RunID, &sb.EnvdUDS, &sb.CiUDS, &sb.FloatingIP, &sb.VswitchPort, &sb.InnerIP, &sb.PortMAC,
		&apiHash, &apiEnc, &manifestHash, &manifestEnc,
		&resumeSourceKind, &sb.ResumeSource.Ref, &autoPauseMemory, &launchMode,
		&serviceSecretEnc, &envdAccessTokenEnc, &trafficAccessTokenEnc, &forwardAccessTokenEnc,
		&meta, &env, &sb.CreatedUnix, &sb.DeadUnix); err != nil {
		return nil, err
	}
	pair, err := s.decryptVerifiedKeyPair(apiHash, apiEnc, manifestHash, manifestEnc)
	if err != nil {
		return nil, fmt.Errorf("store: read credential pair for %s: %w", sb.ID, err)
	}
	sb.APISecret = pair.APISecret
	sb.ManifestKey = pair.ManifestKey
	for name, encrypted := range map[string]struct {
		ciphertext string
		dest       *string
	}{
		"service secret":       {serviceSecretEnc, &sb.ServiceSecret},
		"envd access token":    {envdAccessTokenEnc, &sb.EnvdAccessToken},
		"traffic access token": {trafficAccessTokenEnc, &sb.TrafficAccessToken},
		"forward access token": {forwardAccessTokenEnc, &sb.ForwardAccessToken},
	} {
		plaintext, err := s.box.DecryptString(encrypted.ciphertext)
		if err != nil {
			return nil, fmt.Errorf("store: decrypt %s for sandbox %s: %w", name, sb.ID, err)
		}
		*encrypted.dest = plaintext
	}
	sb.Profile, sb.State = types.Profile(profile), types.State(st)
	sb.ResumeSource.Kind = types.ResumeSourceKind(resumeSourceKind)
	sb.AutoPauseMemory = autoPauseMemory
	sb.LaunchMode = types.LaunchMode(launchMode)
	if !sb.Profile.Valid() {
		return nil, fmt.Errorf("store: sandbox %s has invalid profile %q", sb.ID, profile)
	}
	switch {
	case clusterGroup == "" && clusterRouteKey == "":
		sb.Cluster = nil
	case clusterGroup == "" || clusterRouteKey == "":
		return nil, fmt.Errorf("store: sandbox %s has incomplete cluster context", sb.ID)
	default:
		sb.Cluster = &types.ClusterSandboxContext{Group: clusterGroup, RouteKey: clusterRouteKey}
	}
	if err := validateSandboxServiceCredentials(&sb); err != nil {
		return nil, fmt.Errorf("store: sandbox %s has corrupt service credentials: %w", sb.ID, err)
	}
	if err := validateSandboxLifecycle(&sb); err != nil {
		return nil, fmt.Errorf("store: sandbox %s has invalid lifecycle state: %w", sb.ID, err)
	}
	sb.Metadata, sb.Env = uj(meta), uj(env)
	return &sb, nil
}

func validateSandboxServiceCredentials(sb *types.Sandbox) error {
	if _, err := secretHash("service secret", sb.ServiceSecret); err != nil {
		return err
	}
	if sb.ForwardAccessToken == "" {
		return errors.New("forward access token is required")
	}
	if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, sb.ServiceSecret, sb.StableID()); err != nil {
		return errors.New("forward access token is invalid")
	}
	switch sb.Profile {
	case types.ProfileE2B:
		if !sandboxcfg.ValidE2BAccessToken(sb.EnvdAccessToken) ||
			!sandboxcfg.ValidE2BAccessToken(sb.TrafficAccessToken) {
			return errors.New("envd and traffic access tokens must be valid UTF-8 and at most 256 bytes")
		}
		if sb.EnvdAccessToken == "" {
			return errors.New("envd access token is required for e2b profile")
		}
		if sb.TrafficAccessToken == "" {
			return errors.New("traffic access token is required for e2b profile")
		}
	case types.ProfileBare:
		if sb.EnvdAccessToken != "" || sb.TrafficAccessToken != "" {
			return errors.New("envd and traffic access tokens must be empty for bare profile")
		}
	default:
		return fmt.Errorf("invalid sandbox profile %q", sb.Profile)
	}
	return nil
}

func sandboxIdentityColumns(sb *types.Sandbox) (group, routeKey string, err error) {
	if !sb.Profile.Valid() {
		return "", "", fmt.Errorf("invalid sandbox profile %q", sb.Profile)
	}
	if sb.Cluster == nil {
		return "", "", nil
	}
	if sb.Cluster.Group == "" || sb.Cluster.RouteKey == "" {
		return "", "", errors.New("cluster group and route key are both required")
	}
	return sb.Cluster.Group, sb.Cluster.RouteKey, nil
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

// RangeSandboxes streams every durable sandbox in a stable order from one
// SQLite read snapshot. fn must be read-only with respect to Store while the
// cursor is open. Returning an error stops iteration and returns that error.
func (s *Store) RangeSandboxes(ctx context.Context, fn func(*types.Sandbox) error) error {
	if fn == nil {
		return errors.New("store: range sandboxes callback is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+cols+` FROM sandboxes ORDER BY created_unix ASC, id ASC`)
	if err != nil {
		return fmt.Errorf("store: range sandboxes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		sandbox, err := s.scan(rows)
		if err != nil {
			return err
		}
		if err := fn(sandbox); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: range sandboxes: %w", err)
	}
	return nil
}

// List returns sandboxes ordered by id (cursor pagination). ownerCandidateHash
// is the 24-hex API-secret fingerprint prefix embedded in an API key. It is only
// a pre-filter; the caller must verify the API-key MAC against every candidate.
// An empty ownerCandidateHash disables only the owner filter. When state is
// empty, List returns the public running/paused states and hides internal
// starting/deleting/dead lifecycle records.
func (s *Store) List(ctx context.Context, state, ownerCandidateHash string, limit int, cursor string) ([]*types.Sandbox, string, error) {
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
	} else {
		// Match the upstream E2B list contract: an omitted state means the two
		// public states, not internal starting/deleting/dead lifecycle records.
		conds = append(conds, "state IN (?,?)")
		args = append(args, string(types.StateRunning), string(types.StatePaused))
	}
	if ownerCandidateHash != "" {
		conds = append(conds, "substr(api_secret_hash,1,24)=?")
		args = append(args, ownerCandidateHash)
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
	if st != types.StateRunning {
		return fmt.Errorf("store: SetState only supports running; use a typed terminal transition for %q", st)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=?, launch_mode='' WHERE id=?`, string(st), id)
	return err
}

// CASRunState changes lifecycle state only while the row still names the exact
// runner that observed the transition. It fences late launch completion/cleanup
// from changing a deleted, recreated, or subsequently launched sandbox with the
// same ID.
func (s *Store) CASRunState(ctx context.Context, id, runID string, from, to types.State) (bool, error) {
	if to == types.StatePaused || to == types.StateStarting || to == types.StateDeleting || to == types.StateDead {
		return false, fmt.Errorf("store: CASRunState cannot enter %q; use the typed lifecycle transition", to)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=?, launch_mode='' WHERE id=? AND run_id=? AND state=?`,
		string(to), id, runID, string(from))
	if err != nil {
		return false, fmt.Errorf("store: cas sandbox %s run state: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: cas sandbox %s run state rows: %w", id, err)
	}
	return n == 1, nil
}

func (s *Store) SetDeadline(ctx context.Context, id string, unix int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET deadline_unix=? WHERE id=?`, unix, id)
	return err
}

// SetDeadlineIfState updates a live lifecycle row without allowing a stale
// timeout request to mutate deleting/dead cleanup history. The caller supplies
// the state it observed while holding the per-sandbox lifecycle lock; the SQL
// predicate remains the durable fence if another writer bypasses that lock.
func (s *Store) SetDeadlineIfState(ctx context.Context, id string, expected types.State, unix int64) (bool, error) {
	switch expected {
	case types.StateStarting, types.StateRunning, types.StatePaused:
	default:
		return false, fmt.Errorf("store: set deadline sandbox %s: invalid state %q", id, expected)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET deadline_unix=? WHERE id=? AND state=?`, unix, id, string(expected))
	if err != nil {
		return false, fmt.Errorf("store: set deadline sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("set deadline", id, result)
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sandboxes WHERE id=?`, id)
	return err
}

// --- builds (also the template registry) ---

var buildCols = `build_id,template_id,persist_id,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,profile,kind,
  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,
  registration_image_repo,registration_registry_auth_enc,registration_mmds_routes_digest,registration_mmds_values_digest,registration_request_digest,cluster_group,
  resources_cpu,resources_memory,resources_storage,metadata_json,builder_json,instance_config_enc,
  waiting_unix,waiting_sequence,execution_claimed,execution_claimed_unix,enforcement_status,phase,phase_sandbox_id,
  runtime_vswitch_port,runtime_floating_ip,runtime_port_mac,runtime_envd_access_token_enc,runtime_prepare_json,execution_result_json,finished_unix`

func (s *Store) scanBuild(row interface{ Scan(...any) error }) (*types.Build, error) {
	var b types.Build
	var profile, kind, status, names, aliases, apiHash, apiEnc, manifestHash, manifestEnc, raEnc, registrationRAEnc, steps, meta, builder string
	var runtimeEnvdAccessTokenEnc, executionResultJSON, instanceConfigEnc string
	var executionClaimed int
	if err := row.Scan(&b.BuildID, &b.TemplateID, &b.PersistID, &apiHash, &apiEnc, &manifestHash, &manifestEnc, &profile, &kind,
		&b.FromImage, &b.FromTemplate, &b.StartCmd, &b.ReadyCmd, &steps, &status, &b.Reason, &b.RunID, &names, &aliases, &b.CreatedUnix, &raEnc,
		&b.RegistrationImageRepo, &registrationRAEnc, &b.RegistrationMMDSRoutesDigest, &b.RegistrationMMDSValuesDigest, &b.RegistrationRequestDigest, &b.ClusterGroup,
		&b.Resources.CPU, &b.Resources.Memory, &b.Resources.Storage, &meta, &builder, &instanceConfigEnc,
		&b.WaitingUnix, &b.WaitingSequence, &executionClaimed, &b.ExecutionClaimedUnix, &b.EnforcementStatus, &b.Phase, &b.PhaseSandboxID,
		&b.RuntimeVswitchPort, &b.RuntimeFloatingIP, &b.RuntimePortMAC, &runtimeEnvdAccessTokenEnc, &b.RuntimePrepareJSON, &executionResultJSON, &b.FinishedUnix); err != nil {
		return nil, err
	}
	b.ExecutionClaimed = executionClaimed != 0
	b.Metadata = uj(meta)
	b.Builder = ub(builder)
	plain, err := s.box.DecryptString(instanceConfigEnc)
	if err != nil {
		return nil, fmt.Errorf("store: decrypt instance config for build %s: %w", b.BuildID, err)
	}
	var instance buildInstanceConfig
	if err := json.Unmarshal([]byte(plain), &instance); err != nil {
		return nil, fmt.Errorf("store: decode instance config for build %s: %w", b.BuildID, err)
	}
	b.Env, b.Secure = instance.Env, instance.Secure
	b.ServiceSecret, b.EnvdAccessToken, b.TrafficAccessToken = instance.ServiceSecret, instance.EnvdAccessToken, instance.TrafficAccessToken
	if steps != "" && steps != "[]" {
		if err := json.Unmarshal([]byte(steps), &b.Steps); err != nil {
			return nil, fmt.Errorf("store: build %s steps: %w", b.BuildID, err)
		}
	}
	pair, err := s.decryptVerifiedKeyPair(apiHash, apiEnc, manifestHash, manifestEnc)
	if err != nil {
		return nil, fmt.Errorf("store: read credential pair for build %s: %w", b.BuildID, err)
	}
	b.APISecret = pair.APISecret
	b.ManifestKey = pair.ManifestKey
	if raEnc != "" {
		if b.RegistryAuth, err = s.box.DecryptString(raEnc); err != nil {
			return nil, fmt.Errorf("store: decrypt registry auth for build %s: %w", b.BuildID, err)
		}
	}
	if registrationRAEnc != "" {
		if b.RegistrationRegistryAuth, err = s.box.DecryptString(registrationRAEnc); err != nil {
			return nil, fmt.Errorf("store: decrypt registration registry auth for build %s: %w", b.BuildID, err)
		}
	}
	if runtimeEnvdAccessTokenEnc != "" {
		if b.RuntimeEnvdAccessToken, err = s.box.DecryptString(runtimeEnvdAccessTokenEnc); err != nil {
			return nil, fmt.Errorf("store: decrypt runtime envd access token for build %s: %w", b.BuildID, err)
		}
	}
	if executionResultJSON != "" {
		var result types.BuildResult
		if err := json.Unmarshal([]byte(executionResultJSON), &result); err != nil {
			return nil, fmt.Errorf("store: decode execution result for build %s: %w", b.BuildID, err)
		}
		b.ExecutionResult = &result
	}
	b.Profile, b.Kind, b.Status = types.Profile(profile), types.Kind(kind), types.BuildState(status)
	b.Names, b.Aliases = ujs(names), ujs(aliases)
	return &b, nil
}

const buildInsertSQL = `
	INSERT INTO builds (build_id,template_id,persist_id,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,profile,kind,
	  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,
	  registration_image_repo,registration_registry_auth_enc,registration_mmds_routes_digest,registration_mmds_values_digest,registration_request_digest,cluster_group,
	  resources_cpu,resources_memory,resources_storage,metadata_json,builder_json,instance_config_enc,
	  waiting_unix,waiting_sequence,execution_claimed,execution_claimed_unix,enforcement_status,phase,phase_sandbox_id,
	  runtime_vswitch_port,runtime_floating_ip,runtime_port_mac,runtime_envd_access_token_enc,runtime_prepare_json,execution_result_json,finished_unix)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

const buildUpsertSQL = buildInsertSQL + `
	ON CONFLICT(build_id) DO UPDATE SET
	  template_id=excluded.template_id, persist_id=excluded.persist_id,
	  profile=excluded.profile, kind=excluded.kind, from_image=excluded.from_image,
	  from_template=excluded.from_template, start_cmd=excluded.start_cmd,
	  ready_cmd=excluded.ready_cmd, steps_json=excluded.steps_json,
	  status=excluded.status, reason=excluded.reason, run_id=excluded.run_id,
	  names_json=excluded.names_json, aliases_json=excluded.aliases_json,
	  registry_auth_enc=excluded.registry_auth_enc, cluster_group=excluded.cluster_group,
	  resources_cpu=excluded.resources_cpu, resources_memory=excluded.resources_memory,
	  resources_storage=excluded.resources_storage,
	  metadata_json=excluded.metadata_json, builder_json=excluded.builder_json,
	  instance_config_enc=excluded.instance_config_enc,
	  waiting_unix=excluded.waiting_unix, waiting_sequence=excluded.waiting_sequence,
	  execution_claimed=excluded.execution_claimed,
	  execution_claimed_unix=excluded.execution_claimed_unix,
	  enforcement_status=excluded.enforcement_status, phase=excluded.phase,
	  phase_sandbox_id=excluded.phase_sandbox_id,
	  runtime_vswitch_port=excluded.runtime_vswitch_port,
	  runtime_floating_ip=excluded.runtime_floating_ip,
	  runtime_port_mac=excluded.runtime_port_mac,
	  runtime_envd_access_token_enc=excluded.runtime_envd_access_token_enc,
	  runtime_prepare_json=excluded.runtime_prepare_json,
	  execution_result_json=excluded.execution_result_json,
	  finished_unix=excluded.finished_unix`

const buildInsertOnlySQL = buildInsertSQL + ` ON CONFLICT(build_id) DO NOTHING`

func (s *Store) prepareBuildWrite(b *types.Build) ([]any, error) {
	if b == nil || b.BuildID == "" {
		return nil, errors.New("build is required")
	}
	terminal := b.Status == types.BuildReady || b.Status == types.BuildError
	if terminal && b.FinishedUnix == 0 {
		b.FinishedUnix = time.Now().Unix()
	}
	if !terminal && b.FinishedUnix != 0 {
		return nil, fmt.Errorf("store: put build %s: nonterminal state %s has a terminal timestamp", b.BuildID, b.Status)
	}
	apiHash, apiEnc, err := s.encSecret("API secret", b.APISecret)
	if err != nil {
		return nil, fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	manifestHash, manifestEnc, err := s.encSecret("manifest key", b.ManifestKey)
	if err != nil {
		return nil, fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	var raEnc string
	if b.RegistryAuth != "" {
		if raEnc, err = s.box.EncryptString(b.RegistryAuth); err != nil {
			return nil, fmt.Errorf("store: put build %s: encrypt registry auth: %w", b.BuildID, err)
		}
	}
	var registrationRAEnc string
	if b.RegistrationRegistryAuth != "" {
		if registrationRAEnc, err = s.box.EncryptString(b.RegistrationRegistryAuth); err != nil {
			return nil, fmt.Errorf("store: put build %s: encrypt registration registry auth: %w", b.BuildID, err)
		}
	}
	var runtimeEnvdAccessTokenEnc string
	if b.RuntimeEnvdAccessToken != "" {
		if runtimeEnvdAccessTokenEnc, err = s.box.EncryptString(b.RuntimeEnvdAccessToken); err != nil {
			return nil, fmt.Errorf("store: put build %s: encrypt runtime envd access token: %w", b.BuildID, err)
		}
	}
	executionResultJSON := ""
	if b.ExecutionResult != nil {
		encoded, err := json.Marshal(b.ExecutionResult)
		if err != nil {
			return nil, fmt.Errorf("store: put build %s: execution result: %w", b.BuildID, err)
		}
		executionResultJSON = string(encoded)
	}
	instanceJSON, err := json.Marshal(buildInstanceConfig{
		Env: b.Env, Secure: b.Secure, ServiceSecret: b.ServiceSecret,
		EnvdAccessToken: b.EnvdAccessToken, TrafficAccessToken: b.TrafficAccessToken,
	})
	if err != nil {
		return nil, fmt.Errorf("store: put build %s: instance config: %w", b.BuildID, err)
	}
	instanceConfigEnc, err := s.box.EncryptString(string(instanceJSON))
	if err != nil {
		return nil, fmt.Errorf("store: put build %s: encrypt instance config: %w", b.BuildID, err)
	}
	stepsJSON := "[]"
	if len(b.Steps) > 0 {
		sj, jerr := json.Marshal(b.Steps)
		if jerr != nil {
			return nil, fmt.Errorf("store: put build %s: steps: %w", b.BuildID, jerr)
		}
		stepsJSON = string(sj)
	}
	return []any{
		b.BuildID, b.TemplateID, b.PersistID, apiHash, apiEnc, manifestHash, manifestEnc, string(b.Profile), string(b.Kind),
		b.FromImage, b.FromTemplate, b.StartCmd, b.ReadyCmd, stepsJSON, string(b.Status), b.Reason, b.RunID,
		mjs(b.Names), mjs(b.Aliases), b.CreatedUnix, raEnc,
		b.RegistrationImageRepo, registrationRAEnc, b.RegistrationMMDSRoutesDigest, b.RegistrationMMDSValuesDigest, b.RegistrationRequestDigest, b.ClusterGroup,
		b.Resources.CPU, b.Resources.Memory, b.Resources.Storage,
		mj(b.Metadata), mb(b.Builder), instanceConfigEnc, b.WaitingUnix, b.WaitingSequence,
		boolInt(b.ExecutionClaimed), b.ExecutionClaimedUnix,
		b.EnforcementStatus, b.Phase, b.PhaseSandboxID,
		b.RuntimeVswitchPort, b.RuntimeFloatingIP, b.RuntimePortMAC, runtimeEnvdAccessTokenEnc, b.RuntimePrepareJSON, executionResultJSON, b.FinishedUnix,
	}, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// PutBuild upserts a build record. The credential pair is written only by the
// initial insert; later build-state updates cannot rebind an existing build.
func (s *Store) PutBuild(ctx context.Context, b *types.Build) error {
	args, err := s.prepareBuildWrite(b)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, buildUpsertSQL, args...)
	if err != nil {
		return fmt.Errorf("store: put build %s: %w", b.BuildID, err)
	}
	return nil
}

// CommitBuildTrigger atomically publishes the complete trigger-owned work
// order and transitions a registered build to waiting. The builder pool must
// never observe waiting before every work-order field is visible.
func (s *Store) CommitBuildTrigger(ctx context.Context, b *types.Build) (bool, error) {
	if b == nil || b.BuildID == "" {
		return false, errors.New("build is required")
	}
	if b.Status != types.BuildWaiting {
		return false, fmt.Errorf("store: commit build trigger %s: status must be waiting", b.BuildID)
	}
	if b.Kind != "" {
		return false, fmt.Errorf("store: commit build trigger %s: kind must remain unset until terminal result", b.BuildID)
	}
	stepsJSON := "[]"
	if len(b.Steps) > 0 {
		encoded, err := json.Marshal(b.Steps)
		if err != nil {
			return false, fmt.Errorf("store: commit build trigger %s: steps: %w", b.BuildID, err)
		}
		stepsJSON = string(encoded)
	}
	registryAuthEnc := ""
	if b.RegistryAuth != "" {
		encrypted, err := s.box.EncryptString(b.RegistryAuth)
		if err != nil {
			return false, fmt.Errorf("store: commit build trigger %s: encrypt registry auth: %w", b.BuildID, err)
		}
		registryAuthEnc = encrypted
	}
	res, err := s.db.ExecContext(ctx, `UPDATE builds SET
		from_image=?, from_template=?, start_cmd=?, ready_cmd=?, steps_json=?,
		registry_auth_enc=?, status=?, waiting_unix=?,
		waiting_sequence=(SELECT CASE
			WHEN COALESCE(MAX(waiting_sequence),0) >= 9223372036854775807 THEN NULL
			ELSE COALESCE(MAX(waiting_sequence),0)+1 END FROM builds)
		WHERE build_id=? AND status=?`,
		b.FromImage, b.FromTemplate, b.StartCmd, b.ReadyCmd, stepsJSON,
		registryAuthEnc, string(types.BuildWaiting), b.WaitingUnix,
		b.BuildID, string(types.BuildRegistered))
	if err != nil {
		return false, fmt.Errorf("store: commit build trigger %s: %w", b.BuildID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: commit build trigger %s rows affected: %w", b.BuildID, err)
	}
	return n == 1, nil
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

// RangeBuilds streams the durable current Build set in a stable order from one
// SQLite read snapshot. Error rows are historical and deliberately excluded.
// fn must be read-only with respect to Store while the cursor is open.
func (s *Store) RangeBuilds(ctx context.Context, fn func(*types.Build) error) error {
	if fn == nil {
		return errors.New("store: range builds callback is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds
		WHERE status IN (?,?,?,?) ORDER BY created_unix ASC, build_id ASC`,
		string(types.BuildRegistered), string(types.BuildWaiting), string(types.BuildBuilding), string(types.BuildReady))
	if err != nil {
		return fmt.Errorf("store: range builds: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		build, err := s.scanBuild(rows)
		if err != nil {
			return err
		}
		if err := fn(build); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: range builds: %w", err)
	}
	return nil
}

// RangeClusterBuilds streams every still-retained cluster-owned Build,
// including ready/error history. It is the node SQLite source for reconnect
// Build full sync; callers must remain read-only until the cursor closes.
func (s *Store) RangeClusterBuilds(ctx context.Context, fn func(*types.Build) error) error {
	if fn == nil {
		return errors.New("store: range cluster builds callback is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds
		WHERE cluster_group<>'' ORDER BY build_id ASC`)
	if err != nil {
		return fmt.Errorf("store: range cluster builds: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		build, err := s.scanBuild(rows)
		if err != nil {
			return err
		}
		if err := fn(build); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: range cluster builds: %w", err)
	}
	return nil
}

// GetClaimedBuildIDByRunID resolves an already-published builder assignment from
// durable execution ownership. It is the idempotent replay path when the
// config-socket response was interrupted after BindBuildRun committed.
func (s *Store) GetClaimedBuildIDByRunID(ctx context.Context, runID string) (string, bool, error) {
	if runID == "" {
		return "", false, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT build_id FROM builds
		WHERE run_id=? AND status=? AND execution_claimed=1 ORDER BY build_id LIMIT 2`, runID, string(types.BuildBuilding))
	if err != nil {
		return "", false, fmt.Errorf("store: get claimed build by run %s: %w", runID, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("store: get claimed build by run %s: %w", runID, err)
		}
		return "", false, nil
	}
	var buildID string
	if err := rows.Scan(&buildID); err != nil {
		return "", false, fmt.Errorf("store: get claimed build by run %s: %w", runID, err)
	}
	if rows.Next() {
		return "", false, fmt.Errorf("store: run %s has multiple claimed builds", runID)
	}
	return buildID, true, nil
}

// BuildingTaskIdentity verifies only non-secret exact-run ownership for the
// config-socket authentication phase. It deliberately does not scan/decrypt the
// Build row before SO_PEERCRED and pidfile authentication succeeds.
func (s *Store) BuildingTaskIdentity(ctx context.Context, buildID, runID string) (bool, error) {
	if buildID == "" || runID == "" {
		return false, nil
	}
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM builds
		WHERE build_id=? AND run_id=? AND status=? AND execution_claimed=1`,
		buildID, runID, string(types.BuildBuilding)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: verify build %s run %s task identity: %w", buildID, runID, err)
	}
	return one == 1, nil
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
	order := "created_unix ASC, build_id ASC"
	if status == types.BuildWaiting {
		order = "waiting_sequence ASC"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+buildCols+` FROM builds WHERE status=? ORDER BY `+order, string(status))
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
	if to == types.BuildReady || to == types.BuildError {
		return false, fmt.Errorf("store: CASBuildStatus cannot enter terminal state %q; use a typed terminal transition", to)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE builds SET status=? WHERE build_id=? AND status=?`,
		string(to), buildID, string(from))
	if err != nil {
		return false, fmt.Errorf("store: cas build %s: %w", buildID, err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// --- key pairs (the create/build allowlist, stored in manifest_keys) ---

// KeyPairInfo is a non-secret view of an allowlist row.
type KeyPairInfo struct {
	APISecretHash   string // complete SHA-256 fingerprint (64 lowercase hex)
	ManifestKeyHash string // complete SHA-256 fingerprint (64 lowercase hex)
	Label           string
	CreatedUnix     int64
	ExpiresUnix     int64 // 0 = never expires
}

// AddKeyPair atomically inserts a credential pair or refreshes an exact existing
// pair. A complete API-secret fingerprint binds exactly one pair: attempting to
// reuse it with different secret material returns ErrKeyPairConflict and leaves
// the existing row unchanged. ttlSec>0 sets expiry to now+ttlSec; ttlSec<=0 means
// never expires. Empty label or registryAuthJSON retains the existing value on a
// refresh. It returns true only when a new row was inserted.
func (s *Store) AddKeyPair(ctx context.Context, pair KeyPair, label string, ttlSec int64, registryAuthJSON string) (bool, error) {
	apiHash, apiEnc, err := s.encSecret("API secret", pair.APISecret)
	if err != nil {
		return false, err
	}
	manifestHash, manifestEnc, err := s.encSecret("manifest key", pair.ManifestKey)
	if err != nil {
		return false, err
	}
	var expires int64
	if ttlSec > 0 {
		expires = time.Now().Unix() + ttlSec
	}
	var registryAuthEnc string
	if registryAuthJSON != "" {
		registryAuthEnc, err = s.box.EncryptString(registryAuthJSON)
		if err != nil {
			return false, err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
INSERT INTO manifest_keys
  (api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,label,created_unix,expires_unix,registry_auth_enc)
VALUES (?,?,?,?,?,?,?,?)
ON CONFLICT(api_secret_hash) DO NOTHING`,
		apiHash, apiEnc, manifestHash, manifestEnc, label, time.Now().Unix(), expires, registryAuthEnc)
	if err != nil {
		return false, fmt.Errorf("store: add key pair: %w", err)
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: add key pair rows affected: %w", err)
	}
	if inserted == 1 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: add key pair commit: %w", err)
		}
		return true, nil
	}

	var existingAPIHash, existingAPIEnc, existingManifestHash, existingManifestEnc string
	if err := tx.QueryRowContext(ctx,
		`SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM manifest_keys WHERE api_secret_hash=?`, apiHash,
	).Scan(&existingAPIHash, &existingAPIEnc, &existingManifestHash, &existingManifestEnc); err != nil {
		return false, fmt.Errorf("store: read existing key pair: %w", err)
	}
	existing, err := s.decryptVerifiedKeyPair(existingAPIHash, existingAPIEnc, existingManifestHash, existingManifestEnc)
	if err != nil {
		return false, fmt.Errorf("store: read existing key pair: %w", err)
	}
	if !equalKeyPair(existing, pair) {
		return false, ErrKeyPairConflict
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE manifest_keys SET
  expires_unix=?,
  label=CASE WHEN ?='' THEN label ELSE ? END,
  registry_auth_enc=CASE WHEN ?='' THEN registry_auth_enc ELSE ? END
WHERE api_secret_hash=?`,
		expires, label, label, registryAuthJSON, registryAuthEnc, apiHash); err != nil {
		return false, fmt.Errorf("store: refresh key pair: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: refresh key pair commit: %w", err)
	}
	return false, nil
}

// RegistryAuthForKeyPair returns the registry credentials stored with an exact
// pair, or "" when the pair is absent or has no credentials.
func (s *Store) RegistryAuthForKeyPair(ctx context.Context, pair KeyPair) (string, error) {
	apiHash, _, err := keyPairHashes(pair)
	if err != nil {
		return "", err
	}
	var storedAPIHash, apiEnc, storedManifestHash, manifestEnc, registryAuthEnc string
	err = s.db.QueryRowContext(ctx, `
SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,registry_auth_enc
FROM manifest_keys WHERE api_secret_hash=?`, apiHash).Scan(
		&storedAPIHash, &apiEnc, &storedManifestHash, &manifestEnc, &registryAuthEnc,
	)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	stored, err := s.decryptVerifiedKeyPair(storedAPIHash, apiEnc, storedManifestHash, manifestEnc)
	if err != nil {
		return "", err
	}
	if !equalKeyPair(stored, pair) || registryAuthEnc == "" {
		return "", nil
	}
	return s.box.DecryptString(registryAuthEnc)
}

func (s *Store) decryptKeyPair(apiEnc, manifestEnc string) (KeyPair, error) {
	apiSecret, err := s.box.DecryptString(apiEnc)
	if err != nil {
		return KeyPair{}, fmt.Errorf("store: decrypt API secret: %w", err)
	}
	manifestKey, err := s.box.DecryptString(manifestEnc)
	if err != nil {
		return KeyPair{}, fmt.Errorf("store: decrypt manifest key: %w", err)
	}
	return KeyPair{APISecret: apiSecret, ManifestKey: manifestKey}, nil
}

func (s *Store) decryptVerifiedKeyPair(apiHash, apiEnc, manifestHash, manifestEnc string) (KeyPair, error) {
	pair, err := s.decryptKeyPair(apiEnc, manifestEnc)
	if err != nil {
		return KeyPair{}, err
	}
	actualAPIHash, err := APISecretHash(pair.APISecret)
	if err != nil {
		return KeyPair{}, fmt.Errorf("store: invalid stored API secret: %w", err)
	}
	actualManifestHash, err := ManifestKeyHash(pair.ManifestKey)
	if err != nil {
		return KeyPair{}, fmt.Errorf("store: invalid stored manifest key: %w", err)
	}
	if actualAPIHash != apiHash || actualManifestHash != manifestHash {
		return KeyPair{}, fmt.Errorf("store: key pair fingerprint mismatch")
	}
	return pair, nil
}

// HasKeyPair reports whether an exact, non-expired pair is in the allowlist.
func (s *Store) HasKeyPair(ctx context.Context, pair KeyPair) (bool, error) {
	apiHash, _, err := keyPairHashes(pair)
	if err != nil {
		return false, err
	}
	var storedAPIHash, apiEnc, storedManifestHash, manifestEnc string
	err = s.db.QueryRowContext(ctx, `
SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM manifest_keys
WHERE api_secret_hash=? AND (expires_unix=0 OR expires_unix>?)`,
		apiHash, time.Now().Unix()).Scan(&storedAPIHash, &apiEnc, &storedManifestHash, &manifestEnc)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	stored, err := s.decryptVerifiedKeyPair(storedAPIHash, apiEnc, storedManifestHash, manifestEnc)
	return equalKeyPair(stored, pair), err
}

// RemoveKeyPair deletes an exact allowlist pair and returns the number removed.
func (s *Store) RemoveKeyPair(ctx context.Context, pair KeyPair) (int, error) {
	apiHash, _, err := keyPairHashes(pair)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var storedAPIHash, apiEnc, storedManifestHash, manifestEnc string
	err = tx.QueryRowContext(ctx,
		`SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM manifest_keys WHERE api_secret_hash=?`, apiHash,
	).Scan(&storedAPIHash, &apiEnc, &storedManifestHash, &manifestEnc)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	stored, err := s.decryptVerifiedKeyPair(storedAPIHash, apiEnc, storedManifestHash, manifestEnc)
	if err != nil {
		return 0, err
	}
	if !equalKeyPair(stored, pair) {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM manifest_keys WHERE api_secret_hash=?`, apiHash)
	if err != nil {
		return 0, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(removed), nil
}

// RemoveKeyPairByAPISecretFingerprint deletes key-table material identified by
// a complete API-secret fingerprint. Expiry does not protect the allowlist row
// from deletion. Durable sandbox and build credential copies are not touched.
func (s *Store) RemoveKeyPairByAPISecretFingerprint(ctx context.Context, fingerprint string) (int, error) {
	if err := validateAPISecretFingerprint(fingerprint); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM manifest_keys WHERE api_secret_hash=?`, fingerprint)
	if err != nil {
		return 0, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(removed), nil
}

// ListKeyPairs returns the allowlist as non-secret fingerprints and labels.
func (s *Store) ListKeyPairs(ctx context.Context) ([]KeyPairInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT api_secret_hash,manifest_key_hash,label,created_unix,expires_unix
FROM manifest_keys ORDER BY created_unix ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyPairInfo
	for rows.Next() {
		var info KeyPairInfo
		if err := rows.Scan(&info.APISecretHash, &info.ManifestKeyHash, &info.Label, &info.CreatedUnix, &info.ExpiresUnix); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// AllowedKeyPairByAPISecretFingerprint returns the unique non-expired pair
// identified by a complete API-secret fingerprint. The decrypted pair is
// checked against both fingerprints stored in the row before it is returned.
func (s *Store) AllowedKeyPairByAPISecretFingerprint(ctx context.Context, fingerprint string) (KeyPair, bool, error) {
	if err := validateAPISecretFingerprint(fingerprint); err != nil {
		return KeyPair{}, false, err
	}
	var apiHash, apiEnc, manifestHash, manifestEnc string
	err := s.db.QueryRowContext(ctx, `
SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc
FROM manifest_keys
WHERE api_secret_hash=? AND (expires_unix=0 OR expires_unix>?)`,
		fingerprint, time.Now().Unix()).Scan(&apiHash, &apiEnc, &manifestHash, &manifestEnc)
	if err == sql.ErrNoRows {
		return KeyPair{}, false, nil
	}
	if err != nil {
		return KeyPair{}, false, err
	}
	pair, err := s.decryptVerifiedKeyPair(apiHash, apiEnc, manifestHash, manifestEnc)
	if err != nil {
		return KeyPair{}, false, err
	}
	return pair, true, nil
}

func validateAPISecretFingerprint(fingerprint string) error {
	if len(fingerprint) != secretHexLen || !hexSecretRE.MatchString(fingerprint) {
		return fmt.Errorf("store: API secret fingerprint must be 64 lowercase hex characters")
	}
	return nil
}

// AllowedKeyPairsByAPISecretHashPrefix returns non-expired candidate pairs for
// the 24-hex prefix carried in an API key. A prefix is not authoritative: the
// caller must verify the API-key MAC against every returned APISecret.
func (s *Store) AllowedKeyPairsByAPISecretHashPrefix(ctx context.Context, prefix string) ([]KeyPair, error) {
	if len(prefix) != apiSecretCandidateHashLen || !hexCandidateHashRE.MatchString(prefix) {
		return nil, fmt.Errorf("store: API secret hash prefix must be 24 lowercase hex characters")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM manifest_keys
WHERE substr(api_secret_hash,1,24)=? AND (expires_unix=0 OR expires_unix>?)`,
		prefix, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyPair
	for rows.Next() {
		var apiHash, apiEnc, manifestHash, manifestEnc string
		if err := rows.Scan(&apiHash, &apiEnc, &manifestHash, &manifestEnc); err != nil {
			return nil, err
		}
		pair, err := s.decryptVerifiedKeyPair(apiHash, apiEnc, manifestHash, manifestEnc)
		if err != nil {
			return nil, err
		}
		out = append(out, pair)
	}
	return out, rows.Err()
}

// PruneExpiredKeyPairs deletes expired allowlist rows. Expiry and pruning never
// touch credential copies already held by sandbox or build rows.
func (s *Store) PruneExpiredKeyPairs(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM manifest_keys WHERE expires_unix>0 AND expires_unix<?`, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
