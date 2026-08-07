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
  auth_sandbox_id      TEXT NOT NULL DEFAULT '',
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
  snapshot_ref         TEXT NOT NULL DEFAULT '',
  service_secret_enc       TEXT NOT NULL,
  envd_access_token_enc    TEXT NOT NULL,
  traffic_access_token_enc TEXT NOT NULL,
  forward_access_token_enc TEXT NOT NULL,
  metadata_json        TEXT NOT NULL DEFAULT '{}',
  env_json             TEXT NOT NULL DEFAULT '{}',
  created_unix         INTEGER NOT NULL
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
  metadata_json     TEXT NOT NULL DEFAULT '{}',
  builder_json      TEXT NOT NULL DEFAULT '{}'
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
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: init schema: %w", err)
	}
	return &Store{db: db, box: box}, nil
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

// --- sandboxes ---

const sandboxInsertSQL = `
	INSERT INTO sandboxes (id,profile,cluster_group,cluster_route_key,auth_sandbox_id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,
	  floatingip,vswitch_port,inner_ip,port_mac,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,snapshot_ref,
	  service_secret_enc,envd_access_token_enc,traffic_access_token_enc,forward_access_token_enc,metadata_json,env_json,created_unix)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

const sandboxUpsertSQL = sandboxInsertSQL + `
ON CONFLICT(id) DO UPDATE SET
  template_id=excluded.template_id, state=excluded.state, deadline_unix=excluded.deadline_unix,
  run_dir=excluded.run_dir, base_dir=excluded.base_dir, run_id=excluded.run_id, envd_uds=excluded.envd_uds,
  ci_uds=excluded.ci_uds, floatingip=excluded.floatingip, vswitch_port=excluded.vswitch_port,
  inner_ip=excluded.inner_ip, port_mac=excluded.port_mac,
  snapshot_ref=excluded.snapshot_ref,
  metadata_json=excluded.metadata_json, env_json=excluded.env_json`

const sandboxInsertOnlySQL = sandboxInsertSQL + `
ON CONFLICT(id) DO NOTHING`

func (s *Store) prepareSandboxInsert(sb *types.Sandbox) ([]any, error) {
	if sb == nil {
		return nil, errors.New("sandbox is required")
	}
	if !types.ValidLocalSandboxID(sb.ID) {
		return nil, errors.New("invalid sandbox id")
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
		sb.ID, string(sb.Profile), clusterGroup, clusterRouteKey, sb.AuthSandboxIDValue,
		sb.TemplateID, string(sb.State), sb.DeadlineUnix, sb.RunDir, sb.BaseDir, sb.RunID, sb.EnvdUDS,
		sb.CiUDS, sb.FloatingIP, sb.VswitchPort, sb.InnerIP, sb.PortMAC, apiHash, apiEnc, manifestHash, manifestEnc, sb.SnapshotRef,
		serviceSecretEnc, envdAccessTokenEnc, trafficAccessTokenEnc, forwardAccessTokenEnc,
		mj(sb.Metadata), mj(sb.Env), sb.CreatedUnix,
	}, nil
}

func sandboxWriteError(operation string, sb *types.Sandbox, err error) error {
	if sb == nil {
		return fmt.Errorf("store: %s: %w", operation, err)
	}
	return fmt.Errorf("store: %s %s: %w", operation, sb.ID, err)
}

// Put upserts a sandbox record. Profile, cluster identity, credential subject,
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

// BeginResume atomically accepts a paused resume. The previous runner and
// network fields are cleared in the same update because a paused FloatingIP may
// already have been released and reused while starting/running MMDS lookups are
// allowed.
func (s *Store) BeginResume(ctx context.Context, id string, deadlineUnix int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, deadline_unix=?, run_id='', floatingip='', vswitch_port='', inner_ip='', port_mac=''
		 WHERE id=? AND state=?`,
		string(types.StateStarting), deadlineUnix, id, string(types.StatePaused))
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

// CommitStartingRunning commits a successfully initialized sandbox only while
// the exact runner bound by the assignment callback still owns it.
func (s *Store) CommitStartingRunning(ctx context.Context, id, runID string) (bool, error) {
	if runID == "" {
		return false, fmt.Errorf("store: commit starting running sandbox %s: empty run id", id)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes SET state=?
		 WHERE id=? AND state=? AND run_id=?`,
		string(types.StateRunning), id, string(types.StateStarting), runID)
	if err != nil {
		return false, fmt.Errorf("store: commit starting running sandbox %s: %w", id, err)
	}
	return sandboxUpdateChanged("commit starting running", id, result)
}

func (s *Store) rollbackStarting(ctx context.Context, id, expectedRunID string, target types.State) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		   SET state=?, run_id='', floatingip='', vswitch_port='', inner_ip='', port_mac=''
		 WHERE id=? AND state=? AND run_id=?`,
		string(target), id, string(types.StateStarting), expectedRunID)
	if err != nil {
		return false, fmt.Errorf("store: rollback starting sandbox %s to %s: %w", id, target, err)
	}
	return sandboxUpdateChanged("rollback starting to "+string(target), id, result)
}

func (s *Store) RollbackStartingDead(ctx context.Context, id, expectedRunID string) (bool, error) {
	return s.rollbackStarting(ctx, id, expectedRunID, types.StateDead)
}

func (s *Store) RollbackStartingPaused(ctx context.Context, id, expectedRunID string) (bool, error) {
	return s.rollbackStarting(ctx, id, expectedRunID, types.StatePaused)
}

var cols = `id,profile,cluster_group,cluster_route_key,auth_sandbox_id,template_id,state,deadline_unix,run_dir,base_dir,run_id,envd_uds,ci_uds,floatingip,
  vswitch_port,inner_ip,port_mac,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,snapshot_ref,
  service_secret_enc,envd_access_token_enc,traffic_access_token_enc,forward_access_token_enc,metadata_json,env_json,created_unix`

func (s *Store) scan(row interface{ Scan(...any) error }) (*types.Sandbox, error) {
	var sb types.Sandbox
	var profile, clusterGroup, clusterRouteKey, st, meta, env, apiHash, apiEnc, manifestHash, manifestEnc string
	var serviceSecretEnc, envdAccessTokenEnc, trafficAccessTokenEnc, forwardAccessTokenEnc string
	if err := row.Scan(&sb.ID, &profile, &clusterGroup, &clusterRouteKey, &sb.AuthSandboxIDValue,
		&sb.TemplateID, &st, &sb.DeadlineUnix, &sb.RunDir, &sb.BaseDir,
		&sb.RunID, &sb.EnvdUDS, &sb.CiUDS, &sb.FloatingIP, &sb.VswitchPort, &sb.InnerIP, &sb.PortMAC,
		&apiHash, &apiEnc, &manifestHash, &manifestEnc, &sb.SnapshotRef,
		&serviceSecretEnc, &envdAccessTokenEnc, &trafficAccessTokenEnc, &forwardAccessTokenEnc,
		&meta, &env, &sb.CreatedUnix); err != nil {
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
	if err := keys.VerifyForwardAccessToken(sb.ForwardAccessToken, sb.ServiceSecret, sb.AuthSandboxID()); err != nil {
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

// List returns sandboxes ordered by id (cursor pagination). ownerCandidateHash
// is the 24-hex API-secret fingerprint prefix embedded in an API key. It is only
// a pre-filter; the caller must verify the API-key MAC against every candidate.
// An empty ownerCandidateHash disables only the owner filter. When state is
// empty, List returns the public running/paused states and hides internal
// starting/dead lifecycle records.
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
		// public states, not internal starting/dead lifecycle records.
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
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=? WHERE id=?`, string(st), id)
	return err
}

// CASRunState changes lifecycle state only while the row still names the exact
// runner that observed the transition. It fences late launch completion/cleanup
// from changing a deleted, recreated, or subsequently launched sandbox with the
// same ID.
func (s *Store) CASRunState(ctx context.Context, id, runID string, from, to types.State) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=? WHERE id=? AND run_id=? AND state=?`,
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

func (s *Store) SetSnapshotRef(ctx context.Context, id, ref string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET snapshot_ref=? WHERE id=?`, ref, id)
	return err
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sandboxes WHERE id=?`, id)
	return err
}

// --- builds (also the template registry) ---

var buildCols = `build_id,template_id,persist_id,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,profile,kind,
  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,metadata_json,builder_json`

func (s *Store) scanBuild(row interface{ Scan(...any) error }) (*types.Build, error) {
	var b types.Build
	var profile, kind, status, names, aliases, apiHash, apiEnc, manifestHash, manifestEnc, raEnc, steps, meta, builder string
	if err := row.Scan(&b.BuildID, &b.TemplateID, &b.PersistID, &apiHash, &apiEnc, &manifestHash, &manifestEnc, &profile, &kind,
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
	b.Profile, b.Kind, b.Status = types.Profile(profile), types.Kind(kind), types.BuildState(status)
	b.Names, b.Aliases = ujs(names), ujs(aliases)
	return &b, nil
}

const buildInsertSQL = `
	INSERT INTO builds (build_id,template_id,persist_id,api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,profile,kind,
	  from_image,from_template,start_cmd,ready_cmd,steps_json,status,reason,run_id,names_json,aliases_json,created_unix,registry_auth_enc,metadata_json,builder_json)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

const buildUpsertSQL = buildInsertSQL + `
	ON CONFLICT(build_id) DO UPDATE SET
	  template_id=excluded.template_id, persist_id=excluded.persist_id,
	  profile=excluded.profile, kind=excluded.kind, from_image=excluded.from_image,
	  from_template=excluded.from_template, start_cmd=excluded.start_cmd,
	  ready_cmd=excluded.ready_cmd, steps_json=excluded.steps_json,
	  status=excluded.status, reason=excluded.reason, run_id=excluded.run_id,
	  names_json=excluded.names_json, aliases_json=excluded.aliases_json,
	  registry_auth_enc=excluded.registry_auth_enc, metadata_json=excluded.metadata_json,
	  builder_json=excluded.builder_json`

const buildInsertOnlySQL = buildInsertSQL + ` ON CONFLICT(build_id) DO NOTHING`

func (s *Store) prepareBuildWrite(b *types.Build) ([]any, error) {
	if b == nil || b.BuildID == "" {
		return nil, errors.New("build is required")
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
		mjs(b.Names), mjs(b.Aliases), b.CreatedUnix, raEnc, mj(b.Metadata), mb(b.Builder),
	}, nil
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
