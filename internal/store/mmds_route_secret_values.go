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
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// MMDSRouteSecretOwnerKind distinguishes the two durable owner namespaces.
// Separate SQLite tables provide real foreign keys and cascade cleanup.
type MMDSRouteSecretOwnerKind string

const (
	MMDSRouteSecretOwnerSandbox MMDSRouteSecretOwnerKind = "sandbox"
	MMDSRouteSecretOwnerBuild   MMDSRouteSecretOwnerKind = "build"
)

// MMDSRouteSecretValues is one owner's opaque named value set. encoding/json
// represents each []byte as base64 inside the encrypted JSON blob; no value
// metadata, content type, TTL, or state machine is stored with it.
type MMDSRouteSecretValues map[string][]byte

type mmdsRouteSecretTable struct {
	table  string
	idCol  string
	fkName string
}

func mmdsRouteSecretOwnerTable(kind MMDSRouteSecretOwnerKind) (mmdsRouteSecretTable, error) {
	switch kind {
	case MMDSRouteSecretOwnerSandbox:
		return mmdsRouteSecretTable{table: "sandbox_mmds_route_secret_values", idCol: "sandbox_id", fkName: "sandbox"}, nil
	case MMDSRouteSecretOwnerBuild:
		return mmdsRouteSecretTable{table: "build_mmds_route_secret_values", idCol: "build_id", fkName: "build"}, nil
	default:
		return mmdsRouteSecretTable{}, fmt.Errorf("store: invalid MMDS route secret owner kind %q", kind)
	}
}

func mmdsRouteSecretAAD(kind MMDSRouteSecretOwnerKind, ownerID, routesDigest string, revision int64) []byte {
	// JSON gives unambiguous field boundaries even when an ID contains ':' or
	// another delimiter. Marshal cannot fail for this fixed string/int shape.
	aad, _ := json.Marshal(struct {
		OwnerKind    MMDSRouteSecretOwnerKind `json:"owner_kind"`
		OwnerID      string                   `json:"owner_id"`
		RoutesDigest string                   `json:"routes_digest"`
		Revision     int64                    `json:"revision"`
	}{kind, ownerID, routesDigest, revision})
	return aad
}

func encodeMMDSRouteSecretValues(values MMDSRouteSecretValues) ([]byte, error) {
	if values == nil {
		values = MMDSRouteSecretValues{}
	}
	return json.Marshal(values)
}

func decodeMMDSRouteSecretValues(plaintext []byte) (MMDSRouteSecretValues, error) {
	values := MMDSRouteSecretValues{}
	if err := json.Unmarshal(plaintext, &values); err != nil {
		return nil, err
	}
	if values == nil {
		values = MMDSRouteSecretValues{}
	}
	return values, nil
}

func cloneMMDSRouteSecretValues(values MMDSRouteSecretValues) MMDSRouteSecretValues {
	if values == nil {
		return nil
	}
	out := make(MMDSRouteSecretValues, len(values))
	for name, value := range values {
		out[name] = append([]byte(nil), value...)
	}
	return out
}

func registrationMMDSValuesDigest(manifestKey, buildID, routesDigest string, values MMDSRouteSecretValues) (string, error) {
	key, err := hex.DecodeString(manifestKey)
	if err != nil || len(key) != sha256.Size {
		return "", fmt.Errorf("registration MMDS value identity requires a 64-character hex manifest key")
	}
	canonical, err := encodeMMDSRouteSecretValues(values)
	if err != nil {
		return "", fmt.Errorf("encode registration MMDS value identity: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("kuasar-build-registration-mmds-values-v1\x00"))
	_, _ = mac.Write(mmdsRouteSecretAAD(MMDSRouteSecretOwnerBuild, buildID, routesDigest, 1))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func prepareBuildRegistrationMMDSIdentity(build *types.Build, routesDigest string, values MMDSRouteSecretValues) error {
	if build == nil {
		return errors.New("build is required")
	}
	if build.RegistrationMMDSRoutesDigest != "" && build.RegistrationMMDSRoutesDigest != routesDigest {
		return fmt.Errorf("store: register build %s: MMDS route identity does not match initial values", build.BuildID)
	}
	digest, err := registrationMMDSValuesDigest(build.ManifestKey, build.BuildID, routesDigest, values)
	if err != nil {
		return fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	build.RegistrationMMDSRoutesDigest = routesDigest
	build.RegistrationMMDSValuesDigest = digest
	return nil
}

func (s *Store) encryptMMDSRouteSecretValues(kind MMDSRouteSecretOwnerKind, ownerID, routesDigest string, revision int64, values MMDSRouteSecretValues) (string, error) {
	plaintext, err := encodeMMDSRouteSecretValues(values)
	if err != nil {
		return "", fmt.Errorf("store: encode MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	ciphertext, err := s.box.EncryptAAD(plaintext, mmdsRouteSecretAAD(kind, ownerID, routesDigest, revision))
	if err != nil {
		return "", fmt.Errorf("store: encrypt MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	return ciphertext, nil
}

func (s *Store) decryptMMDSRouteSecretValues(kind MMDSRouteSecretOwnerKind, ownerID, routesDigest string, revision int64, ciphertext string) (MMDSRouteSecretValues, error) {
	plaintext, err := s.box.DecryptAAD(ciphertext, mmdsRouteSecretAAD(kind, ownerID, routesDigest, revision))
	if err != nil {
		return nil, fmt.Errorf("store: decrypt MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	values, err := decodeMMDSRouteSecretValues(plaintext)
	if err != nil {
		return nil, fmt.Errorf("store: decode MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	return values, nil
}

func (s *Store) insertInitialMMDSRouteSecretValuesTx(ctx context.Context, tx *sql.Tx, kind MMDSRouteSecretOwnerKind, ownerID, routesDigest string, values MMDSRouteSecretValues) error {
	if len(values) == 0 {
		return nil
	}
	table, err := mmdsRouteSecretOwnerTable(kind)
	if err != nil {
		return err
	}
	const revision int64 = 1
	ciphertext, err := s.encryptMMDSRouteSecretValues(kind, ownerID, routesDigest, revision, values)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO %s (%s,routes_digest,revision,ciphertext,updated_unix) VALUES (?,?,?,?,?)`, table.table, table.idCol)
	if _, err := tx.ExecContext(ctx, query, ownerID, routesDigest, revision, ciphertext, time.Now().Unix()); err != nil {
		return fmt.Errorf("store: insert MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	return nil
}

// InsertSandboxWithMMDSRouteSecretValues atomically creates the sandbox row,
// routes-only metadata already held by sb, and its optional encrypted initial
// values. No row from either half remains if the transaction fails.
func (s *Store) InsertSandboxWithMMDSRouteSecretValues(ctx context.Context, sb *types.Sandbox, routesDigest string, values MMDSRouteSecretValues) error {
	args, err := s.prepareSandboxInsert(sb)
	if err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, sandboxInsertOnlySQL, args...)
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
	if err := s.insertInitialMMDSRouteSecretValuesTx(ctx, tx, MMDSRouteSecretOwnerSandbox, sb.ID, routesDigest, values); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return sandboxWriteError("insert sandbox", sb, err)
	}
	return nil
}

// InsertBuildWithMMDSRouteSecretValues is the Build Register equivalent of the
// sandbox transaction primitive.
func (s *Store) InsertBuildWithMMDSRouteSecretValues(ctx context.Context, build *types.Build, routesDigest string, values MMDSRouteSecretValues) error {
	if err := prepareBuildRegistrationMMDSIdentity(build, routesDigest, values); err != nil {
		return err
	}
	args, err := s.prepareBuildWrite(build)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: insert build %s: %w", build.BuildID, err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, buildInsertOnlySQL, args...)
	if err != nil {
		return fmt.Errorf("store: insert build %s: %w", build.BuildID, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted != 1 {
		return fmt.Errorf("store: insert build %s: build already exists", build.BuildID)
	}
	if err := s.insertInitialMMDSRouteSecretValuesTx(ctx, tx, MMDSRouteSecretOwnerBuild, build.BuildID, routesDigest, values); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: insert build %s: %w", build.BuildID, err)
	}
	return nil
}

var (
	ErrBuildRegistrationConflict = errors.New("build registration conflicts with existing immutable definition")
	ErrBuildRegistrationCapacity = errors.New("build registration admission capacity exceeded")
)

// RegisterBuildWithMMDSRouteSecretValues atomically checks registration
// headroom, inserts the complete immutable Build definition, and creates its
// optional confidential MMDS values. An exact BuildID replay returns the
// original row without consuming capacity again.
func (s *Store) RegisterBuildWithMMDSRouteSecretValues(ctx context.Context, build *types.Build, limit types.BuildAdmissionLimit, routesDigest string, values MMDSRouteSecretValues) (*types.Build, bool, error) {
	if err := prepareBuildRegistrationMMDSIdentity(build, routesDigest, values); err != nil {
		return nil, false, err
	}
	args, err := s.prepareBuildWrite(build)
	if err != nil {
		return nil, false, err
	}
	if err := build.Resources.ValidateRequired(); err != nil {
		return nil, false, fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	defer tx.Rollback()
	existing, err := scanBuildTx(s, tx.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE build_id=?`, build.BuildID))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, fmt.Errorf("store: register build %s lookup: %w", build.BuildID, err)
	}
	if err == nil {
		sameMMDS := true
		exactOriginalRequest := existing.RegistrationRequestDigest != "" &&
			existing.RegistrationRequestDigest == build.RegistrationRequestDigest
		if !exactOriginalRequest && existing.Status != types.BuildReady && existing.Status != types.BuildError {
			var compareErr error
			sameMMDS, compareErr = s.sameInitialBuildMMDSRouteSecretValuesTx(ctx, tx, build.BuildID, routesDigest, values)
			if compareErr != nil {
				return nil, false, compareErr
			}
		}
		if !sameImmutableBuild(existing, build) || !sameMMDS {
			return nil, false, fmt.Errorf("%w: %s", ErrBuildRegistrationConflict, build.BuildID)
		}
		return existing, false, nil
	}
	// Limits govern new ownership only. An exact retry of an existing durable
	// definition must return its original result even if operator configuration
	// was tightened after the first ACCEPTED response (or that response was
	// lost). The immutable comparison above still rejects a different request.
	if !limit.AllowsOne(build.Resources) {
		return nil, false, fmt.Errorf("%w: build %s cannot fit configured registration limits", ErrBuildRegistrationCapacity, build.BuildID)
	}

	// INSERT ... SELECT evaluates the aggregate predicates while holding the
	// SQLite writer lock. Concurrent registrations therefore cannot both pass a
	// stale pre-check and oversubscribe the durable ledger.
	insertSelect := strings.Replace(buildInsertSQL, "\n\tVALUES (", "\n\tSELECT ", 1)
	insertSelect = strings.TrimSuffix(insertSelect, ")") + `
	WHERE (?=0 OR (SELECT COUNT(*) FROM builds WHERE status IN ('registered','waiting','building') AND cancel_requested_unix=0 AND delete_requested_unix=0) <= ?)
	  AND (?=0 OR COALESCE((SELECT SUM(resources_cpu) FROM builds WHERE status IN ('registered','waiting','building') AND cancel_requested_unix=0 AND delete_requested_unix=0),0) <= ?)
	  AND (?=0 OR COALESCE((SELECT SUM(resources_memory) FROM builds WHERE status IN ('registered','waiting','building') AND cancel_requested_unix=0 AND delete_requested_unix=0),0) <= ?)
	  AND (?=0 OR COALESCE((SELECT SUM(resources_storage) FROM builds WHERE status IN ('registered','waiting','building') AND cancel_requested_unix=0 AND delete_requested_unix=0),0) <= ?)`
	headroom := func(configured, requested int64) int64 {
		if configured == 0 {
			return 0
		}
		return configured - requested
	}
	args = append(args,
		limit.MaxBuilds, headroom(limit.MaxBuilds, 1),
		limit.Resources.CPU, headroom(limit.Resources.CPU, build.Resources.CPU),
		limit.Resources.Memory, headroom(limit.Resources.Memory, build.Resources.Memory),
		limit.Resources.Storage, headroom(limit.Resources.Storage, build.Resources.Storage),
	)
	result, err := tx.ExecContext(ctx, insertSelect, args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	if inserted == 0 {
		return nil, false, fmt.Errorf("%w: build %s", ErrBuildRegistrationCapacity, build.BuildID)
	}
	if err := s.insertInitialMMDSRouteSecretValuesTx(ctx, tx, MMDSRouteSecretOwnerBuild, build.BuildID, routesDigest, values); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("store: register build %s: %w", build.BuildID, err)
	}
	return build, true, nil
}

func (s *Store) sameInitialBuildMMDSRouteSecretValuesTx(ctx context.Context, tx *sql.Tx, buildID, routesDigest string, values MMDSRouteSecretValues) (bool, error) {
	var storedDigest string
	var revision int64
	var ciphertext string
	err := tx.QueryRowContext(ctx, `SELECT routes_digest,revision,ciphertext FROM build_mmds_route_secret_values WHERE build_id=?`, buildID).
		Scan(&storedDigest, &revision, &ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return len(values) == 0, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: compare build %s initial MMDS values: %w", buildID, err)
	}
	if storedDigest != routesDigest || len(values) == 0 {
		return false, nil
	}
	stored, err := s.decryptMMDSRouteSecretValues(MMDSRouteSecretOwnerBuild, buildID, storedDigest, revision, ciphertext)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(stored, values), nil
}

func scanBuildTx(s *Store, row *sql.Row) (*types.Build, error) { return s.scanBuild(row) }

func sameImmutableBuild(a, b *types.Build) bool {
	if a == nil || b == nil {
		return false
	}
	// A cluster registration Extension may modify its credential-free mutable
	// candidate. Replay cannot invoke that Hook again: policy may have changed
	// after an ACK was lost. Its tenant-keyed original-request digest therefore
	// becomes the immutable comparison authority while core IDs and credentials
	// are still checked independently. Every cluster row must use this schema;
	// direct registrations keep the field-by-field comparison below.
	if a.ClusterGroup != "" || b.ClusterGroup != "" {
		return a.BuildID == b.BuildID && a.TemplateID == b.TemplateID &&
			a.ClusterGroup != "" && a.ClusterGroup == b.ClusterGroup &&
			a.RegistrationRequestDigest != "" && a.RegistrationRequestDigest == b.RegistrationRequestDigest &&
			hmac.Equal([]byte(a.APISecret), []byte(b.APISecret)) && hmac.Equal([]byte(a.ManifestKey), []byte(b.ManifestKey))
	}
	// Trigger intentionally mutates work-order fields; finalization alone sets
	// kind and mutates names/aliases. A delayed retry of the original registration
	// must therefore compare only the registration-owned definition. These
	// fields remain immutable for the lifetime of the Build row.
	return a.BuildID == b.BuildID && a.TemplateID == b.TemplateID && a.Profile == b.Profile &&
		a.Resources == b.Resources && a.ClusterGroup == b.ClusterGroup &&
		a.RegistrationImageRepo == b.RegistrationImageRepo &&
		hmac.Equal([]byte(a.RegistrationRegistryAuth), []byte(b.RegistrationRegistryAuth)) &&
		a.RegistrationMMDSRoutesDigest == b.RegistrationMMDSRoutesDigest &&
		hmac.Equal([]byte(a.RegistrationMMDSValuesDigest), []byte(b.RegistrationMMDSValuesDigest)) &&
		equalRegistrationMetadata(a, b) &&
		maps.Equal(a.Env, b.Env) && a.Secure == b.Secure &&
		a.ServiceSecret == b.ServiceSecret && a.EnvdAccessToken == b.EnvdAccessToken &&
		a.TrafficAccessToken == b.TrafficAccessToken &&
		reflect.DeepEqual(a.Builder, b.Builder) &&
		hmac.Equal([]byte(a.APISecret), []byte(b.APISecret)) && hmac.Equal([]byte(a.ManifestKey), []byte(b.ManifestKey))
}

func equalRegistrationMetadata(a, b *types.Build) bool {
	// Terminal persistence deliberately removes builder-only MMDS routes from
	// portable template metadata. Cluster registration retains their immutable
	// digest separately, so an exact delayed replay can still prove identity
	// without retaining those routes or their confidential values indefinitely.
	if a.RegistrationMMDSRoutesDigest == "" && b.RegistrationMMDSRoutesDigest == "" {
		return equalEmptyCollections(a.Metadata, b.Metadata)
	}
	withoutMMDS := func(in map[string]string) map[string]string {
		if _, present := in[sandboxcfg.NsMMDS]; !present {
			return in
		}
		out := make(map[string]string, len(in)-1)
		for key, value := range in {
			if key != sandboxcfg.NsMMDS {
				out[key] = value
			}
		}
		return out
	}
	return equalEmptyCollections(withoutMMDS(a.Metadata), withoutMMDS(b.Metadata))
}

func terminalBuildMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	terminal := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if key != sandboxcfg.NsMMDS {
			terminal[key] = value
		}
	}
	return terminal
}

func terminalBuildMetadataJSON(raw string) (string, error) {
	var metadata map[string]string
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return "", fmt.Errorf("decode build metadata: %w", err)
	}
	return mj(terminalBuildMetadata(metadata)), nil
}

func equalEmptyCollections(a, b any) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.IsValid() && bv.IsValid() && (av.Kind() == reflect.Map || av.Kind() == reflect.Slice) &&
		av.Kind() == bv.Kind() && av.Len() == 0 && bv.Len() == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

type rawMMDSRouteSecretRow struct {
	routesDigest string
	revision     int64
	ciphertext   string
	found        bool
}

func (s *Store) rawMMDSRouteSecretValues(ctx context.Context, kind MMDSRouteSecretOwnerKind, ownerID string) (rawMMDSRouteSecretRow, error) {
	table, err := mmdsRouteSecretOwnerTable(kind)
	if err != nil {
		return rawMMDSRouteSecretRow{}, err
	}
	query := fmt.Sprintf(`SELECT routes_digest,revision,ciphertext FROM %s WHERE %s=?`, table.table, table.idCol)
	var row rawMMDSRouteSecretRow
	err = s.db.QueryRowContext(ctx, query, ownerID).Scan(&row.routesDigest, &row.revision, &row.ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return rawMMDSRouteSecretRow{}, nil
	}
	if err != nil {
		return rawMMDSRouteSecretRow{}, fmt.Errorf("store: read MMDS route secret values for %s %s: %w", kind, ownerID, err)
	}
	row.found = true
	return row, nil
}

// GetMMDSRouteSecretValues decrypts an owner's value set. expectedRoutesDigest
// is compared with the row before decryption and is also used as AAD; callers
// therefore cannot silently read a blob substituted from another route config.
func (s *Store) GetMMDSRouteSecretValues(ctx context.Context, kind MMDSRouteSecretOwnerKind, ownerID, expectedRoutesDigest string) (MMDSRouteSecretValues, int64, bool, error) {
	row, err := s.rawMMDSRouteSecretValues(ctx, kind, ownerID)
	if err != nil || !row.found {
		return nil, 0, false, err
	}
	if row.routesDigest != expectedRoutesDigest {
		return nil, row.revision, true, fmt.Errorf("store: MMDS route digest mismatch for %s %s", kind, ownerID)
	}
	values, err := s.decryptMMDSRouteSecretValues(kind, ownerID, expectedRoutesDigest, row.revision, row.ciphertext)
	if err != nil {
		return nil, row.revision, true, err
	}
	return cloneMMDSRouteSecretValues(values), row.revision, true, nil
}

func (s *Store) mutateMMDSRouteSecretValues(ctx context.Context, kind MMDSRouteSecretOwnerKind, ownerID, routesDigest string, mutate func(MMDSRouteSecretValues) bool) (int64, error) {
	table, err := mmdsRouteSecretOwnerTable(kind)
	if err != nil {
		return 0, err
	}
	const maxAttempts = 32
	for attempt := 0; attempt < maxAttempts; attempt++ {
		row, err := s.rawMMDSRouteSecretValues(ctx, kind, ownerID)
		if err != nil {
			return 0, err
		}
		values := MMDSRouteSecretValues{}
		if row.found {
			if row.routesDigest != routesDigest {
				return 0, fmt.Errorf("store: MMDS route digest mismatch for %s %s", kind, ownerID)
			}
			values, err = s.decryptMMDSRouteSecretValues(kind, ownerID, routesDigest, row.revision, row.ciphertext)
			if err != nil {
				return 0, err
			}
		}
		if !mutate(values) {
			return row.revision, nil
		}
		newRevision := row.revision + 1
		ciphertext, err := s.encryptMMDSRouteSecretValues(kind, ownerID, routesDigest, newRevision, values)
		if err != nil {
			return 0, err
		}
		query := fmt.Sprintf(`
INSERT INTO %s (%s,routes_digest,revision,ciphertext,updated_unix)
VALUES (?,?,?,?,?)
ON CONFLICT(%s) DO UPDATE SET
  routes_digest=excluded.routes_digest,
  revision=excluded.revision,
  ciphertext=excluded.ciphertext,
  updated_unix=excluded.updated_unix
WHERE %s.revision=? AND %s.routes_digest=?`,
			table.table, table.idCol, table.idCol, table.table, table.table)
		result, err := s.db.ExecContext(ctx, query,
			ownerID, routesDigest, newRevision, ciphertext, time.Now().Unix(), row.revision, routesDigest)
		if err != nil {
			return 0, fmt.Errorf("store: update MMDS route secret values for %s %s: %w", kind, ownerID, err)
		}
		if changed, _ := result.RowsAffected(); changed == 1 {
			return newRevision, nil
		}
	}
	return 0, fmt.Errorf("store: update MMDS route secret values for %s %s: too much contention", kind, ownerID)
}

func (s *Store) PutMMDSRouteSecretValue(ctx context.Context, kind MMDSRouteSecretOwnerKind, ownerID, routesDigest, name string, value []byte) (int64, error) {
	valueCopy := append([]byte(nil), value...)
	return s.mutateMMDSRouteSecretValues(ctx, kind, ownerID, routesDigest, func(values MMDSRouteSecretValues) bool {
		values[name] = valueCopy
		return true
	})
}

func (s *Store) DeleteMMDSRouteSecretValue(ctx context.Context, kind MMDSRouteSecretOwnerKind, ownerID, routesDigest, name string) (int64, error) {
	return s.mutateMMDSRouteSecretValues(ctx, kind, ownerID, routesDigest, func(values MMDSRouteSecretValues) bool {
		if _, present := values[name]; !present {
			return false
		}
		delete(values, name)
		return true
	})
}

// DeleteBuildMMDSRouteSecretValues is the explicit cleanup path for a build
// that is discarded before a terminal record update.
func (s *Store) DeleteBuildMMDSRouteSecretValues(ctx context.Context, buildID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM build_mmds_route_secret_values WHERE build_id=?`, buildID)
	if err != nil {
		return fmt.Errorf("store: delete build MMDS route secret values for %s: %w", buildID, err)
	}
	return nil
}

// PutBuildTerminal atomically persists a build's terminal state and removes
// its builder-only execution ownership, MMDS routes, and confidential values.
// The build row remains as retention-bounded status/index history, but no runner
// or builder input can survive as terminal ownership or template metadata.
func (s *Store) PutBuildTerminal(ctx context.Context, build *types.Build) error {
	if build == nil {
		return errors.New("build is required")
	}
	terminal := *build
	terminal.Metadata = terminalBuildMetadata(build.Metadata)
	if terminal.Status != types.BuildReady && terminal.Status != types.BuildError {
		return fmt.Errorf("store: finish build %s: status %s is not terminal", build.BuildID, terminal.Status)
	}
	if terminal.RuntimeVswitchPort != "" || terminal.RuntimeFloatingIP != "" || terminal.RuntimePortMAC != "" ||
		terminal.RuntimeEnvdAccessToken != "" || terminal.RuntimePrepareJSON != "" {
		return fmt.Errorf("store: finish build %s: runtime ownership cleanup is incomplete", build.BuildID)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	defer tx.Rollback()
	current, err := s.scanBuild(tx.QueryRowContext(ctx, `SELECT `+buildCols+` FROM builds WHERE build_id=? AND template_id=? AND run_id=?`, build.BuildID, build.TemplateID, build.RunID))
	if err != nil {
		return fmt.Errorf("store: finish build identity: %w", err)
	}
	terminal.CancelRequestedUnix, terminal.DeleteRequestedUnix = current.CancelRequestedUnix, current.DeleteRequestedUnix
	if current.CancelRequestedUnix != 0 && current.ExecutionResult == nil {
		terminal.Status, terminal.Reason = types.BuildError, BuildCancelledReason
		terminal.PersistID, terminal.Kind = current.PersistID, current.Kind
		terminal.Names, terminal.Aliases = current.Names, current.Aliases
	}
	result, err := tx.ExecContext(ctx, `UPDATE builds SET
		persist_id=?,kind=?,start_cmd=?,ready_cmd=?,status=?,reason=?,run_id='',
		names_json=?,aliases_json=?,metadata_json=?,execution_claimed=0,
		execution_claimed_unix=0,enforcement_status='',phase='',phase_sandbox_id='',
		runtime_vswitch_port='',runtime_floating_ip='',runtime_port_mac='',runtime_envd_access_token_enc='',runtime_prepare_json='',
		execution_result_json='',finished_unix=unixepoch()
		WHERE build_id=? AND template_id=? AND run_id=? AND status=? AND execution_claimed=1
		  AND runtime_vswitch_port='' AND runtime_floating_ip='' AND runtime_port_mac=''
		  AND runtime_envd_access_token_enc='' AND runtime_prepare_json=''`,
		terminal.PersistID, string(terminal.Kind), terminal.StartCmd, terminal.ReadyCmd,
		string(terminal.Status), terminal.Reason, mjs(terminal.Names), mjs(terminal.Aliases),
		mj(terminal.Metadata), terminal.BuildID, terminal.TemplateID, terminal.RunID, string(types.BuildBuilding))
	if err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("store: finish build %s: execution ownership is not clean or was lost", build.BuildID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM build_mmds_route_secret_values WHERE build_id=?`, build.BuildID); err != nil {
		return fmt.Errorf("store: finish build %s MMDS cleanup: %w", build.BuildID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	*build = terminal
	return nil
}
