package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	if err != nil {
		return fmt.Errorf("store: insert build %s: %w", build.BuildID, err)
	}
	if inserted == 0 {
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
// its builder-only MMDS routes and confidential values. The build row remains
// as the template/status registry, but neither part of the builder MMDS input
// can become template metadata.
func (s *Store) PutBuildTerminal(ctx context.Context, build *types.Build) error {
	if build == nil {
		return errors.New("build is required")
	}
	terminal := *build
	if build.Metadata != nil {
		terminal.Metadata = make(map[string]string, len(build.Metadata))
		for key, value := range build.Metadata {
			if key != sandboxcfg.NsMMDS {
				terminal.Metadata[key] = value
			}
		}
	}
	args, err := s.prepareBuildWrite(&terminal)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, buildUpsertSQL, args...); err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM build_mmds_route_secret_values WHERE build_id=?`, build.BuildID); err != nil {
		return fmt.Errorf("store: finish build %s MMDS cleanup: %w", build.BuildID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: finish build %s: %w", build.BuildID, err)
	}
	return nil
}
