package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var (
	ErrKeyLeaseRevisionRegression = errors.New("store: key lease revision regressed")
	ErrKeyLeaseRevisionConflict   = errors.New("store: key lease revision conflicts with registry auth")
	ErrKeyLeaseExpiryRegression   = errors.New("store: key lease expiry regressed")
)

// KeyLease is the node-local, encrypted-at-rest bundle required before a new
// Sandbox or Build can copy its independent AuthKey and ManifestKey roots.
type KeyLease struct {
	Group              string
	AuthKey            string
	ManifestKey        string
	KeyRevision        uint64
	RegistryAuth       string
	RegistryAuthDigest string
	Label              string
	CreatedUnix        int64
	ExpiresUnix        int64 // 0 is allowed only for explicitly installed local leases
}

type KeyLeaseInfo struct {
	Group                  string `json:"group"`
	AuthKeyFingerprint     string `json:"auth_key_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	Label                  string `json:"label"`
	CreatedUnix            int64  `json:"created_unix"`
	ExpiresUnix            int64  `json:"expires_unix"`
}

func (s *Store) PutKeyLease(ctx context.Context, lease KeyLease) (bool, error) {
	if lease.Group == "" {
		return false, errors.New("store: key lease group is required")
	}
	if lease.KeyRevision == 0 {
		if lease.RegistryAuthDigest != "" {
			return false, errors.New("store: a local key lease cannot carry a registry auth digest")
		}
	} else {
		digest, err := hex.DecodeString(lease.RegistryAuthDigest)
		if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != lease.RegistryAuthDigest {
			return false, errors.New("store: a cluster key lease requires a canonical registry auth digest")
		}
		if lease.ExpiresUnix <= 0 {
			return false, errors.New("store: a cluster key lease requires a positive expiry")
		}
	}
	authHash, authEnc, err := s.encKeyField("AuthKey", lease.AuthKey)
	if err != nil {
		return false, err
	}
	manifestHash, manifestEnc, err := s.encKeyField("ManifestKey", lease.ManifestKey)
	if err != nil {
		return false, err
	}
	if authHash == manifestHash {
		return false, errors.New("store: AuthKey and ManifestKey must use separate key material")
	}
	registryAuthEnc := ""
	if lease.RegistryAuth != "" {
		registryAuthEnc, err = s.box.EncryptString(lease.RegistryAuth)
		if err != nil {
			return false, fmt.Errorf("store: encrypt registry auth: %w", err)
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, fmt.Errorf("store: begin key lease update: %w", err)
	}
	defer tx.Rollback()
	if err := advanceKeyLeaseRevisionFence(ctx, tx, lease, authHash, manifestHash); err != nil {
		return false, err
	}
	rowID, found, err := s.findKeyLeaseRowWith(ctx, tx, lease.Group, authHash, manifestHash, lease.AuthKey, lease.ManifestKey)
	if err != nil {
		return false, err
	}
	if found {
		var currentRevisionEncoded []byte
		var currentRegistryAuthDigest string
		var currentExpiresUnix int64
		if err := tx.QueryRowContext(ctx, `
SELECT key_revision,registry_auth_digest,expires_unix
FROM key_leases WHERE rowid=?`, rowID).Scan(
			&currentRevisionEncoded, &currentRegistryAuthDigest, &currentExpiresUnix,
		); err != nil {
			return false, err
		}
		currentRevision, err := decodeUint64(currentRevisionEncoded)
		if err != nil {
			return false, fmt.Errorf("store: decode key lease revision: %w", err)
		}
		if lease.KeyRevision < currentRevision {
			return false, ErrKeyLeaseRevisionRegression
		}
		if lease.KeyRevision > 0 && lease.KeyRevision == currentRevision {
			if lease.RegistryAuthDigest != currentRegistryAuthDigest {
				return false, ErrKeyLeaseRevisionConflict
			}
			if lease.ExpiresUnix < currentExpiresUnix {
				return false, ErrKeyLeaseExpiryRegression
			}
		}
		clusterLease := lease.KeyRevision > 0
		result, updateErr := tx.ExecContext(ctx, `
UPDATE key_leases
SET auth_key_enc=?, manifest_key_enc=?,
	key_revision=?, registry_auth_digest=?,
    registry_auth_enc=CASE WHEN ? THEN ? WHEN ?='' THEN registry_auth_enc ELSE ? END,
    expires_unix=?,
    label=CASE WHEN ?='' THEN label ELSE ? END
WHERE rowid=?`, authEnc, manifestEnc, encodeUint64(lease.KeyRevision), lease.RegistryAuthDigest,
			clusterLease, registryAuthEnc, registryAuthEnc, registryAuthEnc,
			lease.ExpiresUnix, lease.Label, lease.Label, rowID)
		if updateErr != nil {
			return false, updateErr
		}
		changed, updateErr := result.RowsAffected()
		if updateErr != nil {
			return false, updateErr
		}
		if changed != 1 {
			return false, errors.New("store: key lease disappeared during refresh")
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("store: commit key lease refresh: %w", err)
		}
		return false, nil
	}
	created := lease.CreatedUnix
	if created == 0 {
		created = time.Now().Unix()
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO key_leases
  (group_name,auth_key_hash,auth_key_enc,manifest_key_hash,manifest_key_enc,key_revision,registry_auth_digest,
   label,created_unix,expires_unix,registry_auth_enc)
VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		lease.Group, authHash, authEnc, manifestHash, manifestEnc, encodeUint64(lease.KeyRevision),
		lease.RegistryAuthDigest, lease.Label, created, lease.ExpiresUnix, registryAuthEnc)
	if err != nil {
		return false, fmt.Errorf("store: put key lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit key lease insert: %w", err)
	}
	return true, nil
}

func advanceKeyLeaseRevisionFence(
	ctx context.Context,
	tx *sql.Tx,
	lease KeyLease,
	authHash, manifestHash string,
) error {
	if lease.KeyRevision == 0 {
		return nil
	}
	var currentRevisionEncoded []byte
	var currentRegistryAuthDigest string
	var currentExpiresUnix int64
	err := tx.QueryRowContext(ctx, `
SELECT key_revision,registry_auth_digest,expires_unix
FROM key_lease_revision_fences
WHERE group_name=? AND auth_key_hash=? AND manifest_key_hash=?`,
		lease.Group, authHash, manifestHash,
	).Scan(&currentRevisionEncoded, &currentRegistryAuthDigest, &currentExpiresUnix)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `
INSERT INTO key_lease_revision_fences
  (group_name,auth_key_hash,manifest_key_hash,key_revision,registry_auth_digest,expires_unix)
VALUES (?,?,?,?,?,?)`, lease.Group, authHash, manifestHash, encodeUint64(lease.KeyRevision),
			lease.RegistryAuthDigest, lease.ExpiresUnix)
		return err
	}
	if err != nil {
		return err
	}
	currentRevision, err := decodeUint64(currentRevisionEncoded)
	if err != nil {
		return fmt.Errorf("store: decode key lease revision fence: %w", err)
	}
	if lease.KeyRevision < currentRevision {
		return ErrKeyLeaseRevisionRegression
	}
	if lease.KeyRevision == currentRevision {
		if lease.RegistryAuthDigest != currentRegistryAuthDigest {
			return ErrKeyLeaseRevisionConflict
		}
		if lease.ExpiresUnix < currentExpiresUnix {
			return ErrKeyLeaseExpiryRegression
		}
	}
	_, err = tx.ExecContext(ctx, `
UPDATE key_lease_revision_fences
SET key_revision=?,registry_auth_digest=?,expires_unix=?
WHERE group_name=? AND auth_key_hash=? AND manifest_key_hash=?`,
		encodeUint64(lease.KeyRevision), lease.RegistryAuthDigest, lease.ExpiresUnix,
		lease.Group, authHash, manifestHash)
	return err
}

func (s *Store) KeyLeasesByAuthHash(ctx context.Context, authHash string) ([]KeyLease, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT group_name,key_revision,registry_auth_digest,auth_key_enc,manifest_key_enc,registry_auth_enc,label,created_unix,expires_unix
FROM key_leases
WHERE auth_key_hash=? AND (expires_unix=0 OR expires_unix>?)
ORDER BY created_unix`, authHash, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leases []KeyLease
	for rows.Next() {
		lease, err := s.scanKeyLease(rows)
		if err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, rows.Err()
}

func (s *Store) KeyLeaseByFingerprints(
	ctx context.Context,
	group, authFingerprint, manifestFingerprint string,
) (KeyLease, bool, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT group_name,key_revision,registry_auth_digest,auth_key_enc,manifest_key_enc,registry_auth_enc,label,created_unix,expires_unix
FROM key_leases
WHERE group_name=? AND auth_key_hash=? AND manifest_key_hash=?
  AND (expires_unix=0 OR expires_unix>?)`,
		group, authFingerprint, manifestFingerprint, time.Now().Unix())
	if err != nil {
		return KeyLease{}, false, err
	}
	defer rows.Close()
	var result KeyLease
	found := false
	for rows.Next() {
		lease, err := s.scanKeyLease(rows)
		if err != nil {
			return KeyLease{}, false, err
		}
		if found {
			return KeyLease{}, false, errors.New("store: key lease fingerprint collision is ambiguous")
		}
		result, found = lease, true
	}
	return result, found, rows.Err()
}

func (s *Store) HasKeyLease(ctx context.Context, group, authKey, manifestKey string) (bool, error) {
	authHash, err := AuthKeyHash(authKey)
	if err != nil {
		return false, err
	}
	manifestHash, err := ManifestKeyHash(manifestKey)
	if err != nil {
		return false, err
	}
	rowID, found, err := s.findKeyLeaseRow(ctx, group, authHash, manifestHash, authKey, manifestKey)
	if err != nil || !found {
		return false, err
	}
	var expires int64
	if err := s.db.QueryRowContext(ctx, `SELECT expires_unix FROM key_leases WHERE rowid=?`, rowID).Scan(&expires); err != nil {
		return false, err
	}
	return expires == 0 || expires > time.Now().Unix(), nil
}

func (s *Store) RemoveKeyLease(ctx context.Context, group, authKey, manifestKey string) (bool, error) {
	authHash, err := AuthKeyHash(authKey)
	if err != nil {
		return false, err
	}
	manifestHash, err := ManifestKeyHash(manifestKey)
	if err != nil {
		return false, err
	}
	rowID, found, err := s.findKeyLeaseRow(ctx, group, authHash, manifestHash, authKey, manifestKey)
	if err != nil || !found {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM key_leases WHERE rowid=?`, rowID)
	if err != nil {
		return false, err
	}
	count, _ := result.RowsAffected()
	return count == 1, nil
}

func (s *Store) DropKeyLeaseRef(
	ctx context.Context,
	group, authFingerprint, manifestFingerprint string,
	keyRevision uint64,
	registryAuthDigest string,
) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
DELETE FROM key_leases
WHERE group_name=? AND auth_key_hash=? AND manifest_key_hash=?
  AND key_revision=? AND registry_auth_digest=?`,
		group, authFingerprint, manifestFingerprint, encodeUint64(keyRevision), registryAuthDigest)
	if err != nil {
		return false, err
	}
	count, _ := result.RowsAffected()
	return count > 0, nil
}

func (s *Store) ListKeyLeases(ctx context.Context) ([]KeyLeaseInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT group_name,auth_key_hash,manifest_key_hash,label,created_unix,expires_unix
FROM key_leases ORDER BY created_unix,group_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []KeyLeaseInfo
	for rows.Next() {
		var info KeyLeaseInfo
		if err := rows.Scan(
			&info.Group, &info.AuthKeyFingerprint, &info.ManifestKeyFingerprint,
			&info.Label, &info.CreatedUnix, &info.ExpiresUnix,
		); err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, rows.Err()
}

func (s *Store) PruneExpiredKeyLeases(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM key_leases WHERE expires_unix>0 AND expires_unix<=?`, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}

func (s *Store) findKeyLeaseRow(
	ctx context.Context,
	group, authHash, manifestHash, authKey, manifestKey string,
) (int64, bool, error) {
	return s.findKeyLeaseRowWith(ctx, s.db, group, authHash, manifestHash, authKey, manifestKey)
}

type keyLeaseQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Store) findKeyLeaseRowWith(
	ctx context.Context,
	queryer keyLeaseQueryer,
	group, authHash, manifestHash, authKey, manifestKey string,
) (int64, bool, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT rowid,auth_key_enc,manifest_key_enc
FROM key_leases WHERE group_name=? AND auth_key_hash=? AND manifest_key_hash=?`,
		group, authHash, manifestHash)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var rowID int64
		var authEnc, manifestEnc string
		if err := rows.Scan(&rowID, &authEnc, &manifestEnc); err != nil {
			return 0, false, err
		}
		storedAuth, authErr := s.box.DecryptString(authEnc)
		storedManifest, manifestErr := s.box.DecryptString(manifestEnc)
		if authErr != nil || manifestErr != nil {
			return 0, false, errors.New("store: decrypt key lease identity")
		}
		if storedAuth == authKey && storedManifest == manifestKey {
			return rowID, true, nil
		}
	}
	return 0, false, rows.Err()
}

func (s *Store) scanKeyLease(row interface{ Scan(...any) error }) (KeyLease, error) {
	var lease KeyLease
	var revisionEncoded []byte
	var authEnc, manifestEnc, registryAuthEnc string
	if err := row.Scan(
		&lease.Group, &revisionEncoded, &lease.RegistryAuthDigest, &authEnc, &manifestEnc, &registryAuthEnc,
		&lease.Label, &lease.CreatedUnix, &lease.ExpiresUnix,
	); err != nil {
		return KeyLease{}, err
	}
	var err error
	lease.KeyRevision, err = decodeUint64(revisionEncoded)
	if err != nil {
		return KeyLease{}, fmt.Errorf("store: decode key lease revision: %w", err)
	}
	lease.AuthKey, err = s.box.DecryptString(authEnc)
	if err != nil {
		return KeyLease{}, errors.New("store: decrypt key lease AuthKey")
	}
	lease.ManifestKey, err = s.box.DecryptString(manifestEnc)
	if err != nil {
		return KeyLease{}, errors.New("store: decrypt key lease ManifestKey")
	}
	if registryAuthEnc != "" {
		lease.RegistryAuth, err = s.box.DecryptString(registryAuthEnc)
		if err != nil {
			return KeyLease{}, errors.New("store: decrypt key lease registry auth")
		}
	}
	return lease, nil
}
