package store

// This file supports the pre-cutover runtime while the dormant final node
// components are stacked. The atomic Phase 5 cutover deletes this storage and
// every caller; new code must use key_leases.

import (
	"context"
	"fmt"
	"time"
)

type ManifestKeyInfo struct {
	Hash        string
	Label       string
	CreatedUnix int64
	ExpiresUnix int64
}

func (s *Store) AddManifestKey(ctx context.Context, key, label string, ttlSec int64, registryAuth string) (bool, error) {
	hash, enc, err := s.encKeyField("ManifestKey", key)
	if err != nil {
		return false, err
	}
	expires := int64(0)
	if ttlSec > 0 {
		expires = time.Now().Unix() + ttlSec
	}
	registryAuthEnc := ""
	if registryAuth != "" {
		registryAuthEnc, err = s.box.EncryptString(registryAuth)
		if err != nil {
			return false, err
		}
	}
	rowID, found, err := s.findManifestKeyRow(ctx, key, hash)
	if err != nil {
		return false, err
	}
	if found {
		_, err = s.db.ExecContext(ctx, `
UPDATE manifest_keys SET expires_unix=?,
  label=CASE WHEN ?='' THEN label ELSE ? END,
  registry_auth_enc=CASE WHEN ?='' THEN registry_auth_enc ELSE ? END
WHERE rowid=?`, expires, label, label, registryAuthEnc, registryAuthEnc, rowID)
		return false, err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO manifest_keys (key_hash,key_enc,label,created_unix,expires_unix,registry_auth_enc)
VALUES (?,?,?,?,?,?)`, hash, enc, label, time.Now().Unix(), expires, registryAuthEnc)
	if err != nil {
		return false, fmt.Errorf("store: add legacy manifest key: %w", err)
	}
	return true, nil
}

func (s *Store) RegistryAuthForKey(ctx context.Context, key string) (string, error) {
	hash, err := ManifestKeyHash(key)
	if err != nil {
		return "", err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT key_enc,registry_auth_enc FROM manifest_keys WHERE key_hash=?`, hash)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var keyEnc, registryAuthEnc string
		if err := rows.Scan(&keyEnc, &registryAuthEnc); err != nil {
			return "", err
		}
		stored, err := s.box.DecryptString(keyEnc)
		if err != nil || stored != key {
			continue
		}
		if registryAuthEnc == "" {
			return "", nil
		}
		return s.box.DecryptString(registryAuthEnc)
	}
	return "", rows.Err()
}

func (s *Store) HasManifestKey(ctx context.Context, key string) (bool, error) {
	hash, err := ManifestKeyHash(key)
	if err != nil {
		return false, err
	}
	keys, err := s.AllowedManifestKeysByHash(ctx, hash)
	if err != nil {
		return false, err
	}
	for _, candidate := range keys {
		if candidate == key {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) RemoveManifestKey(ctx context.Context, key string) (int, error) {
	hash, err := ManifestKeyHash(key)
	if err != nil {
		return 0, err
	}
	rowID, found, err := s.findManifestKeyRow(ctx, key, hash)
	if err != nil || !found {
		return 0, err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM manifest_keys WHERE rowid=?`, rowID)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return int(count), nil
}

func (s *Store) ListManifestKeys(ctx context.Context) ([]ManifestKeyInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key_hash,label,created_unix,expires_unix FROM manifest_keys ORDER BY created_unix`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ManifestKeyInfo
	for rows.Next() {
		var info ManifestKeyInfo
		if err := rows.Scan(&info.Hash, &info.Label, &info.CreatedUnix, &info.ExpiresUnix); err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, rows.Err()
}

func (s *Store) AllowedManifestKeysByHash(ctx context.Context, hash string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT key_enc FROM manifest_keys WHERE key_hash=? AND (expires_unix=0 OR expires_unix>?)`,
		hash, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var encrypted string
		if err := rows.Scan(&encrypted); err != nil {
			return nil, err
		}
		key, err := s.box.DecryptString(encrypted)
		if err != nil {
			return nil, err
		}
		result = append(result, key)
	}
	return result, rows.Err()
}

func (s *Store) PruneExpiredManifestKeys(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM manifest_keys WHERE expires_unix>0 AND expires_unix<=?`, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	return count, nil
}

func (s *Store) findManifestKeyRow(ctx context.Context, key, hash string) (int64, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rowid,key_enc FROM manifest_keys WHERE key_hash=?`, hash)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var rowID int64
		var encrypted string
		if err := rows.Scan(&rowID, &encrypted); err != nil {
			return 0, false, err
		}
		stored, err := s.box.DecryptString(encrypted)
		if err != nil {
			return 0, false, err
		}
		if stored == key {
			return rowID, true, nil
		}
	}
	return 0, false, rows.Err()
}
