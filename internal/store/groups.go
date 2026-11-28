package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

type groupRow struct {
	name                                                 string
	revision                                             int64
	active                                               bool
	owner, apiEnc, manifestHash, manifestEnc, contextEnc string
}

func readGroup(ctx context.Context, tx *sql.Tx, name string) (groupRow, error) {
	r := groupRow{name: name}
	err := tx.QueryRowContext(ctx, `SELECT revision,active,owner_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,context_enc FROM node_groups WHERE name=?`, name).
		Scan(&r.revision, &r.active, &r.owner, &r.apiEnc, &r.manifestHash, &r.manifestEnc, &r.contextEnc)
	if err == sql.ErrNoRows {
		return r, nil
	}
	return r, err
}

func groupKey(apiKey, name string) (*apikey.Parsed, error) {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return nil, nodegroup.ErrUnauthorized
	}
	if err := nodegroup.ValidateName(name); err != nil {
		return nil, err
	}
	return p, nil
}

func groupVerify(p *apikey.Parsed, pair KeyPair) bool {
	raw, err := hex.DecodeString(pair.APISecret)
	if err != nil || len(raw) != 32 {
		return false
	}
	defer clear(raw)
	return apikey.Verify(p, raw)
}

// groupAllowedTx loads the retained pair and verifies the full API token MAC.
// The short token fingerprint is only a candidate index, never ownership.
func (s *Store) groupAllowedTx(ctx context.Context, tx *sql.Tx, p *apikey.Parsed) (KeyPair, error) {
	rows, err := tx.QueryContext(ctx, `SELECT api_secret_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM manifest_keys
WHERE substr(api_secret_hash,1,24)=? AND (expires_unix=0 OR expires_unix>?)`, hex.EncodeToString(p.CandidateFingerprint), time.Now().Unix())
	if err != nil {
		return KeyPair{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var owner, apiEnc, mkHash, mkEnc string
		if err := rows.Scan(&owner, &apiEnc, &mkHash, &mkEnc); err != nil {
			return KeyPair{}, err
		}
		pair, err := s.decryptVerifiedKeyPair(owner, apiEnc, mkHash, mkEnc)
		if err != nil {
			return KeyPair{}, err
		}
		if groupVerify(p, pair) {
			return pair, nil
		}
	}
	return KeyPair{}, rows.Err()
}

func (s *Store) groupOwner(r groupRow, p *apikey.Parsed) (KeyPair, error) {
	pair, err := s.decryptVerifiedKeyPair(r.owner, r.apiEnc, r.manifestHash, r.manifestEnc)
	if err != nil {
		return KeyPair{}, err
	}
	if !groupVerify(p, pair) {
		return KeyPair{}, nodegroup.ErrNotFound
	}
	return pair, nil
}

func (s *Store) groupDetail(r groupRow) (nodegroup.Detail, error) {
	raw, err := s.box.DecryptString(r.contextEnc)
	if err != nil {
		return nodegroup.Detail{}, fmt.Errorf("store: decrypt Group context: %w", err)
	}
	out := nodegroup.Detail{Group: r.name, Version: r.revision}
	if err := json.Unmarshal([]byte(raw), &out.Context); err != nil {
		return nodegroup.Detail{}, err
	}
	return out, nil
}

// GroupGet authenticates against retained roots for an active object. Deleted
// objects keep only owner fingerprints and require a current allowed pair.
func (s *Store) GroupGet(ctx context.Context, apiKey, name string) (nodegroup.Detail, error) {
	p, err := groupKey(apiKey, name)
	if err != nil {
		return nodegroup.Detail{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Detail{}, err
	}
	defer tx.Rollback()
	r, err := readGroup(ctx, tx, name)
	if err != nil {
		return nodegroup.Detail{}, err
	}
	if r.active {
		if _, err := s.groupOwner(r, p); err != nil {
			return nodegroup.Detail{}, err
		}
		return s.groupDetail(r)
	}
	pair, err := s.groupAllowedTx(ctx, tx, p)
	if err != nil {
		return nodegroup.Detail{}, err
	}
	if pair.APISecret == "" {
		if r.revision == 0 {
			return nodegroup.Detail{}, nodegroup.ErrForbidden
		}
		return nodegroup.Detail{}, nodegroup.ErrNotFound
	}
	owner, _ := APISecretHash(pair.APISecret)
	if r.revision != 0 && owner != r.owner {
		return nodegroup.Detail{}, nodegroup.ErrNotFound
	}
	return nodegroup.Detail{}, &nodegroup.VersionError{Kind: nodegroup.ErrNotFound, Version: r.revision}
}

// GroupPut is the one conditional-write core for HTTP/UDS and trusted NodeLink.
// A nonnil keyPut is authorized only by the trusted transport adapter. No pair
// change commits if authentication, binding, CAS, validation or the write fails.
func (s *Store) GroupPut(ctx context.Context, apiKey, name string, expected int64, desired nodegroup.Context, keyPut *nodegroup.KeyPut) (nodegroup.Result, bool, error) {
	p, err := groupKey(apiKey, name)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	if expected < 0 {
		return nodegroup.Result{}, false, nodegroup.ErrInvalid
	}
	normalized, raw, err := nodegroup.Normalize(desired)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	var supplied KeyPair
	if keyPut != nil {
		if keyPut.ExpiresUnix < 0 || (keyPut.ExpiresUnix != 0 && keyPut.ExpiresUnix <= time.Now().Unix()) {
			return nodegroup.Result{}, false, fmt.Errorf("%w: key_put has expired", nodegroup.ErrInvalid)
		}
		supplied = KeyPair{APISecret: keyPut.APISecret, ManifestKey: keyPut.ManifestKey, ManifestKeyFingerprint: keyPut.ManifestKeyFingerprint}
		supplied, err = ResolveKeyPair(ctx, supplied)
		if err != nil {
			return nodegroup.Result{}, false, fmt.Errorf("%w: invalid key_put pair", nodegroup.ErrInvalid)
		}
		if !groupVerify(p, supplied) {
			return nodegroup.Result{}, false, nodegroup.ErrForbidden
		}
		// Keep the receiver's verified projection available to its caller;
		// mirroring an accepted command must not resolve the root a second time.
		keyPut.ManifestKeyFingerprint = supplied.ManifestKeyFingerprint
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	defer tx.Rollback()
	r, err := readGroup(ctx, tx, name)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	var pair KeyPair
	carrierChanged := false
	if r.active {
		pair, err = s.groupOwner(r, p)
		if err != nil {
			return nodegroup.Result{}, false, err
		}
		if keyPut != nil {
			if !equalKeyPair(pair, supplied) {
				return nodegroup.Result{}, false, ErrKeyPairConflict
			}
			carrierChanged = pair.ManifestKey != supplied.ManifestKey
			pair = supplied
		}
	} else {
		pair = supplied
		if keyPut == nil {
			pair, err = s.groupAllowedTx(ctx, tx, p)
			if err != nil {
				return nodegroup.Result{}, false, err
			}
		}
		if pair.APISecret == "" {
			return nodegroup.Result{}, false, nodegroup.ErrForbidden
		}
		owner, _ := APISecretHash(pair.APISecret)
		if r.revision != 0 && owner != r.owner {
			return nodegroup.Result{}, false, nodegroup.ErrNotFound
		}
	}
	if r.revision != expected {
		return nodegroup.Result{}, false, &nodegroup.VersionError{Kind: nodegroup.ErrConflict, Version: r.revision}
	}
	owner, _ := APISecretHash(pair.APISecret)
	if err := nodegroup.ValidateSupportBudget(normalized.Support(name, math.MaxInt64, owner, pair.APISecret)); err != nil {
		return nodegroup.Result{}, false, err
	}
	if keyPut != nil {
		// Recheck after acquiring SQLite's writer lock; the absolute lease may
		// have expired while this request waited for another transaction.
		if keyPut.ExpiresUnix != 0 && keyPut.ExpiresUnix <= time.Now().Unix() {
			return nodegroup.Result{}, false, fmt.Errorf("%w: key_put has expired", nodegroup.ErrInvalid)
		}
		if _, err := s.addKeyPairTx(ctx, tx, pair, "", keyPut.ExpiresUnix, "", true); err != nil {
			return nodegroup.Result{}, false, err
		}
	}
	if r.active {
		previous, err := s.box.DecryptString(r.contextEnc)
		if err != nil {
			return nodegroup.Result{}, false, err
		}
		if previous == string(raw) && !carrierChanged {
			if err := tx.Commit(); err != nil {
				return nodegroup.Result{}, false, err
			}
			return nodegroup.Result{Group: name, Version: r.revision}, false, nil
		}
	}
	apiHash, apiEnc, err := s.encSecret("API secret", pair.APISecret)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	mkHash, mkEnc, err := s.encManifestKey(pair.ManifestKey, pair.ManifestKeyFingerprint)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	contextEnc, err := s.box.EncryptString(string(raw))
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	revision, err := nextGroupRevision(ctx, tx)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_groups(name,revision,active,owner_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,context_enc)
VALUES(?,?,1,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET revision=excluded.revision,active=1,owner_hash=excluded.owner_hash,
api_secret_enc=excluded.api_secret_enc,manifest_key_hash=excluded.manifest_key_hash,manifest_key_enc=excluded.manifest_key_enc,context_enc=excluded.context_enc`, name, revision, apiHash, apiEnc, mkHash, mkEnc, contextEnc)
	if err != nil {
		return nodegroup.Result{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return nodegroup.Result{}, false, err
	}
	return nodegroup.Result{Group: name, Version: revision}, !r.active, nil
}

func nextGroupRevision(ctx context.Context, tx *sql.Tx) (int64, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `UPDATE node_group_sequence SET revision=revision+1 WHERE id=1 AND revision<? RETURNING revision`, int64(math.MaxInt64)).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("store: Group revision exhausted")
	}
	return revision, err
}

func (s *Store) GroupDelete(ctx context.Context, apiKey, name string, expected int64) (nodegroup.Result, error) {
	p, err := groupKey(apiKey, name)
	if err != nil {
		return nodegroup.Result{}, err
	}
	if expected < 0 {
		return nodegroup.Result{}, nodegroup.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Result{}, err
	}
	defer tx.Rollback()
	r, err := readGroup(ctx, tx, name)
	if err != nil {
		return nodegroup.Result{}, err
	}
	if r.active {
		if _, err := s.groupOwner(r, p); err != nil {
			return nodegroup.Result{}, err
		}
	} else {
		pair, err := s.groupAllowedTx(ctx, tx, p)
		if err != nil {
			return nodegroup.Result{}, err
		}
		if pair.APISecret == "" {
			return nodegroup.Result{}, nodegroup.ErrNotFound
		}
		owner, _ := APISecretHash(pair.APISecret)
		if r.revision == 0 || owner != r.owner {
			return nodegroup.Result{}, nodegroup.ErrNotFound
		}
	}
	if r.revision != expected {
		return nodegroup.Result{}, &nodegroup.VersionError{Kind: nodegroup.ErrConflict, Version: r.revision}
	}
	if !r.active {
		return nodegroup.Result{Group: name, Version: r.revision}, nil
	}
	revision, err := nextGroupRevision(ctx, tx)
	if err != nil {
		return nodegroup.Result{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE node_groups SET revision=?,active=0,api_secret_enc='',manifest_key_hash='',manifest_key_enc='',context_enc='' WHERE name=?`, revision, name); err != nil {
		return nodegroup.Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return nodegroup.Result{}, err
	}
	return nodegroup.Result{Group: name, Version: revision}, nil
}

// GroupAdmit snapshots exactly one supported revision and checks current
// create/build admission without weakening the retained resource ownership rule.
func (s *Store) GroupAdmit(ctx context.Context, apiKey, name string, expected int64) (nodegroup.Detail, KeyPair, error) {
	p, err := groupKey(apiKey, name)
	if err != nil {
		return nodegroup.Detail{}, KeyPair{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Detail{}, KeyPair{}, err
	}
	defer tx.Rollback()
	r, err := readGroup(ctx, tx, name)
	if err != nil {
		return nodegroup.Detail{}, KeyPair{}, err
	}
	if !r.active {
		return nodegroup.Detail{}, KeyPair{}, nodegroup.ErrNotFound
	}
	pair, err := s.groupOwner(r, p)
	if err != nil {
		return nodegroup.Detail{}, KeyPair{}, err
	}
	allowed, err := s.groupAllowedTx(ctx, tx, p)
	if err != nil {
		return nodegroup.Detail{}, KeyPair{}, err
	}
	if !equalKeyPair(pair, allowed) {
		return nodegroup.Detail{}, KeyPair{}, nodegroup.ErrForbidden
	}
	if r.revision != expected {
		return nodegroup.Detail{}, KeyPair{}, &nodegroup.VersionError{Kind: nodegroup.ErrConflict, Version: r.revision}
	}
	detail, err := s.groupDetail(r)
	return detail, pair, err
}

type groupCursor struct {
	Owner string `json:"owner"`
	After string `json:"after"`
}

func (s *Store) GroupList(ctx context.Context, apiKey, cursor string, limit int) (nodegroup.Page, error) {
	p, err := apikey.Parse(apiKey)
	if err != nil {
		return nodegroup.Page{}, nodegroup.ErrUnauthorized
	}
	if limit == 0 {
		limit = nodegroup.MaxPageSize
	}
	if limit < 1 || limit > nodegroup.MaxPageSize {
		return nodegroup.Page{}, nodegroup.ErrInvalid
	}
	var next groupCursor
	if cursor != "" {
		// JSON may expand one name byte to six bytes (for example '<' to
		// \u003c), before raw base64 expands it again. The fixed allowance
		// covers the 64-byte owner fingerprint and JSON field syntax.
		if len(cursor) > base64.RawURLEncoding.EncodedLen(6*nodegroup.MaxNameBytes+128) {
			return nodegroup.Page{}, nodegroup.ErrInvalid
		}
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(raw, &next) != nil || nodegroup.ValidateName(next.After) != nil {
			return nodegroup.Page{}, nodegroup.ErrInvalid
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Page{}, err
	}
	defer tx.Rollback()
	// An existing active Group keeps management available after allowlist GC.
	rows, err := tx.QueryContext(ctx, `SELECT name,revision,owner_hash,api_secret_enc,manifest_key_hash,manifest_key_enc FROM node_groups WHERE active=1 AND substr(owner_hash,1,24)=?`, hex.EncodeToString(p.CandidateFingerprint))
	if err != nil {
		return nodegroup.Page{}, err
	}
	owner := ""
	for rows.Next() {
		var r groupRow
		if err := rows.Scan(&r.name, &r.revision, &r.owner, &r.apiEnc, &r.manifestHash, &r.manifestEnc); err != nil {
			rows.Close()
			return nodegroup.Page{}, err
		}
		if _, err := s.groupOwner(r, p); err == nil {
			owner = r.owner
			break
		} else if !errors.Is(err, nodegroup.ErrNotFound) {
			rows.Close()
			return nodegroup.Page{}, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nodegroup.Page{}, err
	}
	if owner == "" {
		pair, err := s.groupAllowedTx(ctx, tx, p)
		if err != nil {
			return nodegroup.Page{}, err
		}
		if pair.APISecret == "" {
			return nodegroup.Page{}, nodegroup.ErrForbidden
		}
		owner, _ = APISecretHash(pair.APISecret)
	}
	if cursor != "" && next.Owner != owner {
		return nodegroup.Page{}, nodegroup.ErrInvalid
	}
	rows, err = tx.QueryContext(ctx, `SELECT name,revision FROM node_groups WHERE active=1 AND owner_hash=? AND name>? ORDER BY name LIMIT ?`, owner, next.After, limit+1)
	if err != nil {
		return nodegroup.Page{}, err
	}
	defer rows.Close()
	out := nodegroup.Page{Groups: []nodegroup.Result{}}
	for rows.Next() {
		var item nodegroup.Result
		if err := rows.Scan(&item.Group, &item.Version); err != nil {
			return nodegroup.Page{}, err
		}
		if len(out.Groups) == limit {
			raw, _ := json.Marshal(groupCursor{Owner: owner, After: out.Groups[limit-1].Group})
			out.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		out.Groups = append(out.Groups, item)
	}
	return out, rows.Err()
}

// GroupSnapshot is an internal consistent catalog boundary. Its sequence is
// node-wide and unrelated to the shared engine's per-key revisions. No private
// default configuration or manifest root is projected into GroupSupport.
func (s *Store) GroupSupport(ctx context.Context, name string) (nodegroup.Support, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nodegroup.Support{}, err
	}
	defer tx.Rollback()
	r, err := readGroup(ctx, tx, name)
	if err != nil {
		return nodegroup.Support{}, err
	}
	if !r.active {
		return nodegroup.Support{}, nodegroup.ErrNotFound
	}
	pair, err := s.decryptVerifiedKeyPair(r.owner, r.apiEnc, r.manifestHash, r.manifestEnc)
	if err != nil {
		return nodegroup.Support{}, err
	}
	detail, err := s.groupDetail(r)
	if err != nil {
		return nodegroup.Support{}, err
	}
	return detail.Context.Support(r.name, r.revision, r.owner, pair.APISecret), nil
}

func (s *Store) GroupSnapshot(ctx context.Context) (int64, []nodegroup.Support, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM node_group_sequence WHERE id=1`).Scan(&seq); err != nil {
		return 0, nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT name,revision,owner_hash,api_secret_enc,manifest_key_hash,manifest_key_enc,context_enc FROM node_groups WHERE active=1 ORDER BY name`)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	out := []nodegroup.Support{}
	for rows.Next() {
		var r groupRow
		if err := rows.Scan(&r.name, &r.revision, &r.owner, &r.apiEnc, &r.manifestHash, &r.manifestEnc, &r.contextEnc); err != nil {
			return 0, nil, err
		}
		pair, err := s.decryptVerifiedKeyPair(r.owner, r.apiEnc, r.manifestHash, r.manifestEnc)
		if err != nil {
			return 0, nil, err
		}
		detail, err := s.groupDetail(r)
		if err != nil {
			return 0, nil, err
		}
		out = append(out, detail.Context.Support(r.name, r.revision, r.owner, pair.APISecret))
	}
	return seq, out, rows.Err()
}
