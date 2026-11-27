package store

import (
	"context"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
	"github.com/kuasar-sandbox/orchestrator/internal/manifestkey"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ResolveKeyPair validates manifest material at an ingestion boundary. The
// original text is retained for storage and task delivery. The private binding
// lets a caller derive defaults and perform a Store operation from one resolve.
func ResolveKeyPair(ctx context.Context, pair KeyPair) (KeyPair, error) {
	if !secret.IsReference(pair.ManifestKey) {
		// Registration keeps the existing native format. Generic SDK hex
		// decoding must not admit roots rejected by the persisted protocols.
		if _, err := manifestkey.KnownFingerprint(pair.ManifestKey, pair.ManifestKeyFingerprint); err != nil {
			return KeyPair{}, err
		}
	}
	if pair.manifestKeyResolved && pair.manifestKeySource == pair.ManifestKey {
		if manifestkey.Fingerprint(pair.manifestKeyBytes) != pair.ManifestKeyFingerprint {
			return KeyPair{}, fmt.Errorf("manifest key fingerprint mismatch")
		}
		return pair, nil
	}
	key, err := manifestkey.Resolve(ctx, pair.ManifestKey)
	if err != nil {
		return KeyPair{}, err
	}
	fp := manifestkey.Fingerprint(key)
	if pair.ManifestKeyFingerprint != "" && pair.ManifestKeyFingerprint != fp {
		clear(key[:])
		return KeyPair{}, fmt.Errorf("manifest key fingerprint mismatch")
	}
	pair.ManifestKeyFingerprint = fp
	pair.manifestKeyBytes = key
	pair.manifestKeySource = pair.ManifestKey
	pair.manifestKeyResolved = true
	clear(key[:])
	return pair, nil
}

// ManifestKeyBytes returns a copy of material bound by ResolveKeyPair.
func (p KeyPair) ManifestKeyBytes() ([32]byte, bool) {
	if !p.manifestKeyResolved || p.manifestKeySource != p.ManifestKey || manifestkey.Fingerprint(p.manifestKeyBytes) != p.ManifestKeyFingerprint {
		return [32]byte{}, false
	}
	return p.manifestKeyBytes, true
}

func (s *Store) encManifestKey(text, fingerprint string) (hash, encrypted string, err error) {
	hash, err = manifestkey.KnownFingerprint(text, fingerprint)
	if err != nil {
		return "", "", err
	}
	encrypted, err = s.box.EncryptString(text)
	return hash, encrypted, err
}

func bindManifestFingerprint(ctx context.Context, text string, fingerprint *string) error {
	if !secret.IsReference(text) {
		// Lifecycle upserts ignore credential replacements, but historically
		// accept a valid replacement literal in the caller's detached object.
		// Recompute its local projection instead of treating a stale runtime
		// field as an externally declared identity.
		fp, err := manifestkey.KnownFingerprint(text, "")
		if err == nil {
			*fingerprint = fp
		}
		return err
	}
	if *fingerprint == "" {
		key, err := manifestkey.Resolve(ctx, text)
		if err != nil {
			return err
		}
		*fingerprint = manifestkey.Fingerprint(key)
		clear(key[:])
	}
	fp, err := manifestkey.KnownFingerprint(text, *fingerprint)
	if err == nil {
		*fingerprint = fp
	}
	return err
}

func (s *Store) prepareSandboxInsertContext(ctx context.Context, sb *types.Sandbox) ([]any, error) {
	if sb != nil {
		if err := bindManifestFingerprint(ctx, sb.ManifestKey, &sb.ManifestKeyFingerprint); err != nil {
			return nil, err
		}
	}
	return s.prepareSandboxInsert(sb)
}

func (s *Store) prepareBuildWriteContext(ctx context.Context, b *types.Build) ([]any, error) {
	if b != nil {
		if err := bindManifestFingerprint(ctx, b.ManifestKey, &b.ManifestKeyFingerprint); err != nil {
			return nil, err
		}
	}
	return s.prepareBuildWrite(b)
}
