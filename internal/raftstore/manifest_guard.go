package raftstore

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type AcceptedManifest struct {
	ClusterID         string `json:"cluster_id"`
	StorageGeneration string `json:"storage_generation"`
	ManifestVersion   uint64 `json:"manifest_version"`
	ManifestDigest    string `json:"manifest_digest"`
}

func (a AcceptedManifest) Validate() error {
	if a.ClusterID == "" || a.StorageGeneration == "" || a.ManifestVersion == 0 || !isSHA256(a.ManifestDigest) {
		return errors.New("raftstore: invalid accepted manifest state")
	}
	return nil
}

func (a AcceptedManifest) Accept(next Manifest, digest string) (AcceptedManifest, error) {
	if err := a.Validate(); err != nil {
		return AcceptedManifest{}, err
	}
	if err := next.Validate(); err != nil {
		return AcceptedManifest{}, err
	}
	if !isSHA256(digest) || next.ClusterID != a.ClusterID {
		return AcceptedManifest{}, errors.New("raftstore: unrelated manifest lineage")
	}
	if next.StorageGeneration == a.StorageGeneration {
		switch {
		case next.ManifestVersion < a.ManifestVersion:
			return AcceptedManifest{}, errors.New("raftstore: manifest rollback rejected")
		case next.ManifestVersion == a.ManifestVersion:
			if digest != a.ManifestDigest {
				return AcceptedManifest{}, errors.New("raftstore: manifest version equivocation")
			}
			return a, nil
		case next.ManifestVersion != a.ManifestVersion+1 || next.PreviousManifestDigest != a.ManifestDigest:
			return AcceptedManifest{}, errors.New("raftstore: manifest lineage gap")
		}
	} else {
		if next.Predecessor == nil || next.Predecessor.StorageGeneration != a.StorageGeneration ||
			next.Predecessor.ManifestDigest != a.ManifestDigest || next.ManifestVersion != 1 {
			return AcceptedManifest{}, errors.New("raftstore: storage generation rollover is not linked to the accepted predecessor")
		}
	}
	return AcceptedManifest{
		ClusterID: next.ClusterID, StorageGeneration: next.StorageGeneration,
		ManifestVersion: next.ManifestVersion, ManifestDigest: digest,
	}, nil
}

func FirstAcceptedManifest(manifest Manifest, digest string) (AcceptedManifest, error) {
	if err := manifest.Validate(); err != nil {
		return AcceptedManifest{}, err
	}
	if manifest.ManifestVersion != 1 || !isSHA256(digest) {
		return AcceptedManifest{}, errors.New("raftstore: first accepted manifest must start at version one")
	}
	return AcceptedManifest{
		ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
		ManifestVersion: manifest.ManifestVersion, ManifestDigest: digest,
	}, nil
}

type ManifestGuard struct{ Path string }

func (g ManifestGuard) AcceptSigned(
	signed SignedManifest,
	keyring map[string]ed25519.PublicKey,
) (AcceptedManifest, error) {
	digest, err := signed.Verify(keyring)
	if err != nil {
		return AcceptedManifest{}, err
	}
	current, err := g.Load()
	if err != nil {
		return AcceptedManifest{}, err
	}
	var next AcceptedManifest
	if current == nil {
		next, err = FirstAcceptedManifest(signed.Manifest, digest)
	} else {
		next, err = current.Accept(signed.Manifest, digest)
	}
	if err != nil {
		return AcceptedManifest{}, err
	}
	if err := g.store(next); err != nil {
		return AcceptedManifest{}, err
	}
	return next, nil
}

func (g ManifestGuard) Load() (*AcceptedManifest, error) {
	raw, err := os.ReadFile(g.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var accepted AcceptedManifest
	if err := json.Unmarshal(raw, &accepted); err != nil {
		return nil, fmt.Errorf("raftstore: decode manifest guard: %w", err)
	}
	if err := accepted.Validate(); err != nil {
		return nil, err
	}
	return &accepted, nil
}

func (g ManifestGuard) store(accepted AcceptedManifest) error {
	if err := accepted.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(accepted)
	if err != nil {
		return err
	}
	dir := filepath.Dir(g.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-guard-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, g.Path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
