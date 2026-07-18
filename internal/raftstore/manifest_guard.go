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
	ClusterID            string `json:"cluster_id"`
	StorageGeneration    string `json:"storage_generation"`
	ManifestVersion      uint64 `json:"manifest_version"`
	ManifestDigest       string `json:"manifest_digest"`
	FormatVersion        uint32 `json:"format_version"`
	SchemaVersion        uint32 `json:"schema_version"`
	ProtocolVersion      uint32 `json:"protocol_version"`
	HashVersion          string `json:"hash_version"`
	VirtualShardCount    uint32 `json:"virtual_shard_count"`
	RouteBucketCount     uint32 `json:"route_bucket_count"`
	BuildBucketCount     uint32 `json:"build_bucket_count"`
	ReplicationFactor    uint32 `json:"replication_factor"`
	ServePermitMaxMillis uint64 `json:"serve_permit_max_millis"`
	BootstrapTokenDigest string `json:"bootstrap_token_digest"`
}

func (a AcceptedManifest) Validate() error {
	if a.ClusterID == "" || a.StorageGeneration == "" || a.ManifestVersion == 0 || !isSHA256(a.ManifestDigest) ||
		a.FormatVersion != ManifestFormatV1 || a.SchemaVersion == 0 || a.ProtocolVersion == 0 ||
		a.HashVersion != "ShardHashV1" || !isPowerOfTwo(a.VirtualShardCount) ||
		!isPowerOfTwo(a.RouteBucketCount) || !isPowerOfTwo(a.BuildBucketCount) ||
		a.ReplicationFactor != DefaultReplication || a.ServePermitMaxMillis == 0 ||
		!isSHA256(a.BootstrapTokenDigest) {
		return errors.New("raftstore: invalid accepted manifest state")
	}
	return nil
}

func (a AcceptedManifest) matchesFrozenParameters(next Manifest) bool {
	return a.FormatVersion == next.FormatVersion &&
		a.SchemaVersion == next.SchemaVersion &&
		a.ProtocolVersion == next.ProtocolVersion &&
		a.HashVersion == next.HashVersion &&
		a.VirtualShardCount == next.VirtualShardCount &&
		a.RouteBucketCount == next.RouteBucketCount &&
		a.BuildBucketCount == next.BuildBucketCount &&
		a.ReplicationFactor == next.ReplicationFactor &&
		a.ServePermitMaxMillis == next.ServePermitMaxMillis &&
		a.BootstrapTokenDigest == next.BootstrapTokenDigest
}

func acceptedManifest(manifest Manifest, digest string) AcceptedManifest {
	return AcceptedManifest{
		ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
		ManifestVersion: manifest.ManifestVersion, ManifestDigest: digest,
		FormatVersion: manifest.FormatVersion, SchemaVersion: manifest.SchemaVersion,
		ProtocolVersion: manifest.ProtocolVersion, HashVersion: manifest.HashVersion,
		VirtualShardCount: manifest.VirtualShardCount, RouteBucketCount: manifest.RouteBucketCount,
		BuildBucketCount: manifest.BuildBucketCount, ReplicationFactor: manifest.ReplicationFactor,
		ServePermitMaxMillis: manifest.ServePermitMaxMillis,
		BootstrapTokenDigest: manifest.BootstrapTokenDigest,
	}
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
		if !a.matchesFrozenParameters(next) {
			return AcceptedManifest{}, errors.New("raftstore: same-generation manifest changed frozen parameters")
		}
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
			next.Predecessor.ManifestDigest != a.ManifestDigest ||
			next.Predecessor.ServePermitMaxMillis != a.ServePermitMaxMillis || next.ManifestVersion != 1 {
			return AcceptedManifest{}, errors.New("raftstore: storage generation rollover is not linked to the accepted predecessor")
		}
	}
	return acceptedManifest(next, digest), nil
}

func FirstAcceptedManifest(manifest Manifest, digest string) (AcceptedManifest, error) {
	if err := manifest.Validate(); err != nil {
		return AcceptedManifest{}, err
	}
	if manifest.ManifestVersion != 1 || !isSHA256(digest) {
		return AcceptedManifest{}, errors.New("raftstore: first accepted manifest must start at version one")
	}
	return acceptedManifest(manifest, digest), nil
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

func (g ManifestGuard) AcceptSignedChain(
	chain []SignedManifest,
	keyring map[string]ed25519.PublicKey,
) (AcceptedManifest, error) {
	accepted, err := g.EvaluateSignedChain(chain, keyring)
	if err != nil {
		return AcceptedManifest{}, err
	}
	if err := g.store(accepted); err != nil {
		return AcceptedManifest{}, err
	}
	return accepted, nil
}

// EvaluateSignedChain verifies and advances a manifest lineage in memory. It
// lets runtime bootstrap validate every local prerequisite before making the
// anti-rollback decision durable.
func (g ManifestGuard) EvaluateSignedChain(
	chain []SignedManifest,
	keyring map[string]ed25519.PublicKey,
) (AcceptedManifest, error) {
	if len(chain) == 0 || len(chain) > 1024 {
		return AcceptedManifest{}, errors.New("raftstore: manifest chain must be non-empty and bounded")
	}
	current, err := g.Load()
	if err != nil {
		return AcceptedManifest{}, err
	}
	digests := make([]string, len(chain))
	for index, signed := range chain {
		digest, verifyErr := signed.Verify(keyring)
		if verifyErr != nil {
			return AcceptedManifest{}, verifyErr
		}
		digests[index] = digest
	}
	start := 0
	if current != nil {
		anchor := -1
		for index, signed := range chain {
			if signed.Manifest.ClusterID == current.ClusterID &&
				signed.Manifest.StorageGeneration == current.StorageGeneration &&
				signed.Manifest.ManifestVersion == current.ManifestVersion && digests[index] == current.ManifestDigest {
				anchor = index
				break
			}
		}
		if anchor >= 0 {
			for index := 1; index <= anchor; index++ {
				previous := acceptedManifest(chain[index-1].Manifest, digests[index-1])
				if _, linkErr := previous.Accept(chain[index].Manifest, digests[index]); linkErr != nil {
					return AcceptedManifest{}, fmt.Errorf("raftstore: invalid signed manifest chain link: %w", linkErr)
				}
			}
			start = anchor + 1
		}
	}
	for index := start; index < len(chain); index++ {
		signed := chain[index]
		digest := digests[index]
		if current == nil {
			accepted, acceptErr := FirstAcceptedManifest(signed.Manifest, digest)
			if acceptErr != nil {
				return AcceptedManifest{}, acceptErr
			}
			current = &accepted
			continue
		}
		accepted, acceptErr := current.Accept(signed.Manifest, digest)
		if acceptErr != nil {
			return AcceptedManifest{}, acceptErr
		}
		current = &accepted
	}
	if current == nil {
		return AcceptedManifest{}, errors.New("raftstore: manifest chain did not produce an accepted state")
	}
	return *current, nil
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
