package raftstore

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const maximumRegistryLayoutArtifactBytes = 64 << 20

// LoadSignedRegistryLayoutChain reads the operator-published, immutable registryLayout
// lineage. Signature and anti-rollback validation remain OpenRuntime's job.
func LoadSignedRegistryLayoutChain(path string) ([]SignedRegistryLayout, error) {
	if path == "" {
		return nil, errors.New("raftstore: signed registryLayout chain path is required")
	}
	raw, err := readBoundedArtifact(path, maximumRegistryLayoutArtifactBytes)
	if err != nil {
		return nil, err
	}
	var chain []SignedRegistryLayout
	if err := strictJSON(raw, &chain); err != nil {
		return nil, fmt.Errorf("raftstore: decode signed registryLayout chain: %w", err)
	}
	if len(chain) == 0 {
		return nil, errors.New("raftstore: signed registryLayout chain is empty")
	}
	return chain, nil
}

// LoadRegistryLayoutKeyring reads key_id -> Ed25519 public key. Values may be raw
// base64, raw-base64, or 64-character hexadecimal encodings.
func LoadRegistryLayoutKeyring(path string) (map[string]ed25519.PublicKey, error) {
	if path == "" {
		return nil, errors.New("raftstore: registryLayout keyring path is required")
	}
	raw, err := readBoundedArtifact(path, 1<<20)
	if err != nil {
		return nil, err
	}
	var encoded map[string]string
	if err := strictJSON(raw, &encoded); err != nil {
		return nil, fmt.Errorf("raftstore: decode registryLayout keyring: %w", err)
	}
	if len(encoded) == 0 {
		return nil, errors.New("raftstore: registryLayout keyring is empty")
	}
	keys := make(map[string]ed25519.PublicKey, len(encoded))
	for keyID, value := range encoded {
		if keyID == "" {
			return nil, errors.New("raftstore: registryLayout keyring contains an empty key ID")
		}
		decoded, decodeErr := decodePublicKey(value)
		if decodeErr != nil {
			return nil, fmt.Errorf("raftstore: registryLayout key %q: %w", keyID, decodeErr)
		}
		keys[keyID] = ed25519.PublicKey(decoded)
	}
	return keys, nil
}

func readBoundedArtifact(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		return nil, fmt.Errorf("raftstore: artifact %s has an invalid size or type", path)
	}
	return os.ReadFile(path)
}

func strictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("trailing JSON value")
	}
	return nil
}

func decodePublicKey(value string) ([]byte, error) {
	decoders := []func(string) ([]byte, error){
		hex.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.StdEncoding.DecodeString,
	}
	for _, decode := range decoders {
		decoded, err := decode(value)
		if err == nil && len(decoded) == ed25519.PublicKeySize {
			return decoded, nil
		}
	}
	return nil, errors.New("public key must encode exactly 32 bytes")
}
