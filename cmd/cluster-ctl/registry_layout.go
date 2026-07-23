package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

const (
	maximumInitialRegistryLayoutMembers = 128
	// A three-replica shard row occupies at least 124 JSON bytes. The largest
	// power of two that can possibly fit MaxRegistryLayoutBytes is therefore
	// 32768; larger values cannot produce a valid signed artifact.
	maximumBootstrapVirtualShards = uint64(1 << 15)
)

func registryLayoutCmd(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: cluster-ctl registry-layout {bootstrap|append} [flags]")
	}
	switch args[0] {
	case "bootstrap":
		return registryLayoutBootstrapCmd(args[1:])
	case "append":
		return registryLayoutAppendCmd(args[1:])
	default:
		return errors.New("usage: cluster-ctl registry-layout {bootstrap|append} [flags]")
	}

}

func registryLayoutBootstrapCmd(args []string) error {
	flags := flag.NewFlagSet("registry-layout bootstrap", flag.ContinueOnError)
	membersPath := flags.String("members", "", "JSON array of Registry members")
	clusterID := flags.String("cluster-id", "", "stable cluster identity")
	generation := flags.String("registry-generation", "", "new empty Registry History Generation")
	secretPath := flags.String("bootstrap-secret-file", "", "one-time bootstrap secret")
	signingKeyPath := flags.String("signing-key", "", "Ed25519 PKCS#8 PEM private key")
	keyID := flags.String("key-id", "", "Registry Layout signing key ID")
	chainOut := flags.String("chain-out", "", "new signed Registry Layout chain path")
	keyringOut := flags.String("keyring-out", "", "new Registry Layout public-key ring path")
	virtualShards := flags.Uint("virtual-shards", uint(raftstore.DefaultVirtualShards), "fixed power-of-two virtual shard count")
	permitLifetime := flags.Duration("serve-permit-max", 5*time.Second, "maximum Serve Permit lifetime")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *membersPath == "" || *clusterID == "" || *generation == "" ||
		*secretPath == "" || *signingKeyPath == "" || *keyID == "" || *chainOut == "" || *keyringOut == "" {
		return errors.New("registry-layout bootstrap: all identity, member, secret, signing, and output flags are required")
	}
	if err := validateBootstrapVirtualShards(uint64(*virtualShards)); err != nil {
		return err
	}
	permitMillis := *permitLifetime / time.Millisecond
	if permitMillis <= 0 || time.Duration(permitMillis)*time.Millisecond != *permitLifetime ||
		permitMillis > time.Duration(raftstore.MaximumServePermitMillis) {
		return errors.New("registry-layout bootstrap: Serve Permit lifetime must be whole milliseconds in (0,60s]")
	}
	members, err := loadInitialMembers(*membersPath)
	if err != nil {
		return err
	}
	secret, err := readBoundedRegularFile(*secretPath, 4096)
	if err != nil {
		return fmt.Errorf("registry-layout bootstrap: read bootstrap secret: %w", err)
	}
	secret = bytes.TrimSpace(secret)
	if len(secret) == 0 {
		return errors.New("registry-layout bootstrap: bootstrap secret is empty")
	}
	privateKey, err := loadRegistryLayoutSigningKey(*signingKeyPath)
	if err != nil {
		return err
	}
	registryLayout := buildInitialRegistryLayout(
		*clusterID, *generation, members, uint32(*virtualShards), uint64(permitMillis), secret,
	)
	signed, err := raftstore.SignRegistryLayout(registryLayout, *keyID, privateKey)
	if err != nil {
		return err
	}
	chain, err := json.MarshalIndent([]raftstore.SignedRegistryLayout{signed}, "", "  ")
	if err != nil {
		return err
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("registry-layout bootstrap: signing key has no Ed25519 public key")
	}
	keyring, err := json.MarshalIndent(map[string]string{
		*keyID: base64.RawStdEncoding.EncodeToString(publicKey),
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := requireNewOutputPaths(*chainOut, *keyringOut); err != nil {
		return err
	}
	if err := writeNewArtifact(*chainOut, append(chain, '\n'), 0o644); err != nil {
		return err
	}
	if err := writeNewArtifact(*keyringOut, append(keyring, '\n'), 0o644); err != nil {
		_ = os.Remove(*chainOut)
		return err
	}
	return nil
}

func validateBootstrapVirtualShards(value uint64) error {
	if value == 0 || value > uint64(^uint32(0)) || value&(value-1) != 0 {
		return errors.New("registry-layout bootstrap: virtual shard count must be a power of two in uint32")
	}
	if value > maximumBootstrapVirtualShards {
		return fmt.Errorf(
			"registry-layout bootstrap: virtual shard count exceeds artifact capacity (%d)",
			maximumBootstrapVirtualShards,
		)
	}
	return nil
}

func registryLayoutAppendCmd(args []string) error {
	flags := flag.NewFlagSet("registry-layout append", flag.ContinueOnError)
	chainIn := flags.String("chain-in", "", "complete existing signed Registry Layout chain")
	keyringPath := flags.String("keys", "", "trusted Registry Layout public-key ring")
	registryLayoutPath := flags.String("registry-layout", "", "complete unsigned next Registry Layout JSON")
	signingKeyPath := flags.String("signing-key", "", "Ed25519 PKCS#8 PEM private key")
	keyID := flags.String("key-id", "", "trusted Registry Layout signing key ID")
	chainOut := flags.String("chain-out", "", "new immutable signed Registry Layout chain path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *chainIn == "" || *keyringPath == "" || *registryLayoutPath == "" ||
		*signingKeyPath == "" || *keyID == "" || *chainOut == "" {
		return errors.New("registry-layout append: chain, keyring, Registry Layout, signing key, key ID, and output are required")
	}
	if *chainIn == *chainOut {
		return errors.New("registry-layout append: immutable input chain requires a distinct output path")
	}
	chain, err := raftstore.LoadSignedRegistryLayoutChain(*chainIn)
	if err != nil {
		return err
	}
	keyring, err := raftstore.LoadRegistryLayoutKeyring(*keyringPath)
	if err != nil {
		return err
	}
	accepted, err := validateRegistryLayoutChain(chain, keyring)
	if err != nil {
		return err
	}
	candidate, err := loadUnsignedRegistryLayout(*registryLayoutPath)
	if err != nil {
		return err
	}
	latest := chain[len(chain)-1].RegistryLayout
	if candidate.RegistryGeneration == latest.RegistryGeneration {
		if err := raftstore.ValidateRegistryLayoutTransition(latest, candidate); err != nil {
			return err
		}
	}
	privateKey, err := loadRegistryLayoutSigningKey(*signingKeyPath)
	if err != nil {
		return err
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || !bytes.Equal(publicKey, keyring[*keyID]) {
		return errors.New("registry-layout append: signing key does not match the trusted keyring entry")
	}
	signed, err := raftstore.SignRegistryLayout(candidate, *keyID, privateKey)
	if err != nil {
		return err
	}
	digest, err := signed.Verify(keyring)
	if err != nil {
		return err
	}
	if _, err := accepted.Accept(candidate, digest); err != nil {
		return fmt.Errorf("registry-layout append: next Registry Layout does not extend the accepted lineage: %w", err)
	}
	chain = append(chain, signed)
	encoded, err := json.MarshalIndent(chain, "", "  ")
	if err != nil {
		return err
	}
	if err := requireNewOutputPaths(*chainOut); err != nil {
		return err
	}
	return writeNewArtifact(*chainOut, append(encoded, '\n'), 0o644)
}

func validateRegistryLayoutChain(
	chain []raftstore.SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
) (raftstore.AcceptedRegistryLayout, error) {
	var accepted raftstore.AcceptedRegistryLayout
	for index, signed := range chain {
		digest, err := signed.Verify(keyring)
		if err != nil {
			return raftstore.AcceptedRegistryLayout{}, err
		}
		if index == 0 {
			accepted, err = raftstore.FirstAcceptedRegistryLayout(signed.RegistryLayout, digest)
			if err != nil {
				return raftstore.AcceptedRegistryLayout{}, err
			}
			continue
		}
		previous := chain[index-1].RegistryLayout
		if previous.RegistryGeneration == signed.RegistryLayout.RegistryGeneration {
			if err := raftstore.ValidateRegistryLayoutTransition(previous, signed.RegistryLayout); err != nil {
				return raftstore.AcceptedRegistryLayout{}, err
			}
		}
		accepted, err = accepted.Accept(signed.RegistryLayout, digest)
		if err != nil {
			return raftstore.AcceptedRegistryLayout{}, err
		}
	}
	return accepted, nil
}

func loadUnsignedRegistryLayout(path string) (raftstore.RegistryLayout, error) {
	raw, err := readBoundedRegularFile(path, 64<<20)
	if err != nil {
		return raftstore.RegistryLayout{}, fmt.Errorf("registry-layout append: read unsigned Registry Layout: %w", err)
	}
	var registryLayout raftstore.RegistryLayout
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&registryLayout); err != nil {
		return raftstore.RegistryLayout{}, fmt.Errorf("registry-layout append: decode unsigned Registry Layout: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return raftstore.RegistryLayout{}, errors.New("registry-layout append: unsigned Registry Layout must contain exactly one JSON value")
	}
	if err := registryLayout.Validate(); err != nil {
		return raftstore.RegistryLayout{}, err
	}
	return registryLayout, nil
}

func loadInitialMembers(path string) ([]raftstore.RegistryMember, error) {
	raw, err := readBoundedRegularFile(path, 1<<20)
	if err != nil {
		return nil, fmt.Errorf("registry-layout bootstrap: read members: %w", err)
	}
	var members []raftstore.RegistryMember
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&members); err != nil {
		return nil, fmt.Errorf("registry-layout bootstrap: decode members: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("registry-layout bootstrap: members must contain exactly one JSON value")
	}
	if len(members) < int(raftstore.DefaultReplication) || len(members) > maximumInitialRegistryLayoutMembers {
		return nil, fmt.Errorf("registry-layout bootstrap: member count must be in [%d,%d]", raftstore.DefaultReplication, maximumInitialRegistryLayoutMembers)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })
	return members, nil
}

func buildInitialRegistryLayout(
	clusterID, generation string,
	members []raftstore.RegistryMember,
	virtualShards uint32,
	permitMillis uint64,
	bootstrapSecret []byte,
) raftstore.RegistryLayout {
	replicas := func(offset uint32) []raftstore.ReplicaPlacement {
		set := make([]raftstore.ReplicaPlacement, 0, raftstore.DefaultReplication)
		for index := uint32(0); index < raftstore.DefaultReplication; index++ {
			memberIndex := (offset + index) % uint32(len(members))
			set = append(set, raftstore.ReplicaPlacement{
				MemberID: members[memberIndex].MemberID, ReplicaID: uint64(memberIndex) + 1,
			})
		}
		sort.Slice(set, func(i, j int) bool { return set[i].MemberID < set[j].MemberID })
		return set
	}
	placements := make([]raftstore.ShardPlacement, virtualShards)
	for shardID := uint32(0); shardID < virtualShards; shardID++ {
		placements[shardID] = raftstore.ShardPlacement{ShardID: shardID, Replicas: replicas(shardID + 1)}
	}
	digest := sha256.Sum256(bootstrapSecret)
	return raftstore.RegistryLayout{
		FormatVersion: raftstore.RegistryLayoutFormatV1, ClusterID: clusterID, RegistryGeneration: generation,
		RegistryLayoutVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: virtualShards, RouteBucketCount: raftstore.DefaultRouteBuckets,
		BuildBucketCount: raftstore.DefaultBuildBuckets, ReplicationFactor: raftstore.DefaultReplication,
		ServePermitMaxMillis: permitMillis, BootstrapTokenDigest: hex.EncodeToString(digest[:]),
		Members: append([]raftstore.RegistryMember(nil), members...), SystemReplicas: replicas(0), DataShards: placements,
	}
}

func loadRegistryLayoutSigningKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("registry-layout bootstrap: stat signing key: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("registry-layout bootstrap: signing key must be a private regular file with mode 0600 or stricter")
	}
	raw, err := readBoundedRegularFile(path, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("registry-layout bootstrap: read signing key: %w", err)
	}
	block, rest := pem.Decode(raw)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 || block.Type != "PRIVATE KEY" {
		return nil, errors.New("registry-layout bootstrap: signing key must be one PKCS#8 PRIVATE KEY PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("registry-layout bootstrap: parse signing key: %w", err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("registry-layout bootstrap: signing key must be Ed25519")
	}
	return key, nil
}

func readBoundedRegularFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("file has an invalid type or size")
	}
	return io.ReadAll(io.LimitReader(file, maximum+1))
}

func requireNewOutputPaths(paths ...string) error {
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path == "" {
			return errors.New("registry-layout bootstrap: output path is empty")
		}
		if _, duplicate := seen[path]; duplicate {
			return errors.New("registry-layout bootstrap: output paths must be distinct")
		}
		seen[path] = struct{}{}
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("registry-layout bootstrap: output already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func writeNewArtifact(path string, payload []byte, mode os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("registry-layout bootstrap: artifact path is empty")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(payload); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	remove = false
	return nil
}
