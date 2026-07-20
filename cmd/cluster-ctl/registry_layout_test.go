package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

func TestBuildInitialRegistryLayoutBalancesAndValidatesReplicaSets(t *testing.T) {
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:7700", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:7700", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:7700", RaftEndpoint: "registry-c:63001"},
		{MemberID: "registry-d", InternalEndpoint: "https://registry-d:7700", RaftEndpoint: "registry-d:63001"},
	}
	registryLayout := buildInitialRegistryLayout("cluster-1", "generation-1", members, 8, 5000, []byte("secret"))
	if err := registryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, shard := range registryLayout.DataShards {
		for _, replica := range shard.Replicas {
			seen[replica.MemberID] = true
		}
	}
	if len(seen) != len(members) {
		t.Fatalf("initial placement did not use every member: %v", seen)
	}
}

func TestRegistryLayoutBootstrapWritesVerifiableArtifacts(t *testing.T) {
	dir := t.TempDir()
	membersPath := filepath.Join(dir, "members.json")
	secretPath := filepath.Join(dir, "bootstrap.secret")
	keyPath := filepath.Join(dir, "signing.pem")
	chainPath := filepath.Join(dir, "chain.json")
	keyringPath := filepath.Join(dir, "keyring.json")
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:7700", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:7700", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:7700", RaftEndpoint: "registry-c:63001"},
	}
	rawMembers, _ := json.Marshal(members)
	if err := os.WriteFile(membersPath, rawMembers, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secretPath, []byte("bootstrap-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registryLayoutCmd([]string{
		"bootstrap", "--members", membersPath, "--cluster-id", "cluster-1",
		"--registry-generation", "generation-1", "--bootstrap-secret-file", secretPath,
		"--signing-key", keyPath, "--key-id", "root-1", "--chain-out", chainPath,
		"--keyring-out", keyringPath, "--virtual-shards", "8", "--serve-permit-max", "5s",
	}); err != nil {
		t.Fatal(err)
	}
	chain, err := raftstore.LoadSignedRegistryLayoutChain(chainPath)
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := raftstore.LoadRegistryLayoutKeyring(keyringPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 {
		t.Fatalf("registryLayout chain length = %d", len(chain))
	}
	if _, err := chain[0].Verify(keyring); err != nil {
		t.Fatal(err)
	}
	if err := registryLayoutCmd([]string{
		"bootstrap", "--members", membersPath, "--cluster-id", "cluster-1",
		"--registry-generation", "generation-1", "--bootstrap-secret-file", secretPath,
		"--signing-key", keyPath, "--key-id", "root-1", "--chain-out", chainPath,
		"--keyring-out", keyringPath,
	}); err == nil {
		t.Fatal("immutable registryLayout artifacts were overwritten")
	}
}

func TestRegistryLayoutAppendVerifiesAndSignsExactNextArtifact(t *testing.T) {
	dir := t.TempDir()
	membersPath := filepath.Join(dir, "members.json")
	secretPath := filepath.Join(dir, "bootstrap.secret")
	keyPath := filepath.Join(dir, "signing.pem")
	chainPath := filepath.Join(dir, "chain-v1.json")
	keyringPath := filepath.Join(dir, "keyring.json")
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:7700", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:7700", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:7700", RaftEndpoint: "registry-c:63001"},
	}
	rawMembers, _ := json.Marshal(members)
	if err := os.WriteFile(membersPath, rawMembers, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secretPath, []byte("bootstrap-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registryLayoutCmd([]string{
		"bootstrap", "--members", membersPath, "--cluster-id", "cluster-1",
		"--registry-generation", "generation-1", "--bootstrap-secret-file", secretPath,
		"--signing-key", keyPath, "--key-id", "root-1", "--chain-out", chainPath,
		"--keyring-out", keyringPath, "--virtual-shards", "8", "--serve-permit-max", "5s",
	}); err != nil {
		t.Fatal(err)
	}
	chain, err := raftstore.LoadSignedRegistryLayoutChain(chainPath)
	if err != nil {
		t.Fatal(err)
	}
	previousDigest, err := chain[0].RegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := raftstore.CloneRegistryLayout(chain[0].RegistryLayout)
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = previousDigest
	registryLayoutPath := filepath.Join(dir, "registryLayout-v2.json")
	rawNext, _ := json.Marshal(next)
	if err := os.WriteFile(registryLayoutPath, rawNext, 0o600); err != nil {
		t.Fatal(err)
	}
	chainV2 := filepath.Join(dir, "chain-v2.json")
	if err := registryLayoutCmd([]string{
		"append", "--chain-in", chainPath, "--keys", keyringPath, "--registry-layout", registryLayoutPath,
		"--signing-key", keyPath, "--key-id", "root-1", "--chain-out", chainV2,
	}); err != nil {
		t.Fatal(err)
	}
	appended, err := raftstore.LoadSignedRegistryLayoutChain(chainV2)
	if err != nil || len(appended) != 2 || appended[1].RegistryLayout.RegistryLayoutVersion != 2 {
		t.Fatalf("appended chain = %+v, %v", appended, err)
	}
	keyring, err := raftstore.LoadRegistryLayoutKeyring(keyringPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appended[1].Verify(keyring); err != nil {
		t.Fatal(err)
	}

	unsafe := raftstore.CloneRegistryLayout(next)
	unsafe.Members[0].InternalEndpoint = "https://registry-a-moved:7700"
	rawUnsafe, _ := json.Marshal(unsafe)
	unsafePath := filepath.Join(dir, "registryLayout-unsafe.json")
	if err := os.WriteFile(unsafePath, rawUnsafe, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registryLayoutCmd([]string{
		"append", "--chain-in", chainPath, "--keys", keyringPath, "--registry-layout", unsafePath,
		"--signing-key", keyPath, "--key-id", "root-1", "--chain-out", filepath.Join(dir, "unsafe-chain.json"),
	}); err == nil {
		t.Fatal("Registry Layout append accepted a retained member endpoint change")
	}
}
