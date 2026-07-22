package raftstore

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestRuntimeStorageAttestationCoversIdentityAndRaftState(t *testing.T) {
	root := t.TempDir()
	identityDir := filepath.Join(root, "identity")
	var attested []string
	config := RuntimeConfig{
		NodeHostDir:             filepath.Join(root, "nodehost"),
		WALDir:                  filepath.Join(root, "nodehost"),
		StateEngineDir:          filepath.Join(root, "state"),
		RegistryLayoutGuardPath: filepath.Join(identityDir, "registryLayout.json"),
		EnrollmentPath:          filepath.Join(identityDir, "enrollment.json"),
		StorageAttestor: StorageAttestorFunc(func(paths ...string) error {
			attested = append([]string(nil), paths...)
			return nil
		}),
	}
	if err := config.attestStorage(); err != nil {
		t.Fatal(err)
	}
	want := []string{identityDir, config.NodeHostDir, config.StateEngineDir}
	slices.Sort(want)
	if !slices.Equal(attested, want) {
		t.Fatalf("attested paths = %v, want %v", attested, want)
	}
}

func TestRuntimeStorageSeparationResolvesSymlinkedAncestors(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}

	config := RuntimeConfig{
		NodeHostDir:             filepath.Join(real, "nodehost"),
		StateEngineDir:          filepath.Join(alias, "nodehost", "state"),
		RegistryLayoutGuardPath: filepath.Join(real, "identity", "registryLayout.json"),
		EnrollmentPath:          filepath.Join(real, "identity", "enrollment.json"),
	}
	if err := config.validateStoragePathSeparation(); err == nil {
		t.Fatal("StateEngine directory hidden under a symlinked NodeHost directory was accepted")
	}

	config.StateEngineDir = filepath.Join(real, "state")
	config.RegistryLayoutGuardPath = filepath.Join(alias, "nodehost", "registryLayout.json")
	if err := config.validateStoragePathSeparation(); err == nil {
		t.Fatal("identity file hidden under a symlinked NodeHost directory was accepted")
	}
}

func TestRuntimeStoragePathsCanonicalizeNonexistentChildren(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	config := RuntimeConfig{NodeHostDir: filepath.Join(alias, "new", "nodehost")}
	resolved, err := config.resolvedStoragePaths()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(real, "new", "nodehost")
	if resolved.NodeHostDir != want {
		t.Fatalf("resolved NodeHost path = %q, want %q", resolved.NodeHostDir, want)
	}
}

func TestRuntimeConfigDigestBindsRegistryLayoutGuardPath(t *testing.T) {
	root := t.TempDir()
	registryLayout := testRegistryLayout(1, "generation-guard-path")
	member := registryLayout.Members[0]
	config := RuntimeConfig{
		NodeHostDir: filepath.Join(root, "nodehost"), StateEngineDir: filepath.Join(root, "state"),
		RegistryLayoutGuardPath: filepath.Join(root, "identity", "registryLayout.json"),
		Tuning:                  DefaultRuntimeTuning(),
	}
	first, err := config.digest(registryLayout, member)
	if err != nil {
		t.Fatal(err)
	}
	config.RegistryLayoutGuardPath = filepath.Join(root, "other-identity", "registryLayout.json")
	second, err := config.digest(registryLayout, member)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("runtime identity digest did not bind the anti-rollback guard path")
	}
}
