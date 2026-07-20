package raftstore

import (
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
