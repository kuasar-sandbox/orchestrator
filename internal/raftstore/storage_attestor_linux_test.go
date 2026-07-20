//go:build linux

package raftstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDMCryptAttestorWalksBoundedBackingDeviceGraph(t *testing.T) {
	root := t.TempDir()
	crypt := filepath.Join(root, "dev", "block", "253:0", "dm")
	if err := os.MkdirAll(crypt, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(crypt, "uuid"), []byte("CRYPT-LUKS2-deadbeef"), 0o600); err != nil {
		t.Fatal(err)
	}
	parentSlave := filepath.Join(root, "dev", "block", "253:1", "slaves", "crypt-volume")
	if err := os.MkdirAll(parentSlave, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parentSlave, "dev"), []byte("253:0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	encrypted, err := dmCryptInDeviceGraph(root, "253:1", make(map[string]struct{}))
	if err != nil || !encrypted {
		t.Fatalf("nested dm-crypt detection = %t, %v", encrypted, err)
	}
	if _, err := dmCryptInDeviceGraph(root, "../../etc", make(map[string]struct{})); err == nil {
		t.Fatal("invalid device number entered the sysfs graph")
	}
	mixed := filepath.Join(root, "dev", "block", "253:2", "slaves")
	for name, device := range map[string]string{"encrypted": "253:0\n", "plaintext": "8:0\n"} {
		path := filepath.Join(mixed, name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "dev"), []byte(device), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "dev", "block", "8:0", "slaves"), 0o700); err != nil {
		t.Fatal(err)
	}
	if encrypted, err := dmCryptInDeviceGraph(root, "253:2", make(map[string]struct{})); err != nil || encrypted {
		t.Fatalf("mixed encrypted/plain graph = %t, %v", encrypted, err)
	}
}
