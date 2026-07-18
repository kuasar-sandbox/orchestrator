package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadBootID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boot_id")
	if err := os.WriteFile(path, []byte("boot-123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readBootID(path)
	if err != nil || got != "boot-123" {
		t.Fatalf("readBootID = %q, %v", got, err)
	}
}

func TestReadBootIDRejectsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boot_id")
	if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBootID(path); err == nil {
		t.Fatal("empty boot ID accepted")
	}
}
