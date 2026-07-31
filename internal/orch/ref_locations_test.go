package orch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestSnapshotRefLocationsWalksMultipleLocations(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "args.log")
	rootRef := "file://" + strings.Repeat("a", 64) + ".snapshot@location:root-1"
	parentRef := "file://" + strings.Repeat("b", 64) + ".snapshot@location:parent-2"
	imageRef := "file://" + strings.Repeat("c", 64) + ".image@location:image-3"
	runtimeRef := "file://runtime.bundle@location:platform"
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
last=""
for arg in "$@"; do last="$arg"; done
case "$last" in
  %q) printf '%%s\n' %q ;;
  %q) printf '%%s\n' %q ;;
  *) exit 2 ;;
esac
`, logPath, rootRef,
		`{"FromRefs":["`+parentRef+`"],"Boot":{"RuntimeRef":"`+runtimeRef+`","Root":{"BaseRef":"`+imageRef+`"}}}`,
		parentRef, `{"Boot":{"Root":{}}}`)
	path := filepath.Join(dir, "sandbox-ctl")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	o := &Orchestrator{cfg: &config.Config{Checkpoint: config.CheckpointConfig{
		Remote: config.CheckpointRemoteConfig{RefLocationParent: "file:///mnt/shared/snapshots"},
	}}}
	got, err := o.snapshotRefLocations(context.Background(), strings.Repeat("d", 64), rootRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"root-1", "parent-2", "image-3"} {
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
		want := "file:///mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
		if got[name] != want {
			t.Fatalf("location %q = %q, want %q", name, got[name], want)
		}
	}
	if _, ok := got["platform"]; ok {
		t.Fatal("snapshot runtime_ref was treated as a named tenant artifact")
	}
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logBody), "--ref-location root-1=") ||
		!strings.Contains(string(logBody), "--ref-location parent-2=") {
		t.Fatalf("info calls did not receive discovered locations:\n%s", logBody)
	}
}

func TestLocatedRefRequiresConfiguredParent(t *testing.T) {
	o := &Orchestrator{cfg: &config.Config{}}
	ref := "file://" + strings.Repeat("a", 64) + ".snapshot@location:source"
	if _, err := o.snapshotRefLocations(context.Background(), strings.Repeat("b", 64), ref); err == nil {
		t.Fatal("located ref succeeded without ref_location_parent")
	}
}
