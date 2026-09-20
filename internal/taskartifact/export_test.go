package taskartifact

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExportSnapshotMetadataKeepsOpaqueDependencies(t *testing.T) {
	body := "resources:\n  capacity: {cpu: 1, memory: 64MiB}\nboot:\n  root:\n    base: self\n    base_from_refs: [manifest://" + strings.Repeat("a", 64) + "]\nfrom_refs: [manifest://" + strings.Repeat("b", 64) + "]\n"
	t.Run("Manifest", func(t *testing.T) {
		e, s, configPath := writeTaskManifestStoreArtifacts(t, body)
		got, err := SnapshotSandboxRef(context.Background(), types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: s}, configPath, os.Getenv("MANIFEST_KEY"), "")
		if err != nil || got != e {
			t.Fatalf("metadata-only E=%q want=%q err=%v", got, e, err)
		}
	})
	t.Run("located Bundle", func(t *testing.T) {
		parent := "file://" + t.TempDir()
		location, err := reflocation.Resolve(parent, "checkpoint")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(location.Path, 0700); err != nil {
			t.Fatal(err)
		}
		path, sKey, eKey, configPath := writeTaskManifestBundleArtifacts(t, location.Path, nil, body)
		root := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: filepath.Base(path), DigestScheme: "manifest", Digest: sKey, Location: "checkpoint"}
		key := [32]byte{0x41, 0x42, 0x43}
		got, err := SnapshotSandboxRef(context.Background(), types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: root.String()}, configPath, hex.EncodeToString(key[:]), parent)
		root.Digest = eKey
		if err != nil || got != root.String() {
			t.Fatalf("bound E=%q want=%q err=%v", got, root.String(), err)
		}
	})
}
