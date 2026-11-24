package orch

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
)

func TestRuntimeLabelsUsesDeclaredDigestAndLexicalDirectory(t *testing.T) {
	root := t.TempDir()
	local, release := filepath.Join(root, "local"), filepath.Join(root, "release")
	for _, dir := range []string{local, release} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, digest string) {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		if _, err := w.CreateHeader(&zip.FileHeader{Name: tarstream.DigestMarkerPrefix + digest, Method: zip.Store}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		data := append(make([]byte, (2<<20)-b.Len()), b.Bytes()...)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	digest := strings.Repeat("a", 64)
	write(filepath.Join(release, "primary.bundle"), digest)
	write(filepath.Join(release, "outside.bundle"), strings.Repeat("b", 64))
	write(filepath.Join(local, "sibling.bundle"), strings.Repeat("c", 64))
	primary := filepath.Join(local, "alias.bundle")
	if err := os.Symlink(filepath.Join(release, "primary.bundle"), primary); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(local, "bad.bundle"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(local, "subdir.bundle"), 0700); err != nil {
		t.Fatal(err)
	}
	configured := map[string]string{"zone": "test", placement.RuntimeLabelPrefix + "spoof.bundle": "digest:" + digest}
	diagnostics := 0
	labels := RuntimeLabels(configured, primary, func(string, error) { diagnostics++ })
	if labels[placement.RuntimeLabelPrefix+"alias.bundle"] != "digest:"+digest || labels[placement.RuntimeLabelPrefix+"sibling.bundle"] != "digest:"+strings.Repeat("c", 64) {
		t.Fatalf("wrong inventory: %v", labels)
	}
	if len(labels) != 3 || labels["zone"] != "test" || diagnostics != 1 || len(configured) != 2 {
		t.Fatalf("labels escaped lexical scope or changed config: %v diagnostics=%d", labels, diagnostics)
	}
}
