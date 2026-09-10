//go:build ignore

package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const fixtureVersion = "v0.0.1-go1.26.4.linux-amd64"

func fixtureContents() map[string]string {
	return map[string]string{
		"bin/go":                       "synthetic fixture go, not executable",
		"pkg/tool/linux_amd64/compile": "synthetic fixture compiler",
		"src/runtime/runtime.go":       "synthetic fixture standard library",
		"_go.mod":                      "module std\n",
		"VERSION":                      "go1.26.4\ntime 2026-07-01T00:00:00Z\n",
		"LICENSE":                      "synthetic fixture copyright and license\n",
		"PATENTS":                      "synthetic fixture patent grant\n",
		"src/vendor/example.invalid/library/LICENSE": "synthetic nested library license\n",
		"src/cmd/vendor/example.invalid/tool/NOTICE": "synthetic nested compiler notice\n",
		"src/internal/copyright/copyright_test.go":   "synthetic source, not license text\n",
	}
}

func TestLicensePaths(t *testing.T) {
	for name, want := range map[string]bool{
		"LICENSE": true, "vendor/library/License.txt": true,
		"vendor/library/LICENSE-MIT": true, "nested/LICENSES/Apache-2.0.txt": true,
		"src/cmd/vendor/tool/NOTICE": true, "library/PATENTS": true,
		"src/internal/copyright/copyright_test.go": false,
		"src/LICENSE.go": false, "LICENSES/notice.py": false,
		"src/noticeboard.go": false, "VERSION": false,
	} {
		if got := licensePath(name); got != want {
			t.Errorf("licensePath(%q)=%t, want %t", name, got, want)
		}
	}
}

func fixtureArchive(t *testing.T, contents map[string]string, duplicate bool, link string) (string, string) {
	t.Helper()
	archive := filepath.Join(t.TempDir(), "toolchain.zip")
	out, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(out)
	var names []string
	for name := range contents {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		full := "golang.org/toolchain@" + fixtureVersion + "/" + name
		header := &zip.FileHeader{Name: full, Method: zip.Deflate}
		header.SetMode(0o644)
		if name == link {
			header.SetMode(os.ModeSymlink | 0o777)
		}
		file, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(contents[name])); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte(contents[name]))
		fmt.Fprintf(h, "%x  %s\n", digest, full)
	}
	if duplicate {
		file, err := w.Create("golang.org/toolchain@" + fixtureVersion + "/LICENSE")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("conflicting license")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return archive, "h1:" + base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func fixtureInstalled(t *testing.T, contents map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for relative, content := range contents {
		if relative == "_go.mod" {
			relative = "go.mod"
		}
		name := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestDistributionAndTarballNormalization(t *testing.T) {
	contents := fixtureContents()
	archive, h1 := fixtureArchive(t, contents, false, "")
	root := fixtureInstalled(t, contents)
	if err := os.WriteFile(filepath.Join(root, "_go.mod"), []byte(contents["_go.mod"]), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "doc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "doc", "full-distribution-extra"), []byte("not a build input"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, active := range []string{root, "-"} {
		destination := filepath.Join(t.TempDir(), "licenses")
		count, err := verifyDistribution(archive, fixtureVersion, h1, active, destination)
		if err != nil || count != len(contents) {
			t.Fatalf("count=%d err=%v", count, err)
		}
		found := make(map[string]bool)
		err = filepath.WalkDir(destination, func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			relative, err := filepath.Rel(destination, name)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			got, err := os.ReadFile(name)
			if err != nil || !licensePath(relative) || string(got) != contents[relative] {
				return fmt.Errorf("unexpected or changed license %s: %v", relative, err)
			}
			found[relative] = true
			return nil
		})
		if err != nil || len(found) != 4 {
			t.Fatalf("license-only extraction: %v %v", found, err)
		}
		for name := range contents {
			if licensePath(name) && !found[name] {
				t.Errorf("missing nested license %s", name)
			}
		}
	}
}

func TestInstalledInputTampering(t *testing.T) {
	cases := []struct{ name, file, action string }{
		{"compiler", "pkg/tool/linux_amd64/compile", "change"},
		{"standard-library", "src/runtime/runtime.go", "change"},
		{"license", "LICENSE", "change"},
		{"nested-license", "src/vendor/example.invalid/library/LICENSE", "change"},
		{"nested-notice", "src/cmd/vendor/example.invalid/tool/NOTICE", "missing"},
		{"renamed-module", "go.mod", "change"},
		{"missing-go", "bin/go", "missing"},
		{"added-source", "src/runtime/unverified.go", "change"},
		{"extra-root-license", "NOTICE.extra", "change"},
		{"symlink-go", "bin/go", "symlink"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			contents := fixtureContents()
			archive, h1 := fixtureArchive(t, contents, false, "")
			root := fixtureInstalled(t, contents)
			name := filepath.Join(root, filepath.FromSlash(test.file))
			if test.action == "missing" || test.action == "symlink" {
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(name, []byte("changed input"), 0o644); err != nil {
				t.Fatal(err)
			}
			if test.action == "symlink" {
				if err := os.Symlink(filepath.Join(root, "LICENSE"), name); err != nil {
					t.Fatal(err)
				}
			}
			destination := filepath.Join(t.TempDir(), "licenses")
			if _, err := verifyDistribution(archive, fixtureVersion, h1, root, destination); err == nil {
				t.Fatal("accepted changed installed input")
			}
			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("failed check created license output")
			}
		})
	}
}

func TestArchiveRejection(t *testing.T) {
	for _, test := range []string{"checksum", "version", "duplicate", "symlink", "traversal", "missing-compiler", "empty-license"} {
		t.Run(test, func(t *testing.T) {
			contents := fixtureContents()
			link := ""
			switch test {
			case "version":
				contents["VERSION"] = "go0.0.0\n"
			case "symlink":
				link = "LICENSE"
			case "traversal":
				contents["../escaped"] = "invalid"
			case "missing-compiler":
				delete(contents, "pkg/tool/linux_amd64/compile")
			case "empty-license":
				contents["LICENSE"] = ""
			}
			archive, h1 := fixtureArchive(t, contents, test == "duplicate", link)
			if test == "checksum" {
				h1 = "h1:" + strings.Repeat("A", 43) + "="
			}
			destination := filepath.Join(t.TempDir(), "licenses")
			if _, err := verifyDistribution(archive, fixtureVersion, h1, "-", destination); err == nil {
				t.Fatal("accepted invalid archive")
			}
			if _, err := os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("failed check created output")
			}
		})
	}
}
