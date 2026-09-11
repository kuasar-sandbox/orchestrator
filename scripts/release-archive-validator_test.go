//go:build ignore

package main

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func archiveFixture(t *testing.T, memberSize int, trailing []byte) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "fixture.tar.gz")
	file, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	var names []string
	for name := range archiveContract {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := archiveContract[name]
		header := &tar.Header{Name: name, Mode: entry.mode, Typeflag: entry.typeflag}
		if entry.typeflag == tar.TypeReg {
			header.Size = int64(memberSize)
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if entry.typeflag == tar.TypeReg {
			if _, err := tw.Write(make([]byte, memberSize)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := gz.Write(trailing); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestArchiveTrailer(t *testing.T) {
	if err := validateArchive(archiveFixture(t, 16, nil)); err != nil {
		t.Fatal(err)
	}
	err := validateArchive(archiveFixture(t, 0, []byte("unexpected")))
	if err == nil || !strings.Contains(err.Error(), "after tar") {
		t.Fatalf("want trailing-data rejection, got %v", err)
	}
}

func TestArchiveEntryContract(t *testing.T) {
	cases := []struct {
		name, want string
		header     tar.Header
		duplicate  bool
	}{
		{"extra-binary", "unexpected member", tar.Header{Name: "./bin/unexpected-tool", Mode: 0o755, Typeflag: tar.TypeReg}, false},
		{"extra-deployment", "unexpected member", tar.Header{Name: "./deploy/unexpected.service", Mode: 0o644, Typeflag: tar.TypeReg}, false},
		{"extra-directory", "unexpected member", tar.Header{Name: "./bin/extra/", Mode: 0o755, Typeflag: tar.TypeDir}, false},
		{"path-alias", "unexpected member", tar.Header{Name: "./bin/./node-ctl", Mode: 0o755, Typeflag: tar.TypeReg}, false},
		{"foreign-material", "unexpected member", tar.Header{Name: "./share/licenses/foreign/LICENSE", Mode: 0o644, Typeflag: tar.TypeReg}, false},
		{"duplicate", "duplicate member", tar.Header{Name: "./bin/node-ctl", Mode: 0o755, Typeflag: tar.TypeReg}, true},
		{"wrong-mode", "has mode", tar.Header{Name: "./bin/node-ctl", Mode: 0o777, Typeflag: tar.TypeReg}, false},
		{"wrong-owner", "has owner", tar.Header{Name: "./bin/node-ctl", Mode: 0o755, Typeflag: tar.TypeReg, Uid: 123}, false},
		{"symlink", "has type", tar.Header{Name: "./bin/node-ctl", Mode: 0o755, Typeflag: tar.TypeSymlink, Linkname: "/bin/true"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "candidate.tar.gz")
			file, err := os.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			gz := gzip.NewWriter(file)
			tw := tar.NewWriter(gz)
			count := 1
			if tc.duplicate {
				count = 2
			}
			for range count {
				if err := tw.WriteHeader(&tc.header); err != nil {
					t.Fatal(err)
				}
			}
			for _, close := range []func() error{tw.Close, gz.Close, file.Close} {
				if err := close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := validateArchive(name); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %s rejection, got %v", tc.want, err)
			}
		})
	}
}
