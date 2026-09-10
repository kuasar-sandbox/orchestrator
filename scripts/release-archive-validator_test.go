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

func archiveFixture(t *testing.T, oversizedHeader bool, memberSize int, trailing []byte) string {
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
		if oversizedHeader && name == "./bin/node-ctl" {
			header.Size = (512 << 20) + 1
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if oversizedHeader && name == "./bin/node-ctl" {
			// A header-only compressed bomb must fail its declared-size check,
			// before attempting to consume the missing enormous body.
			if err := gz.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			return file.Name()
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

func TestArchiveResourceLimits(t *testing.T) {
	valid := archiveFixture(t, false, 16, nil)
	if err := validateArchive(valid); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, archive, want string
		member, expanded    int64
		entries             int
	}{
		{"declared-member", archiveFixture(t, true, 0, nil), "member", 512 << 20, 1 << 30, 20000},
		{"cumulative-body", archiveFixture(t, false, 4096, nil), "expanded", 8192, 16384, 20000},
		{"padding", archiveFixture(t, false, 0, make([]byte, 65536)), "expanded", 8192, 32768, 20000},
		{"count", valid, "member count", 8192, 65536, 2},
		{"trailing-data", archiveFixture(t, false, 0, []byte("unexpected")), "after tar", 8192, 65536, 20000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateArchiveWithLimits(tc.archive, tc.member, tc.expanded, tc.entries)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %s limit, got %v", tc.want, err)
			}
		})
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
