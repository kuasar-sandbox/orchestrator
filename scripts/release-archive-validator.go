//go:build ignore

package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

type entryContract struct {
	typeflag byte
	mode     int64
}

var archiveContract = map[string]entryContract{
	"./":                                             {typeflag: tar.TypeDir, mode: 0o755},
	"./bin/":                                         {typeflag: tar.TypeDir, mode: 0o755},
	"./deploy/":                                      {typeflag: tar.TypeDir, mode: 0o755},
	"./bin/node-ctl":                                 {typeflag: tar.TypeReg, mode: 0o755},
	"./bin/cluster-ctl":                              {typeflag: tar.TypeReg, mode: 0o755},
	"./bin/node-stub-ctl":                            {typeflag: tar.TypeReg, mode: 0o755},
	"./bin/e2b-key-ctl":                              {typeflag: tar.TypeReg, mode: 0o755},
	"./deploy/node-ctl.service":                      {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/node-proxy.service":                    {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/cluster-registry.service":              {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/cluster-router.service":                {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/cluster-placer.service":                {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/conductor.example.yaml":                {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/proxy.example.yaml":                    {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/registry.example.yaml":                 {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/router.example.yaml":                   {typeflag: tar.TypeReg, mode: 0o644},
	"./deploy/placer.example.yaml":                   {typeflag: tar.TypeReg, mode: 0o644},
	"./share/licenses/orchestrator/project/LICENSE":  {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/orchestrator/GO-BUILD-INFO.tsv": {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/orchestrator/GO-MODULES.tsv":    {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/orchestrator/MATERIALS.sha256":  {typeflag: tar.TypeReg, mode: 0o644},
	"./share/sources/orchestrator/SOURCES.tsv":       {typeflag: tar.TypeReg, mode: 0o644},
}

func materialContract(name string) (entryContract, bool) {
	for _, directory := range []string{
		"./share/",
		"./share/licenses/",
		"./share/licenses/orchestrator/",
		"./share/sources/",
		"./share/sources/orchestrator/",
	} {
		if name == directory {
			return entryContract{typeflag: tar.TypeDir, mode: 0o755}, true
		}
	}
	if !strings.HasPrefix(name, "./share/licenses/orchestrator/") &&
		!strings.HasPrefix(name, "./share/sources/orchestrator/") {
		return entryContract{}, false
	}
	clean := path.Clean(strings.TrimPrefix(name, "./"))
	if clean == "." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return entryContract{}, false
	}
	if strings.HasSuffix(name, "/") {
		if name != "./"+clean+"/" {
			return entryContract{}, false
		}
		return entryContract{typeflag: tar.TypeDir, mode: 0o755}, true
	}
	if name != "./"+clean {
		return entryContract{}, false
	}
	return entryContract{typeflag: tar.TypeReg, mode: 0o644}, true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: release-archive-validator <archive>")
		os.Exit(2)
	}
	if err := validateArchive(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "release archive: %v\n", err)
		os.Exit(1)
	}
}

func validateArchive(path string) error {
	return validateArchiveWithLimits(path, 512<<20, 1<<30, 20000)
}

func validateArchiveWithLimits(path string, maxMember, maxExpanded int64, maxMembers int) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > maxExpanded {
		return errors.New("compressed archive exceeds size limit")
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	seen := make(map[string]struct{}, len(archiveContract))
	limited := &io.LimitedReader{R: gzipReader, N: maxExpanded + 1}
	tarReader := tar.NewReader(limited)
	for {
		header, err := tarReader.Next()
		if limited.N <= 0 {
			return errors.New("expanded archive exceeds size limit")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		// Check before Next consumes any declared member body. The reader limit
		// also bounds padding, extension headers and concatenated gzip streams.
		if header.Size > maxMember {
			return fmt.Errorf("member %q exceeds size limit", header.Name)
		}
		if header.Size >= limited.N {
			return errors.New("expanded archive exceeds size limit")
		}
		if len(seen) >= maxMembers {
			return errors.New("archive exceeds member count limit")
		}

		contract, ok := archiveContract[header.Name]
		if !ok {
			contract, ok = materialContract(header.Name)
			if !ok {
				return fmt.Errorf("unexpected member %q", header.Name)
			}
		}
		if _, ok := seen[header.Name]; ok {
			return fmt.Errorf("duplicate member %q", header.Name)
		}
		seen[header.Name] = struct{}{}

		if !matchesType(header.Typeflag, contract.typeflag) {
			return fmt.Errorf("member %q has type %q, want %q", header.Name, header.Typeflag, contract.typeflag)
		}
		if header.Mode != contract.mode {
			return fmt.Errorf("member %q has mode %#o, want %#o", header.Name, header.Mode, contract.mode)
		}
		if header.Uid != 0 || header.Gid != 0 {
			return fmt.Errorf("member %q has owner %d/%d, want 0/0", header.Name, header.Uid, header.Gid)
		}
		if header.Uname != "" || header.Gname != "" {
			return fmt.Errorf("member %q stores owner names %q/%q", header.Name, header.Uname, header.Gname)
		}
		if header.Linkname != "" {
			return fmt.Errorf("member %q stores link target %q", header.Name, header.Linkname)
		}
	}
	// Consume only bounded zero tar padding; authenticate the gzip trailer too.
	var buffer [32 << 10]byte
	for {
		n, err := limited.Read(buffer[:])
		if limited.N <= 0 {
			return errors.New("expanded archive exceeds size limit")
		}
		for _, value := range buffer[:n] {
			if value != 0 {
				return errors.New("unexpected data after tar end marker")
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}

	for name := range archiveContract {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("missing member %q", name)
		}
	}
	return nil
}

func matchesType(actual, expected byte) bool {
	if expected == tar.TypeReg {
		return actual == tar.TypeReg || actual == tar.TypeRegA
	}
	return actual == expected
}
