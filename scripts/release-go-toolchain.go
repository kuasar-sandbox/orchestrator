//go:build ignore

// Check a checksum-database-authenticated toolchain ZIP, not a mutable GOROOT
// license directory. The caller authenticates the expected module h1 with Go.
package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var distributionVersion = regexp.MustCompile(`^v0\.0\.1-(go[0-9]+\.[0-9]+(?:\.[0-9]+|(?:beta|rc)[0-9]+))\.linux-amd64$`)

func fileDigest(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func licensePath(name string) bool {
	base := strings.ToUpper(path.Base(name))
	// Notices are also shipped alongside vendored compiler/standard-library
	// dependencies. Keep their paths; do not mistake copyright_test.go or
	// similarly named source files for license text.
	switch strings.ToLower(path.Ext(base)) {
	case ".go", ".c", ".h", ".s", ".rs", ".py":
		return false
	}
	for _, directory := range strings.Split(path.Dir(name), "/") {
		if strings.EqualFold(directory, "LICENSES") {
			return true
		}
	}
	for _, prefix := range []string{"LICENSE", "LICENCE", "COPYING", "NOTICE", "PATENTS", "AUTHORS", "CREDITS", "COPYRIGHT"} {
		if base == prefix || strings.HasPrefix(base, prefix+".") || strings.HasPrefix(base, prefix+"-") {
			return true
		}
	}
	return false
}

func checkInstalled(root string, expected map[string]string) error {
	seen := make(map[string]bool)
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "." {
			if !entry.IsDir() {
				return errors.New("GOROOT is not a directory")
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		// The binary module renames go.mod to _go.mod. Standard installations
		// retain go.mod; automatic toolchain setup can contain both names.
		canonical := relative
		if path.Base(relative) == "go.mod" {
			candidate := path.Join(path.Dir(relative), "_go.mod")
			if _, ok := expected[candidate]; ok {
				canonical = candidate
			}
		}
		want, known := expected[canonical]
		if !known {
			// Full tarball distributions include non-build API, documentation,
			// misc and test inputs omitted from the compact toolchain module.
			top, _, _ := strings.Cut(relative, "/")
			if top == "api" || top == "doc" || top == "misc" || top == "test" {
				return nil
			}
			return fmt.Errorf("unexpected installed Go build input: %s", relative)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("installed Go input is not a regular file: %s", relative)
		}
		got, err := fileDigest(name)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("installed Go input differs from authenticated distribution: %s", relative)
		}
		seen[canonical] = true
		return nil
	})
	if err != nil {
		return err
	}
	for name := range expected {
		if !seen[name] {
			return fmt.Errorf("installed Go distribution input is missing: %s", name)
		}
	}
	return nil
}

func verifyDistribution(archive, version, expectedH1, root, destination string) (int, error) {
	match := distributionVersion.FindStringSubmatch(version)
	if match == nil {
		return 0, errors.New("unsupported Go toolchain module identity")
	}
	checksum, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(expectedH1, "h1:"))
	if err != nil || !strings.HasPrefix(expectedH1, "h1:") || len(checksum) != sha256.Size {
		return 0, errors.New("invalid authenticated Go module h1")
	}
	z, err := zip.OpenReader(archive)
	if err != nil {
		return 0, err
	}
	defer z.Close()
	files := append([]*zip.File(nil), z.File...)
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	prefix := "golang.org/toolchain@" + version + "/"
	aggregate := sha256.New()
	digests := make(map[string]string)
	licenses := make(map[string][]byte)
	seen := make(map[string]bool)
	var expanded uint64
	versionText := ""
	for _, file := range files {
		relative := strings.TrimPrefix(file.Name, prefix)
		if relative == file.Name || relative == "" || seen[file.Name] ||
			strings.ContainsAny(relative, "\\\r\n\t") || strings.HasPrefix(relative, "/") ||
			path.Clean(relative) != strings.TrimSuffix(relative, "/") ||
			relative == ".." || strings.HasPrefix(relative, "../") {
			return 0, errors.New("unsafe or duplicate Go distribution ZIP entry")
		}
		seen[file.Name] = true
		if !file.Mode().IsRegular() && !file.FileInfo().IsDir() {
			return 0, errors.New("non-regular Go distribution ZIP entry")
		}
		expanded += file.UncompressedSize64
		if file.UncompressedSize64 > 1<<30 || expanded > 2<<30 {
			return 0, errors.New("Go distribution ZIP exceeds input size limit")
		}
		stream, err := file.Open()
		if err != nil {
			return 0, err
		}
		hash := sha256.New()
		var contents []byte
		if licensePath(relative) || relative == "VERSION" {
			contents, err = io.ReadAll(io.LimitReader(stream, 16<<20+1))
			if len(contents) > 16<<20 {
				err = errors.New("Go license/version material exceeds input limit")
			}
			_, _ = hash.Write(contents)
		} else {
			_, err = io.Copy(hash, stream)
		}
		closeErr := stream.Close()
		if err != nil {
			return 0, err
		}
		if closeErr != nil {
			return 0, closeErr
		}
		digest := hex.EncodeToString(hash.Sum(nil))
		fmt.Fprintf(aggregate, "%s  %s\n", digest, file.Name)
		if file.FileInfo().IsDir() {
			continue
		}
		digests[relative] = digest
		if licensePath(relative) {
			if len(contents) == 0 {
				return 0, fmt.Errorf("empty Go license material: %s", relative)
			}
			licenses[relative] = contents
		}
		if relative == "VERSION" {
			versionText, _, _ = strings.Cut(string(contents), "\n")
		}
	}
	actualH1 := "h1:" + base64.StdEncoding.EncodeToString(aggregate.Sum(nil))
	if actualH1 != expectedH1 {
		return 0, errors.New("Go distribution ZIP differs from authenticated module h1")
	}
	for _, required := range []string{"bin/go", "pkg/tool/linux_amd64/compile", "src/runtime/runtime.go", "LICENSE", "VERSION"} {
		if _, ok := digests[required]; !ok {
			return 0, fmt.Errorf("Go distribution is missing %s", required)
		}
	}
	if versionText != match[1] {
		return 0, errors.New("Go distribution VERSION differs from module identity")
	}
	if root != "-" {
		if err := checkInstalled(root, digests); err != nil {
			return 0, err
		}
	}
	// No output is created until ZIP authentication and installed-input checks
	// have succeeded. Only license/notice files, never executables, are extracted.
	if err := os.Mkdir(destination, 0o755); err != nil {
		return 0, err
	}
	for name, contents := range licenses {
		target := filepath.Join(destination, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return 0, err
		}
		if err := os.WriteFile(target, contents, 0o644); err != nil {
			return 0, err
		}
	}
	return len(digests), nil
}

func main() {
	if len(os.Args) != 6 {
		fmt.Fprintln(os.Stderr, "usage: release-go-toolchain <zip> <module-version> <authenticated-h1> <goroot-or-dash> <new-license-directory>")
		os.Exit(2)
	}
	count, err := verifyDistribution(os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5])
	if err != nil {
		fmt.Fprintln(os.Stderr, "release-go-toolchain:", err)
		os.Exit(1)
	}
	fmt.Printf("verified %d authenticated Go distribution inputs\n", count)
}
