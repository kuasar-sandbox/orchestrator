package util

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// LocateBinary resolves an executable, preferring a copy that ships next to
// the running binary over a $PATH-installed one. For a bare name (no path
// separator) it tries <dir-of-running-executable>/<name> first, then
// exec.LookPath(name) ($PATH). A name that already contains a separator is a
// path and is resolved as-is via exec.LookPath (cwd-relative or absolute).
// Returns the resolved path, or an error if not found.
//
// The exe-dir preference covers the release-bundle layout where helper
// binaries ship alongside the tool under bin/<arch>/; $PATH covers
// distro-installed equivalents. Callers that also honour an env override
// (e.g. SANDBOX_CH_PATH) check that before calling this.
func LocateBinary(name string) (string, error) {
	if name != "" && !strings.ContainsRune(name, os.PathSeparator) {
		if exe, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(exe), name)
			if _, err := os.Stat(candidate); err == nil {
				return candidate, nil
			}
		}
	}
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("%q not found (tried exe-dir, $PATH)", name)
}
