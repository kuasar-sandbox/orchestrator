package configsock

import (
	"crypto/sha256"
	"encoding/base32"
	"path/filepath"
	"strings"
)

const buildRuntimeDigestBytes = 12

var buildRuntimeNameEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// BuildRuntimeDir derives the fixed-length node-local directory shared by the
// orchestrator and run-builder from the complete opaque BuildID.
func BuildRuntimeDir(runRoot, buildID string) string {
	sum := sha256.Sum256([]byte(buildID))
	digest := buildRuntimeNameEncoding.EncodeToString(sum[:buildRuntimeDigestBytes])
	return filepath.Join(runRoot, "build-"+strings.ToLower(digest))
}

// BuildPidfile returns the second, assignment-bound pidfile for run-builder.
func BuildPidfile(runRoot, buildID string) string {
	return filepath.Join(BuildRuntimeDir(runRoot, buildID), "builder.pid")
}
