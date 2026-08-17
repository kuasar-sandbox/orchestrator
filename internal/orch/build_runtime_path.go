package orch

import (
	"crypto/sha256"
	"encoding/base32"
	"path/filepath"
	"strings"
)

const buildRuntimeDigestBytes = 12

var buildRuntimeNameEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// buildRuntimeDir derives a fixed-length node-local directory from the complete
// opaque BuildID. BuildID remains the durable identity; only this reconstructible
// runtime path is compacted so nested sandbox Unix sockets stay below sun_path.
func buildRuntimeDir(runRoot, buildID string) string {
	sum := sha256.Sum256([]byte(buildID))
	digest := buildRuntimeNameEncoding.EncodeToString(sum[:buildRuntimeDigestBytes])
	return filepath.Join(runRoot, "build-"+strings.ToLower(digest))
}
