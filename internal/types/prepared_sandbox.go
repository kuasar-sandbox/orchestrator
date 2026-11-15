package types

import "github.com/kuasar-sandbox/accelerator/pkg/manifest"

// IsPreparedSandboxRef admits only the content-addressed local E written
// by the sandbox task to its own checkpoint directory.
func IsPreparedSandboxRef(raw string) bool {
	ref, err := manifest.ParseRef(raw)
	if err != nil || ref.String() != raw || ref.Scheme != manifest.RefSchemeFile ||
		ref.Location != "" || (ref.DigestScheme != "digest" && ref.DigestScheme != "hmac") ||
		ref.Path != ref.Digest+".sandbox" {
		return false
	}
	return true
}
