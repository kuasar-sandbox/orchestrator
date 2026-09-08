package api

import (
	"fmt"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const identityHeader = "X-Kuasar-Sandbox-Identity"

// mergeIdentityHeader follows whole-namespace Header precedence. Validate both
// supplied layers so a valid Header cannot hide malformed body identity. Core
// still validates/extracts metadata for non-HTTP callers and freezes identity.
func mergeIdentityHeader(meta map[string]string, header http.Header) (map[string]string, error) {
	raw, err := singleOptionalHeader(header, identityHeader)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return meta, nil
	}
	if body, present := meta[sandboxcfg.NsIdentity]; present {
		if _, err := sandboxcfg.ParseIdentity(body); err != nil {
			return nil, err
		}
	}
	if _, err := sandboxcfg.ParseIdentity(*raw); err != nil {
		return nil, fmt.Errorf("%s: %w", identityHeader, err)
	}
	// mergeCreateConfigHeaders already owns the clone from mergeConfigHeaders.
	if meta == nil {
		meta = make(map[string]string)
	}
	meta[sandboxcfg.NsIdentity] = *raw
	return meta, nil
}
