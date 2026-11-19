// Package difftemplate adapts the node's historical host-path configuration to
// sandboxer's shared template-reference contract.
package difftemplate

import (
	"net/url"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// Normalize accepts an optional template URI or a legacy host path. It does not
// access the source: authentication and capacity checks belong to the selected
// tenant operation. Complete URIs are never converted into filesystem paths.
func Normalize(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	parsed, err := url.Parse(raw)
	if strings.Contains(raw, "://") || err == nil && parsed.Scheme != "" {
		return rtconfig.NormalizeDiffTemplate(raw)
	}
	path, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	return rtconfig.NormalizeDiffTemplate((manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path}).String())
}
