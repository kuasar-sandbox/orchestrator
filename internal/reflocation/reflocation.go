// Package reflocation derives node-local storage locations for portable named
// file refs. It is shared by conductor-side CLI construction and task-local
// reader path maps so both planes use one deterministic rule.
package reflocation

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"path"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
)

// Location keeps the two intentionally distinct representations of one named
// location: Path belongs in sandboxer config.RefLocations, while URI belongs on
// sandbox-ctl command lines.
type Location struct {
	Path string
	URI  string
}

// Resolve derives <parent>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>.
// parentURI must be an absolute hostless file URI and name must be a valid
// portable ref-location name.
func Resolve(parentURI, name string) (Location, error) {
	probe := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: "probe", Location: name}
	if err := probe.Validate(); err != nil {
		return Location{}, fmt.Errorf("invalid ref location name %q: %w", name, err)
	}
	if parentURI == "" {
		return Location{}, fmt.Errorf("ref_location_parent is not configured")
	}
	u, err := url.Parse(parentURI)
	if err != nil {
		return Location{}, fmt.Errorf("parse ref_location_parent: %w", err)
	}
	if u.Scheme != "file" || u.Host != "" || u.Path == "" || !path.IsAbs(u.Path) ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return Location{}, fmt.Errorf("ref_location_parent must be file:///absolute/path without host, query, or fragment")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	u.Path = path.Join(u.Path, digest[:2], digest[2:4], name)
	u.RawPath = ""
	return Location{Path: u.Path, URI: u.String()}, nil
}
