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

// PublicationName derives the location name for one publication of entityID
// (a sandbox stable id or a build id): the bare entity id. The name — not
// any date or node state — is the directory key, so every publication of one
// logical entity lands in one directory and content-addressed files
// accumulate there as versions.
func PublicationName(entityID string) string {
	return entityID
}

// Resolve derives <parent>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>.
// The name is the entity key (see PublicationName), keeping the derivation a
// pure function of the name: the name travels inside the portable ref, so
// restore/import/inheritance resolve the identical path on any node without
// extra state. The SHA256 fan-out bounds directory size. The per-entity
// directory accumulates content-addressed versions; whether and how anything
// is deleted is owned by the future management-plane GC and is deliberately
// not this package's contract.
// parentURI must be an absolute hostless file URI and name must be a valid
// portable ref-location name.
func Resolve(parentURI, name string) (Location, error) {
	if name == "" {
		return Location{}, fmt.Errorf("invalid ref location name %q: empty", name)
	}
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
