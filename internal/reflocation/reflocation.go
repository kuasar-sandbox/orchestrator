// Package reflocation derives node-local storage locations for portable named
// file refs. It is shared by conductor-side CLI construction and task-local
// reader path maps so both planes use one deterministic rule.
package reflocation

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
)

// Location keeps the two intentionally distinct representations of one named
// location: Path belongs in sandboxer config.RefLocations, while URI belongs on
// sandbox-ctl command lines.
type Location struct {
	Path string
	URI  string
}

// dateLayout is the fixed-width publication-date suffix appended to entity ids
// in location names. Its lexicographic order equals chronological order, so the
// first path segment derived from it forms a time-ordered GC bucket.
const dateLayout = "20060102"

// PublicationName derives the location name for one publication of entityID
// (sandbox ID or build ID): the entity id plus the publication date,
// e.g. "019f...-20260824". The date suffix is what the time-ordered layout
// below buckets by — publication time, not entity creation time, so an entity
// created long before it exports still lands in a current bucket and a GC can
// never delete a just-published snapshot.
func PublicationName(entityID string, publishedAt time.Time) string {
	return entityID + "-" + publishedAt.UTC().Format(dateLayout)
}

// publicationDate extracts the 8-digit date suffix from a publication name.
func publicationDate(name string) (string, bool) {
	if len(name) < len(dateLayout)+1 {
		return "", false
	}
	suffix := name[len(name)-len(dateLayout):]
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	if name[len(name)-len(dateLayout)-1] != '-' {
		return "", false
	}
	// The suffix must be a real calendar date, not just 8 digits: Feb 30 or
	// month 13 never resolve, so such names can never alias a bucket.
	if _, err := time.Parse(dateLayout, suffix); err != nil {
		return "", false
	}
	return suffix, true
}

// Resolve derives <parent>/<publication-date>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>.
// The date segment is parsed from the name's publication-date suffix (see
// PublicationName), keeping the derivation a pure function of the name: the
// same suffix travels inside the portable ref, so restore/import/inheritance
// resolve the identical path on any node without extra state. The SHA256
// fan-out below the date segment bounds directory size and is unchanged from
// the pre-date layout. A bucket (one date) is removable once every publication
// time it can represent is older than the retention cutoff.
// parentURI must be an absolute hostless file URI and name must be a valid
// portable ref-location name carrying a publication-date suffix.
func Resolve(parentURI, name string) (Location, error) {
	probe := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: "probe", Location: name}
	if err := probe.Validate(); err != nil {
		return Location{}, fmt.Errorf("invalid ref location name %q: %w", name, err)
	}
	date, ok := publicationDate(name)
	if !ok {
		return Location{}, fmt.Errorf("invalid ref location name %q: want <entity-id>-%s publication-date suffix", name, dateLayoutExample)
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
	u.Path = path.Join(u.Path, date, digest[:2], digest[2:4], name)
	u.RawPath = ""
	return Location{Path: u.Path, URI: u.String()}, nil
}

// dateLayoutExample documents the expected suffix shape in error messages.
const dateLayoutExample = "YYYYMMDD"
