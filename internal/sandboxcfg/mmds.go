package sandboxcfg

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// MMDS route types. Only MMDSRouteStatic is served today; MMDSRouteSecret and
// MMDSRouteService are specified and validated now so their guest-visible paths
// are stable and immutable from the start, but their backends are not yet
// implemented (until then the guest GET returns 503).
const (
	MMDSRouteStatic  = "static"
	MMDSRouteSecret  = "secret"
	MMDSRouteService = "service"
)

// MMDSSpec is the tenant-specified kuasar-sandbox.mmds namespace: named secrets
// and services plus routes mapping an exact guest-visible path to one of them
// (or to inline static content). ExtractMMDS always returns a canonical form:
// every route has an explicit Type and, for static routes, an explicit
// ContentType.
type MMDSSpec struct {
	Version  int               `json:"version,omitempty"`
	Secrets  []MMDSSecretSpec  `json:"secrets,omitempty"`
	Services []MMDSServiceSpec `json:"services,omitempty"`
	Routes   []MMDSRouteSpec   `json:"routes,omitempty"`
}

// MMDSSecretSpec specifies a secret name a route may reference. The value
// itself is never carried here -- it is written later through a separate
// admin API, not yet implemented.
type MMDSSecretSpec struct {
	Name string `json:"name"`
}

// MMDSServiceSpec specifies a service alias a route may reference. Target must
// name a service the node operator has registered with a service registry
// that does not exist yet; today ExtractMMDS only validates that Target is
// non-empty.
type MMDSServiceSpec struct {
	Name   string `json:"name"`
	Target string `json:"target"`
}

// MMDSRouteSpec is one guest-visible exact-path route. Exactly the fields for
// Type are populated after canonicalization: static uses ContentType/Data,
// secret uses SecretName, service uses ServiceName.
type MMDSRouteSpec struct {
	Path        string `json:"path"`
	Type        string `json:"type,omitempty"`
	SecretName  string `json:"secret_name,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Data        string `json:"data,omitempty"`
}

// MMDSPolicy is the node-operator policy ExtractMMDS enforces.
type MMDSPolicy struct {
	Enabled               bool
	MaxRoutesPerSandbox   int
	MaxSecretsPerSandbox  int
	MaxServicesPerSandbox int
	MaxStaticBodyBytes    int
	MaxNamespaceBytes     int
	ReservedPathPrefixes  []string
}

var mmdsNameRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

var mmdsPathSegRE = regexp.MustCompile(`^[a-z0-9._-]+$`)

// reservedMMDSPaths are the built-in internal/mmds guest routes; a tenant
// specification must not shadow them.
var reservedMMDSPaths = map[string]bool{
	"/":                 true,
	"/latest/api/token": true,
}

// ExtractMMDS strictly parses, canonicalizes, and validates metadata[NsMMDS]
// against policy. Unlike ExtractCredentials it does not remove the namespace
// on success: it rewrites metadata[NsMMDS] to the canonical form (explicit
// "type" on every route, static shorthand expanded, default content_type
// filled in) in a clone of meta, and returns that clone -- the caller persists
// it as sb.Metadata so the specification stays visible on Get/List/export,
// unlike credentials. meta is returned unchanged (the same map) when the
// namespace is absent. policy.Enabled=false with the namespace present is
// rejected before any tenant-controlled JSON is parsed.
func ExtractMMDS(meta map[string]string, policy MMDSPolicy) (MMDSSpec, map[string]string, error) {
	raw, ok := meta[NsMMDS]
	if !ok {
		return MMDSSpec{}, meta, nil
	}
	if !policy.Enabled {
		return MMDSSpec{}, nil, mmdsError("MMDS routes are disabled by policy")
	}
	if len(raw) > policy.MaxNamespaceBytes {
		return MMDSSpec{}, nil, mmdsErrorWithPublic(fmt.Sprintf("exceeds the maximum size of %d bytes", policy.MaxNamespaceBytes), fmt.Sprintf("metadata exceeds the maximum size of %d bytes", policy.MaxNamespaceBytes))
	}

	var spec MMDSSpec
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return MMDSSpec{}, nil, invalidMMDSJSONError(err)
	}
	// Decode only consumes one JSON value; reject anything trailing it (a
	// second concatenated document, stray text) rather than silently ignoring
	// it. dec.More() is not a valid check for this: it reports whether the
	// *current* array/object has another element pending, not whether
	// unconsumed bytes remain in the stream once the top-level value is
	// fully read -- it wrongly returns false (accepting the input) when the
	// trailing content happens to start with a JSON structural byte, e.g.
	// `{"version":1}]`. Decoding once more is reliable: it must be exactly
	// io.EOF once the stream is legitimately exhausted -- a second value or
	// any other error means reject.
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return MMDSSpec{}, nil, mmdsError("contains unexpected trailing data")
	}
	if spec.Version != 0 && spec.Version != 1 {
		return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("has unsupported version %d", spec.Version))
	}

	secretNames := make(map[string]bool, len(spec.Secrets))
	for _, s := range spec.Secrets {
		if !mmdsNameRE.MatchString(s.Name) {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("secret name %q is invalid", s.Name))
		}
		if secretNames[s.Name] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies secret %q more than once", s.Name))
		}
		secretNames[s.Name] = true
	}
	serviceNames := make(map[string]bool, len(spec.Services))
	for _, s := range spec.Services {
		if !mmdsNameRE.MatchString(s.Name) {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("service name %q is invalid", s.Name))
		}
		if serviceNames[s.Name] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies service %q more than once", s.Name))
		}
		if strings.TrimSpace(s.Target) == "" {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("service %q has an empty target", s.Name))
		}
		serviceNames[s.Name] = true
	}

	if len(spec.Secrets) > policy.MaxSecretsPerSandbox {
		return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies %d secrets, exceeding the limit of %d", len(spec.Secrets), policy.MaxSecretsPerSandbox))
	}
	if len(spec.Services) > policy.MaxServicesPerSandbox {
		return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies %d services, exceeding the limit of %d", len(spec.Services), policy.MaxServicesPerSandbox))
	}
	if len(spec.Routes) > policy.MaxRoutesPerSandbox {
		return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies %d routes, exceeding the limit of %d", len(spec.Routes), policy.MaxRoutesPerSandbox))
	}

	usedSecrets := make(map[string]bool, len(spec.Secrets))
	usedServices := make(map[string]bool, len(spec.Services))
	paths := make(map[string]bool, len(spec.Routes))
	canonRoutes := make([]MMDSRouteSpec, len(spec.Routes))
	for i, r := range spec.Routes {
		if r.Type == "" {
			r.Type = MMDSRouteStatic
		}
		switch r.Type {
		case MMDSRouteStatic:
			if r.SecretName != "" || r.ServiceName != "" {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: static routes must not set secret_name/service_name", r.Path))
			}
			if !utf8.ValidString(r.Data) {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: data must be valid UTF-8", r.Path))
			}
			if len(r.Data) > policy.MaxStaticBodyBytes {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: data exceeds the maximum size of %d bytes", r.Path, policy.MaxStaticBodyBytes))
			}
			if r.ContentType == "" {
				r.ContentType = "application/octet-stream"
			}
		case MMDSRouteSecret:
			if r.Data != "" || r.ContentType != "" || r.ServiceName != "" {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: secret routes must only set secret_name", r.Path))
			}
			if !secretNames[r.SecretName] {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: secret_name %q is not specified", r.Path, r.SecretName))
			}
			usedSecrets[r.SecretName] = true
		case MMDSRouteService:
			if r.Data != "" || r.ContentType != "" || r.SecretName != "" {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: service routes must only set service_name", r.Path))
			}
			if !serviceNames[r.ServiceName] {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: service_name %q is not specified", r.Path, r.ServiceName))
			}
			usedServices[r.ServiceName] = true
		default:
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: unknown type %q", r.Path, r.Type))
		}

		path, perr := canonicalMMDSPath(r.Path)
		if perr != nil {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route path %q %v", r.Path, perr))
		}
		r.Path = path
		if paths[path] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies path %q more than once", path))
		}
		paths[path] = true
		if reservedMMDSPathCollision(path, policy.ReservedPathPrefixes) {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route path %q collides with a reserved path", path))
		}

		canonRoutes[i] = r
	}
	spec.Routes = canonRoutes

	for name := range secretNames {
		if !usedSecrets[name] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies secret %q but no route references it", name))
		}
	}
	for name := range serviceNames {
		if !usedServices[name] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies service %q but no route references it", name))
		}
	}

	spec.Version = 1
	canonical, err := json.Marshal(spec)
	if err != nil {
		return MMDSSpec{}, nil, fmt.Errorf("sandboxcfg: metadata[%q]: %w", NsMMDS, err)
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	out[NsMMDS] = string(canonical)
	return spec, out, nil
}

// LookupMMDSRoute decodes the canonical NsMMDS specification already persisted
// in meta (produced by ExtractMMDS) and returns the route at path, if
// specified. No validation is repeated -- the stored form is already
// canonical, so a decode failure (e.g. absent/corrupt) is simply "not found".
func LookupMMDSRoute(meta map[string]string, path string) (MMDSRouteSpec, bool) {
	raw, ok := meta[NsMMDS]
	if !ok {
		return MMDSRouteSpec{}, false
	}
	var spec MMDSSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return MMDSRouteSpec{}, false
	}
	for _, r := range spec.Routes {
		if r.Path == path {
			return r, true
		}
	}
	return MMDSRouteSpec{}, false
}

// MMDSSpecifiesSecretName reports whether meta's canonical NsMMDS specification
// specifies a secret with this name. ExtractMMDS already guarantees every
// specified secret is referenced by at least one route, so this alone is
// sufficient for the admin API to reject a name that was never part of the
// sandbox's specification (e.g. an operator typo).
func MMDSSpecifiesSecretName(meta map[string]string, name string) bool {
	raw, ok := meta[NsMMDS]
	if !ok {
		return false
	}
	var spec MMDSSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return false
	}
	for _, s := range spec.Secrets {
		if s.Name == name {
			return true
		}
	}
	return false
}

// MMDSConfigDigest returns the hex-encoded SHA-256 digest of meta's canonical
// NsMMDS specification, or "" if absent. Used as part of the secretbox AAD
// binding a sandbox's encrypted MMDS secret blob to its specification, so
// ciphertext cannot be replayed under a different specification state.
func MMDSConfigDigest(meta map[string]string) string {
	raw, ok := meta[NsMMDS]
	if !ok {
		return ""
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// canonicalMMDSPath validates raw as an absolute, exact, unambiguous guest
// path: no percent-escapes, query, fragment, empty/dot/wildcard segments, or
// trailing slash, and not the bare root. Segment characters are restricted to
// a safe bounded ASCII set.
func canonicalMMDSPath(raw string) (string, error) {
	if raw == "" || raw[0] != '/' {
		return "", fmt.Errorf("must be an absolute path")
	}
	if raw == "/" {
		return "", fmt.Errorf("must not be the bare root path")
	}
	if strings.ContainsAny(raw, "?#") {
		return "", fmt.Errorf("must not contain a query or fragment")
	}
	if strings.Contains(raw, "%") {
		return "", fmt.Errorf("must not contain percent-escapes")
	}
	if strings.HasSuffix(raw, "/") {
		return "", fmt.Errorf("must not have a trailing slash")
	}
	for _, seg := range strings.Split(raw[1:], "/") {
		if seg == "" {
			return "", fmt.Errorf("must not contain an empty segment")
		}
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("must not contain a dot segment")
		}
		if strings.Contains(seg, "*") {
			return "", fmt.Errorf("must not contain a wildcard")
		}
		if !mmdsPathSegRE.MatchString(seg) {
			return "", fmt.Errorf("contains an invalid character")
		}
	}
	return raw, nil
}

func reservedMMDSPathCollision(path string, extraPrefixes []string) bool {
	if reservedMMDSPaths[path] {
		return true
	}
	for _, p := range extraPrefixes {
		if mmdsPathUnderPrefix(path, p) {
			return true
		}
	}
	return false
}

// mmdsPathUnderPrefix reports whether path equals prefix or falls under it as
// a path segment -- a raw strings.HasPrefix would also match an unrelated
// sibling that merely shares a textual prefix (e.g. prefix "/internal" would
// wrongly block "/internal2/foo"), which is not what an operator declaring a
// reserved prefix intends. A trailing slash on prefix is accepted and ignored,
// so "/internal" and "/internal/" are equivalent.
func mmdsPathUnderPrefix(path, prefix string) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return false
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
