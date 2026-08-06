package sandboxcfg

import (
	"encoding/json"
	"fmt"
	"mime"
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

// Keep tenant-controlled response headers bounded independently of body size.
const maxMMDSContentTypeBytes = 256

// maxMMDSCanonicalBytes is a fixed protocol-level backstop on the canonical
// kuasar-sandbox.mmds form -- independent of, and in addition to, any
// operator-configured MMDSPolicy.MaxNamespaceBytes. This is a fixed
// invariant, not a tunable policy, so it applies as a hard ceiling
// regardless of how large an operator sets mmds.routes.max_namespace_bytes --
// the only policy either cluster or standalone MMDS has, since every node in
// a cluster deployment runs the same node-local mmds.routes configuration.
//
// The canonical form is later embedded verbatim as a string value inside
// other JSON-marshaled wire structures, which escapes it a second time (each
// already-escaped "\\" in the canonical form becomes "\\\\") -- a
// backslash-heavy specification can double in size on that second pass. Two
// such structures matter here, and this package cannot import either one to
// reference its limit directly (both import this package, directly or
// transitively, so the reverse import would cycle):
//
//  1. routesync.RouteEntry.MMDSRoutes / Command.Config, bounded by
//     routesync's fixed 1 MiB frame (maxFrame). orch.validateMMDSRouteEntryTransport
//     checks the real encoded RouteEntry at create/import time and rejects
//     (or, for a migration-carried value, drops) anything that would
//     actually overflow, so this constant is a coarse backstop for that path,
//     not the precise gate -- generous margin here is fine.
//  2. migrationtoken.MigrationTokenPayloadV1.Metadata, part of the
//     plaintext migrationtoken.Seal encrypts and base64-encodes into a token
//     bounded by migrationtoken.MaxWireSize (also 512 KiB) -- and that budget
//     is shared with Payload.Env, a second tenant-controlled, unbounded field
//     with no size relationship to MMDS at all. There is no equivalent
//     precise check for this path (deliberately -- see the discussion around
//     this constant's value), so this constant is the only mitigation: kept
//     well under MaxWireSize so a specification admitted here is very likely,
//     not guaranteed, to still fit in a migration token alongside Env and the
//     token's other fields once minted.
const maxMMDSCanonicalBytes = 128 * 1024

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

// MMDSPolicy is the admission policy ExtractMMDS enforces. Cluster mode has
// no registry-owned policy of its own, so every node in a cluster deployment
// runs the same node-local mmds.routes policy as standalone.
type MMDSPolicy struct {
	Enabled               bool
	MaxRoutesPerSandbox   int
	MaxSecretsPerSandbox  int
	MaxServicesPerSandbox int
	MaxStaticBodyBytes    int
	MaxNamespaceBytes     int
	ReservedPathPrefixes  []string
}

// ValidateMMDSReservedPathPrefixes validates operator-configured path
// reservations before they become admission policy. mmdsPathUnderPrefix
// matches a prefix against tenant route paths only after they have gone
// through canonicalMMDSPath, which never produces a wildcard, dot segment,
// empty segment, uppercase, or non-ASCII character -- so a prefix in any of
// those shapes can never actually match a real route and would silently
// leave the intended namespace unreserved. Reusing canonicalMMDSPath here
// (rather than a separate, looser check) keeps the two in lockstep as that
// function evolves. "/" is special-cased because it reserves every absolute
// route but is not itself a valid tenant path; a trailing slash is trimmed
// first because mmdsPathUnderPrefix treats "/internal" and "/internal/" as
// equivalent.
func ValidateMMDSReservedPathPrefixes(prefixes []string) error {
	for _, prefix := range prefixes {
		if prefix == "/" {
			continue
		}
		if _, err := canonicalMMDSPath(strings.TrimSuffix(prefix, "/")); err != nil {
			return fmt.Errorf("invalid prefix %q: %w", prefix, err)
		}
	}
	return nil
}

var mmdsNameRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

var mmdsPathSegRE = regexp.MustCompile(`^[a-z0-9._-]+$`)

// reservedMMDSPaths are the built-in internal/mmds guest routes; a tenant
// specification must not shadow them.
var reservedMMDSPaths = map[string]bool{
	"/":                 true,
	"/latest/api/token": true,
}

// ExtractMMDS applies policy and then strictly parses, canonicalizes, and
// validates metadata[NsMMDS]. Unlike ExtractCredentials it does not remove the namespace
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
	spec, out, err := normalizeMMDS(meta, raw)
	if err != nil {
		return MMDSSpec{}, nil, err
	}
	// The raw check above bounds the tenant's input, but json.Marshal inside
	// normalizeMMDS can expand it -- notably '<', '>', and '&' each become a
	// six-byte \uXXXX escape. Re-check the canonical form actually persisted
	// (and later re-encoded onto the routesync/mmdsrpc wire) so a policy near
	// the transport frame limit can't admit a spec that is only exploitable
	// after that expansion. MaxNamespaceBytes is documented as bounding "the
	// complete...JSON", which the canonical form is.
	if canonical := out[NsMMDS]; len(canonical) > policy.MaxNamespaceBytes {
		return MMDSSpec{}, nil, mmdsErrorWithPublic(fmt.Sprintf("canonical form exceeds the maximum size of %d bytes", policy.MaxNamespaceBytes), fmt.Sprintf("metadata exceeds the maximum size of %d bytes", policy.MaxNamespaceBytes))
	}
	if err := validateMMDSPolicy(spec, policy); err != nil {
		return MMDSSpec{}, nil, err
	}
	return spec, out, nil
}

// validateMMDSUnionNames validates the name field shared by secrets[] and
// services[] -- matches mmdsNameRE, no duplicates -- returning the declared
// name set on success. kind ("secret" | "service") only selects the error
// wording; any other field specific to one union member (e.g. a service's
// Target) is validated separately by the caller.
func validateMMDSUnionNames[T any](kind string, items []T, name func(T) string) (map[string]bool, error) {
	names := make(map[string]bool, len(items))
	for _, item := range items {
		n := name(item)
		if !mmdsNameRE.MatchString(n) {
			return nil, mmdsErrorWithPublic(
				fmt.Sprintf("%s name %q is invalid", kind, n),
				fmt.Sprintf("%s name is invalid", kind),
			)
		}
		if names[n] {
			return nil, mmdsError(fmt.Sprintf("specifies %s %q more than once", kind, n))
		}
		names[n] = true
	}
	return names, nil
}

// validateMMDSNamesReferenced rejects a declared (secret or service) name
// that no route actually uses -- dead configuration a tenant almost
// certainly didn't intend.
func validateMMDSNamesReferenced(kind string, declared, used map[string]bool) error {
	for name := range declared {
		if !used[name] {
			return mmdsError(fmt.Sprintf("specifies %s %q but no route references it", kind, name))
		}
	}
	return nil
}

// normalizeMMDS strictly parses and canonicalizes metadata[NsMMDS], enforcing
// only schema and protocol invariants -- the core ExtractMMDS layers policy
// enforcement around. It deliberately does not apply mutable operator
// admission policy on its own.
func normalizeMMDS(meta map[string]string, raw string) (MMDSSpec, map[string]string, error) {
	spec, routeFields, err := decodeMMDSSpec(raw)
	if err != nil {
		return MMDSSpec{}, nil, err
	}
	if spec.Version != 0 && spec.Version != 1 {
		return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("has unsupported version %d", spec.Version))
	}

	secretNames, err := validateMMDSUnionNames("secret", spec.Secrets, func(s MMDSSecretSpec) string { return s.Name })
	if err != nil {
		return MMDSSpec{}, nil, err
	}
	serviceNames, err := validateMMDSUnionNames("service", spec.Services, func(s MMDSServiceSpec) string { return s.Name })
	if err != nil {
		return MMDSSpec{}, nil, err
	}
	for _, s := range spec.Services {
		if s.Target == "" {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("service %q has an empty target", s.Name))
		}
		if s.Target != strings.TrimSpace(s.Target) {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("service %q target must not have leading or trailing whitespace", s.Name))
		}
	}

	usedSecrets := make(map[string]bool, len(spec.Secrets))
	usedServices := make(map[string]bool, len(spec.Services))
	paths := make(map[string]bool, len(spec.Routes))
	canonRoutes := make([]MMDSRouteSpec, len(spec.Routes))
	for i, r := range spec.Routes {
		path, perr := canonicalMMDSPath(r.Path)
		if perr != nil {
			return MMDSSpec{}, nil, mmdsErrorWithPublic(
				fmt.Sprintf("route path %q %v", r.Path, perr),
				fmt.Sprintf("route path %v", perr),
			)
		}
		r.Path = path

		fields := routeFields[i]
		if !fields.typeSet {
			r.Type = MMDSRouteStatic
		} else if r.Type == "" {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: type must not be empty", r.Path))
		}
		switch r.Type {
		case MMDSRouteStatic:
			if fields.secretNameSet || fields.serviceNameSet {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: static routes must not set secret_name/service_name", r.Path))
			}
			if !utf8.ValidString(r.Data) {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: data must be valid UTF-8", r.Path))
			}
			if !fields.contentTypeSet {
				r.ContentType = "application/octet-stream"
			} else if r.ContentType == "" {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: content_type must not be empty", r.Path))
			}
			if len(r.ContentType) > maxMMDSContentTypeBytes {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: content_type exceeds the maximum size of %d bytes", r.Path, maxMMDSContentTypeBytes))
			}
			if _, _, err := mime.ParseMediaType(r.ContentType); err != nil {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: content_type is not a valid media type", r.Path))
			}
		case MMDSRouteSecret:
			if fields.dataSet || fields.contentTypeSet || fields.serviceNameSet {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: secret routes must only set secret_name", r.Path))
			}
			if !secretNames[r.SecretName] {
				return MMDSSpec{}, nil, mmdsErrorWithPublic(
					fmt.Sprintf("route %q: secret_name %q is not specified", r.Path, r.SecretName),
					fmt.Sprintf("route %q: secret_name does not reference a specified secret", r.Path),
				)
			}
			usedSecrets[r.SecretName] = true
		case MMDSRouteService:
			if fields.dataSet || fields.contentTypeSet || fields.secretNameSet {
				return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route %q: service routes must only set service_name", r.Path))
			}
			if !serviceNames[r.ServiceName] {
				return MMDSSpec{}, nil, mmdsErrorWithPublic(
					fmt.Sprintf("route %q: service_name %q is not specified", r.Path, r.ServiceName),
					fmt.Sprintf("route %q: service_name does not reference a specified service", r.Path),
				)
			}
			usedServices[r.ServiceName] = true
		default:
			return MMDSSpec{}, nil, mmdsErrorWithPublic(
				fmt.Sprintf("route %q: unknown type %q", r.Path, r.Type),
				fmt.Sprintf("route %q: type must be %q, %q, or %q", r.Path, MMDSRouteStatic, MMDSRouteSecret, MMDSRouteService),
			)
		}

		if paths[path] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("specifies path %q more than once", path))
		}
		paths[path] = true
		if reservedMMDSPaths[path] {
			return MMDSSpec{}, nil, mmdsError(fmt.Sprintf("route path %q collides with a reserved path", path))
		}

		canonRoutes[i] = r
	}
	spec.Routes = canonRoutes

	if err := validateMMDSNamesReferenced("secret", secretNames, usedSecrets); err != nil {
		return MMDSSpec{}, nil, err
	}
	if err := validateMMDSNamesReferenced("service", serviceNames, usedServices); err != nil {
		return MMDSSpec{}, nil, err
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
	// Applies regardless of policy -- see maxMMDSCanonicalBytes's doc comment.
	// Checked on the canonical form, not the raw input already bounded above
	// in ExtractMMDS, because json.Marshal above can expand it past whatever
	// was checked going in.
	if len(canonical) > maxMMDSCanonicalBytes {
		return MMDSSpec{}, nil, mmdsErrorWithPublic(
			fmt.Sprintf("canonical form exceeds the maximum size of %d bytes", maxMMDSCanonicalBytes),
			fmt.Sprintf("metadata exceeds the maximum size of %d bytes", maxMMDSCanonicalBytes),
		)
	}
	return spec, out, nil
}

func validateMMDSPolicy(spec MMDSSpec, policy MMDSPolicy) error {
	if len(spec.Secrets) > policy.MaxSecretsPerSandbox {
		return mmdsError(fmt.Sprintf("specifies %d secrets, exceeding the limit of %d", len(spec.Secrets), policy.MaxSecretsPerSandbox))
	}
	if len(spec.Services) > policy.MaxServicesPerSandbox {
		return mmdsError(fmt.Sprintf("specifies %d services, exceeding the limit of %d", len(spec.Services), policy.MaxServicesPerSandbox))
	}
	if len(spec.Routes) > policy.MaxRoutesPerSandbox {
		return mmdsError(fmt.Sprintf("specifies %d routes, exceeding the limit of %d", len(spec.Routes), policy.MaxRoutesPerSandbox))
	}
	for _, route := range spec.Routes {
		if route.Type == MMDSRouteStatic && len(route.Data) > policy.MaxStaticBodyBytes {
			return mmdsError(fmt.Sprintf("route %q: data exceeds the maximum size of %d bytes", route.Path, policy.MaxStaticBodyBytes))
		}
		if reservedMMDSPathCollision(route.Path, policy.ReservedPathPrefixes) {
			return mmdsError(fmt.Sprintf("route path %q collides with a reserved path", route.Path))
		}
	}
	return nil
}

// LookupMMDSRoute decodes raw -- a sandbox's canonical NsMMDS specification
// value, already persisted (produced by ExtractMMDS) -- and returns the route
// at path, if specified. No validation is repeated -- the stored form is
// already canonical, so a decode failure (e.g. absent/corrupt) is simply "not
// found". Takes the raw string directly (not a namespace map) since every
// caller already holds it standalone and would otherwise wrap it in a
// throwaway single-entry map on every guest MMDS request.
func LookupMMDSRoute(raw, path string) (MMDSRouteSpec, bool) {
	if raw == "" {
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
// reserved prefix intends. The root prefix reserves every absolute route. A
// trailing slash on any other prefix is accepted and ignored, so "/internal"
// and "/internal/" are equivalent.
func mmdsPathUnderPrefix(path, prefix string) bool {
	if prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return false
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
