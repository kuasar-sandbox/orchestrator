// Package mmdscfg parses and validates the kuasar-sandbox.mmds Create
// metadata namespace: MMDS endpoints backed by either an operator-managed
// "store" value or an SSRF-hardened "relay" to a fixed upstream. Extract
// mirrors internal/buildcfg.Extract's shape (strip the
// namespace, return a clean map) rather than internal/sandboxcfg.ParseSpec's
// (which folds straight into guest-visible SANDBOX_CONFIG.metadata) — endpoint
// definitions must never reach the guest that way.
package mmdscfg

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"gopkg.in/yaml.v3"
)

// Ns is the sandbox Create metadata namespace carrying the endpoint
// declarations. Accepted only by sandbox Create; template register/build
// reject its presence outright (see orch/build.go).
const Ns = "kuasar-sandbox.mmds"

// Backend type tags.
const (
	BackendStore = "store"
	BackendRelay = "relay"
)

// EndpointSpec is one declared endpoint definition: immutable for the
// sandbox's lifetime once Create succeeds.
type EndpointSpec struct {
	Name    string
	Path    string
	Backend BackendSpec
}

// BackendSpec is the tagged-union backend. Type selects which of the
// following fields are populated; Extract rejects any field belonging to the
// other variant.
type BackendSpec struct {
	Type string // BackendStore | BackendRelay

	// Relay-only.
	URL  string
	Auth *RelayAuthSpec
}

// RelayAuthSpec names the header the relay injects the operator-managed auth
// value under. The value itself is never accepted here — only via the admin
// API (internal/configsock's mmds admin routes).
type RelayAuthSpec struct {
	HeaderName string
}

var (
	nameRE    = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	pathSegRE = regexp.MustCompile(`^[a-z0-9._-]+$`)
)

// rawDoc / rawEndpoint mirror the YAML shape decoded with KnownFields(true) so
// a typo'd top-level or endpoint-level key is rejected. backend stays a loose
// map so parseBackend can positively assert "no extra keys for this variant" —
// a tagged union KnownFields(true) alone can't express across Go struct tags.
type rawDoc struct {
	Endpoints []rawEndpoint `yaml:"endpoints"`
}

type rawEndpoint struct {
	Name    string         `yaml:"name"`
	Path    string         `yaml:"path"`
	Backend map[string]any `yaml:"backend"`
}

// Extract removes the mmds namespace from meta and decodes+validates it into
// EndpointSpecs, bounded by limits. A namespace key absent from meta
// entirely returns meta unchanged and a nil endpoint slice — not an error.
// A namespace key that is *present* — even if blank/whitespace-only — is a
// declaration attempt: when limits.Enabled is false it is always an error
// (the configuration is never silently ignored, independent of whether it
// would otherwise validate), and either way the key is always stripped from
// the returned map so a blank value can never leak into guest-visible
// metadata the way returning the original, unstripped meta would.
func Extract(meta map[string]string, limits config.MMDSEndpointsConfig) (map[string]string, []EndpointSpec, error) {
	rawVal, present := meta[Ns]
	if !present {
		return meta, nil, nil
	}
	if !limits.Enabled {
		return nil, nil, fmt.Errorf("mmdscfg: metadata[%q] present but mmds.endpoints.enabled=false", Ns)
	}
	raw := strings.TrimSpace(rawVal)
	if raw == "" {
		// Blank is not a validation error (mirrors an absent namespace's
		// "no endpoints declared" outcome) but the key must still never
		// survive into the returned map.
		return stripNamespace(meta), nil, nil
	}
	if limits.MaxMetadataBytes > 0 && len(raw) > limits.MaxMetadataBytes {
		return nil, nil, fmt.Errorf("mmdscfg: metadata[%q] is %d bytes, exceeds max_metadata_bytes %d", Ns, len(raw), limits.MaxMetadataBytes)
	}

	var doc rawDoc
	dec := yaml.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, nil, fmt.Errorf("mmdscfg: metadata[%q] is not valid JSON: %w", Ns, err)
	}
	if len(doc.Endpoints) == 0 {
		return nil, nil, fmt.Errorf("mmdscfg: endpoints must declare at least one entry")
	}
	if limits.MaxEndpointsPerSandbox > 0 && len(doc.Endpoints) > limits.MaxEndpointsPerSandbox {
		return nil, nil, fmt.Errorf("mmdscfg: %d endpoints exceeds max_endpoints_per_sandbox %d", len(doc.Endpoints), limits.MaxEndpointsPerSandbox)
	}

	names := make(map[string]bool, len(doc.Endpoints))
	paths := make(map[string]bool, len(doc.Endpoints))
	out := make([]EndpointSpec, 0, len(doc.Endpoints))
	for i, re := range doc.Endpoints {
		ep, err := parseEndpoint(re, limits)
		if err != nil {
			return nil, nil, fmt.Errorf("mmdscfg: endpoints[%d]: %w", i, err)
		}
		if names[ep.Name] {
			return nil, nil, fmt.Errorf("mmdscfg: endpoints[%d]: duplicate name %q", i, ep.Name)
		}
		if paths[ep.Path] {
			return nil, nil, fmt.Errorf("mmdscfg: endpoints[%d]: duplicate path %q", i, ep.Path)
		}
		names[ep.Name] = true
		paths[ep.Path] = true
		out = append(out, ep)
	}

	return stripNamespace(meta), out, nil
}

// stripNamespace returns a copy of meta with the mmds namespace key removed
// — the guest must never see it, blank or not.
func stripNamespace(meta map[string]string) map[string]string {
	clean := make(map[string]string, len(meta))
	for k, v := range meta {
		if k != Ns {
			clean[k] = v
		}
	}
	return clean
}

func parseEndpoint(re rawEndpoint, limits config.MMDSEndpointsConfig) (EndpointSpec, error) {
	if !nameRE.MatchString(re.Name) {
		return EndpointSpec{}, fmt.Errorf("name %q invalid (want %s)", re.Name, nameRE.String())
	}
	path, err := validatePath(re.Path, limits)
	if err != nil {
		return EndpointSpec{}, err
	}
	backend, err := parseBackend(re.Backend, limits)
	if err != nil {
		return EndpointSpec{}, err
	}
	return EndpointSpec{Name: re.Name, Path: path, Backend: backend}, nil
}

// ValidateNameAndPath re-checks an already-persisted endpoint's name and
// path against limits — the same rules Extract applies to a fresh Create
// declaration, exported for callers revalidating existing declarations
// against node policy that may have changed since they were created (at
// startup, enabled mode validates every persisted definition against
// current limits and reserved prefixes). Does not touch backend fields —
// those never change after insert and are not derived from anything an
// operator's config edit could invalidate.
func ValidateNameAndPath(name, path string, limits config.MMDSEndpointsConfig) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("name %q invalid (want %s)", name, nameRE.String())
	}
	_, err := validatePath(path, limits)
	return err
}

// validatePath enforces the path rules: absolute, bounded lowercase-ASCII
// segments, no percent-escapes/dot-segments/backslashes/query/fragment/
// trailing-slash/wildcards, and no collision with a reserved prefix — matched
// segment-by-segment so "/latest/api-extra" does NOT collide with the
// reserved prefix "/latest/api/".
func validatePath(p string, limits config.MMDSEndpointsConfig) (string, error) {
	if p == "" {
		return "", fmt.Errorf("path is required")
	}
	if limits.MaxPathBytes > 0 && len(p) > limits.MaxPathBytes {
		return "", fmt.Errorf("path exceeds max_path_bytes (%d > %d)", len(p), limits.MaxPathBytes)
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("path %q must be absolute", p)
	}
	if strings.Contains(p, "%") {
		return "", fmt.Errorf("path %q must not contain a percent-escape", p)
	}
	if strings.Contains(p, "\\") {
		return "", fmt.Errorf("path %q must not contain a backslash", p)
	}
	if strings.ContainsAny(p, "?#") {
		return "", fmt.Errorf("path %q must not contain a query or fragment", p)
	}
	if p != "/" && strings.HasSuffix(p, "/") {
		return "", fmt.Errorf("path %q must not have a trailing slash", p)
	}
	segs := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for _, s := range segs {
		switch s {
		case "":
			return "", fmt.Errorf("path %q has an empty segment", p)
		case ".", "..":
			return "", fmt.Errorf("path %q has a dot segment", p)
		}
		if strings.Contains(s, "*") {
			return "", fmt.Errorf("path %q must not contain a wildcard", p)
		}
		if !pathSegRE.MatchString(s) {
			return "", fmt.Errorf("path %q segment %q has invalid characters (want [a-z0-9._-])", p, s)
		}
	}
	for _, prefix := range limits.ReservedPathPrefixes {
		if segmentPrefixMatch(segs, prefix) {
			return "", fmt.Errorf("path %q collides with reserved prefix %q", p, prefix)
		}
	}
	return p, nil
}

// segmentPrefixMatch reports whether segs (an absolute path's non-empty
// segments) fall under prefix, comparing whole segments rather than raw byte
// prefixes.
func segmentPrefixMatch(segs []string, prefix string) bool {
	trimmed := strings.Trim(prefix, "/")
	if trimmed == "" {
		return false
	}
	prefixSegs := strings.Split(trimmed, "/")
	if len(segs) < len(prefixSegs) {
		return false
	}
	for i, ps := range prefixSegs {
		if segs[i] != ps {
			return false
		}
	}
	return true
}

// parseBackend enforces the tagged union: type is mandatory; "store" permits
// only "type"; "relay" requires "url" and a non-null "auth" containing
// exactly "header_name", and permits no other fields.
func parseBackend(m map[string]any, limits config.MMDSEndpointsConfig) (BackendSpec, error) {
	if m == nil {
		return BackendSpec{}, fmt.Errorf("backend is required")
	}
	typRaw, ok := m["type"]
	if !ok {
		return BackendSpec{}, fmt.Errorf("backend.type is required")
	}
	typ, ok := typRaw.(string)
	if !ok {
		return BackendSpec{}, fmt.Errorf("backend.type must be a string")
	}
	switch typ {
	case BackendStore:
		for k := range m {
			if k != "type" {
				return BackendSpec{}, fmt.Errorf("backend.type=store permits no field %q", k)
			}
		}
		return BackendSpec{Type: BackendStore}, nil
	case BackendRelay:
		return parseRelayBackend(m, limits)
	default:
		return BackendSpec{}, fmt.Errorf("backend.type %q unknown (want %s|%s)", typ, BackendStore, BackendRelay)
	}
}

func parseRelayBackend(m map[string]any, limits config.MMDSEndpointsConfig) (BackendSpec, error) {
	for k := range m {
		switch k {
		case "type", "url", "auth":
		default:
			return BackendSpec{}, fmt.Errorf("backend.type=relay permits no field %q", k)
		}
	}
	urlRaw, ok := m["url"]
	if !ok {
		return BackendSpec{}, fmt.Errorf("backend.type=relay requires url")
	}
	url, ok := urlRaw.(string)
	if !ok || url == "" {
		return BackendSpec{}, fmt.Errorf("backend.url must be a non-empty string")
	}
	if limits.MaxRelayURLBytes > 0 && len(url) > limits.MaxRelayURLBytes {
		return BackendSpec{}, fmt.Errorf("backend.url exceeds max_relay_url_bytes (%d > %d)", len(url), limits.MaxRelayURLBytes)
	}
	authRaw, ok := m["auth"]
	if !ok || authRaw == nil {
		return BackendSpec{}, fmt.Errorf("backend.type=relay requires a non-null auth")
	}
	authMap, ok := authRaw.(map[string]any)
	if !ok {
		return BackendSpec{}, fmt.Errorf("backend.auth must be an object")
	}
	for k := range authMap {
		if k != "header_name" {
			return BackendSpec{}, fmt.Errorf("backend.auth permits no field %q", k)
		}
	}
	hnRaw, ok := authMap["header_name"]
	if !ok {
		return BackendSpec{}, fmt.Errorf("backend.auth.header_name is required")
	}
	hn, ok := hnRaw.(string)
	if !ok || !mmdsrelay.ValidAuthHeaderName(hn) {
		return BackendSpec{}, fmt.Errorf("backend.auth.header_name %q is invalid", hnRaw)
	}
	return BackendSpec{Type: BackendRelay, URL: url, Auth: &RelayAuthSpec{HeaderName: hn}}, nil
}
