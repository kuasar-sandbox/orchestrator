package sandboxcfg

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	MMDSRouteStatic  = "static"
	MMDSRouteSecret  = "secret"
	MMDSRouteService = "service"

	defaultMMDSContentType = "text/plain"
	maxMMDSContentType     = 256
)

// MMDSInput is the accepted Sandbox Create / Build Register document. Secrets
// are request-scoped initial values; only Routes are portable and persisted.
type MMDSInput struct {
	Secrets map[string]string `json:"secrets,omitempty"`
	Routes  []MMDSRoute       `json:"routes,omitempty"`
}

// MMDSRoute is one exact guest-visible route. An empty Type means static. The
// persisted representation deliberately keeps omitted defaults omitted.
type MMDSRoute struct {
	Path        string `json:"path"`
	Type        string `json:"type,omitempty"`
	Secret      string `json:"secret,omitempty"`
	Service     string `json:"service,omitempty"`
	Data        string `json:"data,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

// MMDSPolicy is the conductor-owned admission policy for MMDS routes.
type MMDSPolicy struct {
	Enabled              bool
	MaxRoutesPerSandbox  int
	MaxNamespaceBytes    int
	MaxStaticBodyBytes   int
	MaxSecretValueBytes  int
	ReservedPathPrefixes []string
	Services             map[string]string
}

// MMDSDocument is the validated, merged result. SecretValues contains opaque
// bytes and must never be copied into metadata. RoutesJSON is empty when no
// source explicitly supplied routes; otherwise it is stable routes-only JSON.
type MMDSDocument struct {
	Routes         []MMDSRoute
	SecretValues   map[string][]byte
	RoutesPresent  bool
	SecretsPresent bool
	RoutesJSON     string
}

type mmdsRoutePresence struct {
	path, typ, secret, service, data, contentType bool
}

type parsedMMDSRoute struct {
	route    MMDSRoute
	presence mmdsRoutePresence
}

type partialMMDS struct {
	secrets        map[string]string
	routes         []parsedMMDSRoute
	secretsPresent bool
	routesPresent  bool
}

// ExtractMMDS parses metadata and Header independently, merges them by
// top-level key (Header wins only for keys it explicitly contains), validates
// the effective document, and rewrites metadata to routes-only canonical JSON.
// header is nil when X-Kuasar-Sandbox-MMDS was absent; a non-nil empty string is
// an explicitly supplied malformed document and is rejected.
func ExtractMMDS(meta map[string]string, header *string, policy MMDSPolicy) (MMDSDocument, map[string]string, error) {
	return extractMMDS(meta, header, policy, true)
}

// ExtractMMDSReplay strictly normalizes a previously accepted registration
// without reapplying mutable node admission policy. The durable registration
// row and its confidential value digest remain authoritative for the final
// identity comparison. This path is only for same-owner replay after an
// ambiguous/lost acknowledgement; new registrations must use ExtractMMDS.
func ExtractMMDSReplay(meta map[string]string, header *string) (MMDSDocument, map[string]string, error) {
	maxInt := int(^uint(0) >> 1)
	return extractMMDS(meta, header, MMDSPolicy{
		Enabled:             true,
		MaxRoutesPerSandbox: maxInt,
		MaxNamespaceBytes:   maxInt,
		MaxStaticBodyBytes:  maxInt,
		MaxSecretValueBytes: maxInt,
	}, false)
}

func extractMMDS(meta map[string]string, header *string, policy MMDSPolicy, requireServices bool) (MMDSDocument, map[string]string, error) {
	var metadataRaw *string
	if raw, ok := meta[NsMMDS]; ok {
		copy := raw
		metadataRaw = &copy
	}
	if metadataRaw == nil && header == nil {
		return MMDSDocument{}, meta, nil
	}
	if !policy.Enabled {
		return MMDSDocument{}, nil, errors.New("mmds routes are disabled by policy")
	}

	var metadataPart, headerPart partialMMDS
	var err error
	if metadataRaw != nil {
		metadataPart, err = parseMMDSPartial(*metadataRaw, policy.MaxNamespaceBytes)
		if err != nil {
			return MMDSDocument{}, nil, fmt.Errorf("metadata[%q]: %w", NsMMDS, err)
		}
	}
	if header != nil {
		headerPart, err = parseMMDSPartial(*header, policy.MaxNamespaceBytes)
		if err != nil {
			return MMDSDocument{}, nil, fmt.Errorf("X-Kuasar-Sandbox-MMDS: %w", err)
		}
	}

	effective := metadataPart
	if headerPart.secretsPresent {
		effective.secrets, effective.secretsPresent = headerPart.secrets, true
	}
	if headerPart.routesPresent {
		effective.routes, effective.routesPresent = headerPart.routes, true
	}
	doc, err := validateMMDSEffective(effective, policy, requireServices)
	if err != nil {
		return MMDSDocument{}, nil, err
	}

	out := cloneStringMap(meta)
	delete(out, NsMMDS)
	if doc.RoutesPresent {
		if out == nil {
			out = map[string]string{}
		}
		out[NsMMDS] = doc.RoutesJSON
	}
	return doc, out, nil
}

// ExtractMMDSImportSecrets applies the same strict parsing and top-level merge
// to a standalone CONNECT import, but rejects a routes key in either request
// source. Routes come exclusively from the migration token.
func ExtractMMDSImportSecrets(meta map[string]string, header *string, tokenRoutes []MMDSRoute, policy MMDSPolicy) (map[string][]byte, error) {
	var metadataRaw *string
	if raw, ok := meta[NsMMDS]; ok {
		copy := raw
		metadataRaw = &copy
	}
	if metadataRaw == nil && header == nil {
		return nil, nil
	}
	if !policy.Enabled {
		return nil, errors.New("mmds routes are disabled by policy")
	}
	var metadataPart, headerPart partialMMDS
	var err error
	if metadataRaw != nil {
		metadataPart, err = parseMMDSPartial(*metadataRaw, policy.MaxNamespaceBytes)
		if err != nil {
			return nil, fmt.Errorf("metadata[%q]: %w", NsMMDS, err)
		}
		if metadataPart.routesPresent {
			return nil, errors.New("standalone import MMDS input must not contain routes")
		}
	}
	if header != nil {
		headerPart, err = parseMMDSPartial(*header, policy.MaxNamespaceBytes)
		if err != nil {
			return nil, fmt.Errorf("X-Kuasar-Sandbox-MMDS: %w", err)
		}
		if headerPart.routesPresent {
			return nil, errors.New("standalone import MMDS input must not contain routes")
		}
	}
	secrets := metadataPart.secrets
	present := metadataPart.secretsPresent
	if headerPart.secretsPresent {
		secrets, present = headerPart.secrets, true
	}
	if !present {
		return nil, nil
	}
	referenced := make(map[string]struct{})
	for _, route := range tokenRoutes {
		if route.Type == MMDSRouteSecret {
			referenced[route.Secret] = struct{}{}
		}
	}
	values := make(map[string][]byte, len(secrets))
	for name, value := range secrets {
		if _, ok := referenced[name]; !ok {
			return nil, fmt.Errorf("initial secret %q is not referenced by a secret route", name)
		}
		if len(value) > policy.MaxSecretValueBytes {
			return nil, fmt.Errorf("initial secret %q exceeds the maximum size of %d bytes", name, policy.MaxSecretValueBytes)
		}
		values[name] = []byte(value)
	}
	return values, nil
}

// ValidatePersistedMMDSRoutes validates a routes-only document, for example a
// declaration restored from a standalone migration token. It returns stable
// minimal JSON suitable for metadata persistence.
func ValidatePersistedMMDSRoutes(raw string, policy MMDSPolicy) ([]MMDSRoute, string, error) {
	if !policy.Enabled {
		return nil, "", errors.New("mmds routes are disabled by policy")
	}
	part, err := parseMMDSPartial(raw, policy.MaxNamespaceBytes)
	if err != nil {
		return nil, "", err
	}
	if part.secretsPresent {
		return nil, "", errors.New("persisted MMDS metadata must not contain secrets")
	}
	if !part.routesPresent {
		return nil, "", errors.New("persisted MMDS metadata must contain routes")
	}
	doc, err := validateMMDSEffective(part, policy, true)
	if err != nil {
		return nil, "", err
	}
	return doc.Routes, doc.RoutesJSON, nil
}

// DecodePersistedMMDSRoutes strictly decodes already-admitted routes-only JSON
// without reapplying mutable node policy. It is used on the serving path; a
// corrupt/non-minimal record returns an error and callers fail closed.
func DecodePersistedMMDSRoutes(raw string) ([]MMDSRoute, error) {
	part, err := parseMMDSPartial(raw, 1<<20)
	if err != nil {
		return nil, err
	}
	if part.secretsPresent || !part.routesPresent {
		return nil, errors.New("persisted MMDS metadata must contain only routes")
	}
	policy := MMDSPolicy{
		Enabled:             true,
		MaxRoutesPerSandbox: 1 << 20,
		MaxNamespaceBytes:   1 << 20,
		MaxStaticBodyBytes:  1 << 20,
		MaxSecretValueBytes: 1 << 20,
	}
	doc, err := validateMMDSEffective(part, policy, false)
	if err != nil {
		return nil, err
	}
	return doc.Routes, nil
}

func parseMMDSPartial(raw string, maxBytes int) (partialMMDS, error) {
	if maxBytes <= 0 {
		return partialMMDS{}, errors.New("invalid MMDS namespace size policy")
	}
	if len(raw) > maxBytes {
		return partialMMDS{}, fmt.Errorf("MMDS document exceeds the maximum size of %d bytes", maxBytes)
	}
	if err := rejectDuplicateJSONKeys([]byte(raw)); err != nil {
		return partialMMDS{}, err
	}
	var top map[string]json.RawMessage
	if err := decodeOneStrict([]byte(raw), &top); err != nil {
		return partialMMDS{}, err
	}
	if top == nil {
		return partialMMDS{}, errors.New("MMDS document must be a JSON object")
	}
	for key := range top {
		if key != "secrets" && key != "routes" {
			return partialMMDS{}, fmt.Errorf("MMDS document contains unknown field %q", key)
		}
	}

	var out partialMMDS
	if rawSecrets, ok := top["secrets"]; ok {
		out.secretsPresent = true
		if bytes.Equal(bytes.TrimSpace(rawSecrets), []byte("null")) {
			return partialMMDS{}, errors.New("MMDS secrets must be a JSON object")
		}
		var rawValues map[string]json.RawMessage
		if err := decodeOneStrict(rawSecrets, &rawValues); err != nil {
			return partialMMDS{}, fmt.Errorf("MMDS secrets: %w", err)
		}
		for name, rawValue := range rawValues {
			if bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
				return partialMMDS{}, fmt.Errorf("MMDS secret %q must be a JSON string", name)
			}
		}
		if err := decodeOneStrict(rawSecrets, &out.secrets); err != nil {
			return partialMMDS{}, fmt.Errorf("MMDS secrets: %w", err)
		}
		if out.secrets == nil {
			out.secrets = map[string]string{}
		}
	}
	if rawRoutes, ok := top["routes"]; ok {
		out.routesPresent = true
		if bytes.Equal(bytes.TrimSpace(rawRoutes), []byte("null")) {
			return partialMMDS{}, errors.New("MMDS routes must be a JSON array")
		}
		var raws []json.RawMessage
		if err := decodeOneStrict(rawRoutes, &raws); err != nil {
			return partialMMDS{}, fmt.Errorf("MMDS routes: %w", err)
		}
		if raws == nil {
			raws = []json.RawMessage{}
		}
		out.routes = make([]parsedMMDSRoute, 0, len(raws))
		for i, routeRaw := range raws {
			parsed, err := parseMMDSRoute(routeRaw)
			if err != nil {
				return partialMMDS{}, fmt.Errorf("MMDS route %d: %w", i, err)
			}
			out.routes = append(out.routes, parsed)
		}
	}
	return out, nil
}

func parseMMDSRoute(raw []byte) (parsedMMDSRoute, error) {
	var fields map[string]json.RawMessage
	if err := decodeOneStrict(raw, &fields); err != nil {
		return parsedMMDSRoute{}, err
	}
	if fields == nil {
		return parsedMMDSRoute{}, errors.New("must be a JSON object")
	}
	allowed := map[string]bool{
		"path": true, "type": true, "secret": true, "service": true,
		"data": true, "content_type": true,
	}
	for key := range fields {
		if !allowed[key] {
			return parsedMMDSRoute{}, fmt.Errorf("contains unknown field %q", key)
		}
		if bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
			return parsedMMDSRoute{}, fmt.Errorf("field %q must be a JSON string", key)
		}
	}
	var route MMDSRoute
	if err := decodeOneStrict(raw, &route); err != nil {
		return parsedMMDSRoute{}, err
	}
	return parsedMMDSRoute{
		route: route,
		presence: mmdsRoutePresence{
			path: fields["path"] != nil, typ: fields["type"] != nil,
			secret: fields["secret"] != nil, service: fields["service"] != nil,
			data: fields["data"] != nil, contentType: fields["content_type"] != nil,
		},
	}, nil
}

func validateMMDSEffective(part partialMMDS, policy MMDSPolicy, requireServices bool) (MMDSDocument, error) {
	if len(part.routes) > policy.MaxRoutesPerSandbox {
		return MMDSDocument{}, fmt.Errorf("MMDS document specifies %d routes, exceeding the limit of %d", len(part.routes), policy.MaxRoutesPerSandbox)
	}
	routes := make([]MMDSRoute, len(part.routes))
	paths := make(map[string]struct{}, len(part.routes))
	referencedSecrets := make(map[string]struct{})
	for i, parsed := range part.routes {
		r := parsed.route
		p := parsed.presence
		if !p.path || r.Path == "" {
			return MMDSDocument{}, fmt.Errorf("MMDS route %d: path is required", i)
		}
		path, err := validateMMDSPath(r.Path)
		if err != nil {
			return MMDSDocument{}, fmt.Errorf("MMDS route %q: %w", r.Path, err)
		}
		if _, duplicate := paths[path]; duplicate {
			return MMDSDocument{}, fmt.Errorf("MMDS document specifies path %q more than once", path)
		}
		paths[path] = struct{}{}
		if reservedMMDSPathCollision(path, policy.ReservedPathPrefixes) {
			return MMDSDocument{}, fmt.Errorf("MMDS route path %q collides with a reserved path", path)
		}
		r.Path = path

		typ := r.Type
		if !p.typ {
			typ = MMDSRouteStatic
		} else if typ == "" {
			return MMDSDocument{}, fmt.Errorf("MMDS route %q: type must not be empty", path)
		}
		switch typ {
		case MMDSRouteStatic:
			if p.secret || p.service {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: static routes forbid secret and service", path)
			}
			if len(r.Data) > policy.MaxStaticBodyBytes {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: data exceeds the maximum size of %d bytes", path, policy.MaxStaticBodyBytes)
			}
			if p.contentType {
				if err := validateMMDSContentType(r.ContentType); err != nil {
					return MMDSDocument{}, fmt.Errorf("MMDS route %q: %w", path, err)
				}
			}
			// Both an omitted type and an explicit type:"static" persist without
			// the redundant default. No response defaults are written here.
			r.Type = ""
		case MMDSRouteSecret:
			if !p.secret || r.Secret == "" {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: secret is required", path)
			}
			if p.data || p.service {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: secret routes forbid data and service", path)
			}
			if p.contentType {
				if err := validateMMDSContentType(r.ContentType); err != nil {
					return MMDSDocument{}, fmt.Errorf("MMDS route %q: %w", path, err)
				}
			}
			referencedSecrets[r.Secret] = struct{}{}
		case MMDSRouteService:
			if !p.service || r.Service == "" {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: service is required", path)
			}
			if p.data || p.secret || p.contentType {
				return MMDSDocument{}, fmt.Errorf("MMDS route %q: service routes forbid data, secret, and content_type", path)
			}
			if requireServices {
				if _, ok := policy.Services[r.Service]; !ok {
					return MMDSDocument{}, fmt.Errorf("MMDS route %q: service %q is not configured", path, r.Service)
				}
			}
		default:
			return MMDSDocument{}, fmt.Errorf("MMDS route %q: unknown type %q", path, typ)
		}
		routes[i] = r
	}

	values := make(map[string][]byte, len(part.secrets))
	for name, value := range part.secrets {
		if _, ok := referencedSecrets[name]; !ok {
			return MMDSDocument{}, fmt.Errorf("initial secret %q is not referenced by a secret route", name)
		}
		if len(value) > policy.MaxSecretValueBytes {
			return MMDSDocument{}, fmt.Errorf("initial secret %q exceeds the maximum size of %d bytes", name, policy.MaxSecretValueBytes)
		}
		values[name] = []byte(value)
	}

	doc := MMDSDocument{
		Routes: routes, SecretValues: values,
		RoutesPresent: part.routesPresent, SecretsPresent: part.secretsPresent,
	}
	if doc.RoutesPresent {
		stable, err := json.Marshal(struct {
			Routes []MMDSRoute `json:"routes"`
		}{Routes: routes})
		if err != nil {
			return MMDSDocument{}, err
		}
		if len(stable) > policy.MaxNamespaceBytes {
			return MMDSDocument{}, fmt.Errorf("canonical MMDS routes exceed the maximum size of %d bytes", policy.MaxNamespaceBytes)
		}
		doc.RoutesJSON = string(stable)
	}
	return doc, nil
}

func validateMMDSPath(raw string) (string, error) {
	if !utf8.ValidString(raw) {
		return "", errors.New("path must be valid UTF-8")
	}
	if raw == "" || raw[0] != '/' {
		return "", errors.New("path must be absolute")
	}
	if raw == "/" {
		return "", errors.New("the built-in root path is reserved")
	}
	if strings.ContainsAny(raw, "?#%\\*") {
		return "", errors.New("path contains a query, fragment, percent escape, backslash, or wildcard")
	}
	// Admission must preserve the exact request target. Characters that
	// net/http would percent-encode cannot be reached because guest-side
	// percent escapes are rejected by the MMDS request guard.
	if (&url.URL{Path: raw}).EscapedPath() != raw {
		return "", errors.New("path contains a character that requires percent encoding")
	}
	if strings.HasSuffix(raw, "/") {
		return "", errors.New("path must not have a trailing slash")
	}
	for _, segment := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
		if segment == "" {
			return "", errors.New("path must not contain an empty segment")
		}
		if segment == "." || segment == ".." {
			return "", errors.New("path must not contain a dot segment")
		}
	}
	return raw, nil
}

func validateMMDSContentType(value string) error {
	if value == "" {
		return errors.New("content_type must not be empty when present")
	}
	if len(value) > maxMMDSContentType {
		return fmt.Errorf("content_type exceeds %d bytes", maxMMDSContentType)
	}
	if _, _, err := mime.ParseMediaType(value); err != nil {
		return fmt.Errorf("content_type is invalid: %w", err)
	}
	return nil
}

func reservedMMDSPathCollision(path string, configured []string) bool {
	if path == "/" || path == "/latest/api/token" {
		return true
	}
	for _, prefix := range configured {
		prefix = strings.TrimSuffix(prefix, "/")
		if prefix == "" {
			continue
		}
		if prefix == "/" || path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// ValidateMMDSReservedPathPrefixes validates conductor configuration using the
// same exact-path rules as tenant routes, while permitting a trailing slash and
// the special prefix "/".
func ValidateMMDSReservedPathPrefixes(prefixes []string) error {
	for _, raw := range prefixes {
		if raw == "/" {
			continue
		}
		prefix := strings.TrimSuffix(raw, "/")
		if prefix == "" {
			return errors.New("reserved MMDS path prefix must not be empty")
		}
		if _, err := validateMMDSPath(prefix); err != nil {
			return fmt.Errorf("reserved MMDS path prefix %q: %w", raw, err)
		}
	}
	return nil
}

// MMDSRuntimeContentType applies response-time defaults without mutating or
// expanding persisted configuration.
func MMDSRuntimeContentType(route MMDSRoute) string {
	if route.ContentType != "" {
		return route.ContentType
	}
	return defaultMMDSContentType
}

func LookupMMDSRoute(routes []MMDSRoute, path string) (MMDSRoute, bool) {
	for _, route := range routes {
		if route.Path == path {
			return route, true
		}
	}
	return MMDSRoute{}, false
}

func MMDSReferencesSecret(routes []MMDSRoute, name string) bool {
	for _, route := range routes {
		if route.Type == MMDSRouteSecret && route.Secret == name {
			return true
		}
	}
	return false
}

func MMDSHasSecretRoutes(routes []MMDSRoute) bool {
	for _, route := range routes {
		if route.Type == MMDSRouteSecret {
			return true
		}
	}
	return false
}

func MMDSRoutesDigest(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func decodeOneStrict(raw []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return errors.New("JSON contains trailing data")
	}
	return nil
}

// rejectDuplicateJSONKeys walks one JSON value and rejects duplicate keys in
// every object before encoding/json can apply its last-value-wins behavior.
func rejectDuplicateJSONKeys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSONValue(dec); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return errors.New("JSON contains trailing data")
	}
	return nil
}

func scanJSONValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return fmt.Errorf("invalid JSON: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("JSON contains duplicate key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		return nil
	case '[':
		for dec.More() {
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		return nil
	default:
		return errors.New("invalid JSON delimiter")
	}
}
