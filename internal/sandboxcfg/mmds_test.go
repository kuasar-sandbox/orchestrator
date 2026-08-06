package sandboxcfg

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func testMMDSPolicy() MMDSPolicy {
	return MMDSPolicy{
		Enabled:               true,
		MaxRoutesPerSandbox:   32,
		MaxSecretsPerSandbox:  16,
		MaxServicesPerSandbox: 16,
		MaxStaticBodyBytes:    16384,
		MaxNamespaceBytes:     65536,
		ReservedPathPrefixes:  []string{"/internal/"},
	}
}

func TestExtractMMDSAbsentIsNoop(t *testing.T) {
	meta := map[string]string{"keep": "value"}
	spec, out, err := ExtractMMDS(meta, testMMDSPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if spec.Version != 0 || len(spec.Routes) != 0 {
		t.Fatalf("expected zero-value spec, got %+v", spec)
	}
	if out["keep"] != "value" {
		t.Fatalf("expected meta returned unchanged, got %+v", out)
	}
}

// TestNormalizeMMDSAppliesInvariantsWithoutAdmissionPolicy proves
// normalizeMMDS -- the schema/protocol-only core ExtractMMDS layers policy
// enforcement around -- canonicalizes on its own, with no MMDSPolicy input.
func TestNormalizeMMDSAppliesInvariantsWithoutAdmissionPolicy(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"routes":[{"path":"/tenant/value","data":"payload"}]}`}
	spec, out, err := normalizeMMDS(meta, meta[NsMMDS])
	if err != nil {
		t.Fatal(err)
	}
	if spec.Version != 1 || len(spec.Routes) != 1 {
		t.Fatalf("unexpected canonical spec: %+v", spec)
	}
	route := spec.Routes[0]
	if route.Type != MMDSRouteStatic || route.ContentType != "application/octet-stream" {
		t.Fatalf("route was not canonicalized: %+v", route)
	}
	if out[NsMMDS] == meta[NsMMDS] {
		t.Fatalf("expected canonical metadata, got %q", out[NsMMDS])
	}
}

// TestNormalizeMMDSDoesNotApplyMutablePolicy proves normalizeMMDS itself
// never enforces MMDSPolicy's mutable limits -- ExtractMMDS is solely
// responsible for that layer, checked separately (validateMMDSPolicy) around
// normalizeMMDS's schema/protocol-only result.
func TestNormalizeMMDSDoesNotApplyMutablePolicy(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/operator/value","data":"payload"}]}`}
	policy := testMMDSPolicy()
	policy.MaxRoutesPerSandbox = 0
	policy.MaxStaticBodyBytes = 1
	policy.ReservedPathPrefixes = []string{"/operator"}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected ExtractMMDS to enforce admission policy")
	}
	if _, _, err := normalizeMMDS(meta, meta[NsMMDS]); err != nil {
		t.Fatalf("normalizeMMDS must not enforce mutable policy: %v", err)
	}
}

func TestNormalizeMMDSStillRejectsProtocolReservedPath(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/latest/api/token","data":"payload"}]}`}
	if _, _, err := normalizeMMDS(meta, meta[NsMMDS]); err == nil {
		t.Fatal("expected protocol-reserved path to be rejected")
	}
}

func TestExtractMMDSDisabledPolicyRejectsBeforeParsing(t *testing.T) {
	policy := testMMDSPolicy()
	policy.Enabled = false
	// Deliberately malformed JSON: if this were parsed we'd get a decode
	// error, not the "disabled" error. Proves the disabled check runs first.
	meta := map[string]string{NsMMDS: `not even json {`}
	_, _, err := ExtractMMDS(meta, policy)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected a disabled-policy error, got: %v", err)
	}
	if err.Error() != "MMDS metadata: MMDS routes are disabled by policy" {
		t.Fatalf("error = %q", err)
	}
}

func TestExtractMMDSPublicErrorDoesNotExposePayload(t *testing.T) {
	secret := "do-not-return-this-secret"
	body := "do-not-return-this-body"
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","data":"` + body + `","secret_name":"` + secret + `"}]}`}
	_, _, err := ExtractMMDS(meta, testMMDSPolicy())
	if err == nil {
		t.Fatal("expected invalid cross-type fields")
	}
	public := err.Error()
	if strings.Contains(public, body) || strings.Contains(public, secret) {
		t.Fatalf("public error exposed payload data: %q", public)
	}
	if !strings.Contains(public, `route "/x"`) {
		t.Fatalf("public error omitted safe route context: %q", public)
	}
}

func TestExtractMMDSInvalidPathIsNotReflectedPublicly(t *testing.T) {
	marker := "do-not-reflect-invalid-route-path"
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"` + marker + `","type":"static","content_type":""}]}`}
	_, _, err := ExtractMMDS(meta, testMMDSPolicy())
	if err == nil {
		t.Fatal("expected an invalid path error")
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("public error exposed the invalid route path: %q", err)
	}
	if err.Error() != "MMDS metadata: route path must be an absolute path" {
		t.Fatalf("error = %q", err)
	}
	validationErr, ok := err.(*MMDSValidationError)
	if !ok || !strings.Contains(validationErr.Diagnostic(), marker) {
		t.Fatalf("trusted diagnostic omitted invalid path context: %v", err)
	}
}

func TestExtractMMDSInvalidIdentifiersAreNotReflectedPublicly(t *testing.T) {
	marker := "do-not-reflect-INVALID-identifier"
	for _, tt := range []struct {
		name string
		raw  string
		want string
	}{
		{
			"secret name",
			`{"version":1,"secrets":[{"name":"` + marker + `"}],"routes":[]}`,
			"MMDS metadata: secret name is invalid",
		},
		{
			"service name",
			`{"version":1,"services":[{"name":"` + marker + `","target":"backend"}],"routes":[]}`,
			"MMDS metadata: service name is invalid",
		},
		{
			"secret reference",
			`{"version":1,"routes":[{"path":"/x","type":"secret","secret_name":"` + marker + `"}]}`,
			`MMDS metadata: route "/x": secret_name does not reference a specified secret`,
		},
		{
			"service reference",
			`{"version":1,"routes":[{"path":"/x","type":"service","service_name":"` + marker + `"}]}`,
			`MMDS metadata: route "/x": service_name does not reference a specified service`,
		},
		{
			"route type",
			`{"version":1,"routes":[{"path":"/x","type":"` + marker + `"}]}`,
			`MMDS metadata: route "/x": type must be "static", "secret", or "service"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ExtractMMDS(map[string]string{NsMMDS: tt.raw}, testMMDSPolicy())
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if got := err.Error(); got != tt.want {
				t.Fatalf("error = %q, want %q", got, tt.want)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("public error exposed invalid identifier: %q", err)
			}
			validationErr, ok := err.(*MMDSValidationError)
			if !ok || !strings.Contains(validationErr.Diagnostic(), marker) {
				t.Fatalf("trusted diagnostic omitted invalid identifier context: %v", err)
			}
		})
	}
}

func TestExtractMMDSJSONErrorIsNormalized(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[`}
	_, _, err := ExtractMMDS(meta, testMMDSPolicy())
	if err == nil {
		t.Fatal("expected invalid JSON")
	}
	if err.Error() != "MMDS metadata: is not valid JSON" {
		t.Fatalf("JSON error = %q", err)
	}
	if strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Fatalf("internal error should not depend on raw decoder wording: %v", err)
	}
}

func TestExtractMMDSRejectsUnknownTopLevelField(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"bogus":true,"routes":[]}`}
	_, _, err := ExtractMMDS(meta, testMMDSPolicy())
	if err == nil {
		t.Fatal("expected an error for an unknown top-level field")
	}
	if err.Error() != `MMDS metadata: contains unknown field "bogus"` {
		t.Fatalf("error = %q", err)
	}
}

func TestExtractMMDSRejectsUnknownNestedRouteField(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d","bogus":"y"}]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for an unknown nested field")
	}
}

func TestExtractMMDSRejectsDuplicateJSONFields(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
	}{
		{"top level", `{"version":1,"version":1,"routes":[]}`},
		{"nested route", `{"version":1,"routes":[{"path":"/x","path":"/y","data":"d"}]}`},
		{"nested service", `{"version":1,"services":[{"name":"svc1","target":"a","target":"b"}],"routes":[{"path":"/x","type":"service","service_name":"svc1"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ExtractMMDS(map[string]string{NsMMDS: tt.raw}, testMMDSPolicy())
			if err == nil {
				t.Fatal("expected a duplicate JSON field error")
			}
			if err.Error() != "MMDS metadata: contains a duplicate JSON field" {
				t.Fatalf("error = %q", err)
			}
		})
	}
}

func TestExtractMMDSRejectsCaseInsensitiveJSONFieldAliases(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
	}{
		{"single top-level alias", `{"Version":1,"routes":[]}`},
		{"top-level overwrite alias", `{"version":1,"Version":2,"routes":[]}`},
		{"nested route overwrite alias", `{"version":1,"routes":[{"path":"/x","Path":"/y","data":"d"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ExtractMMDS(map[string]string{NsMMDS: tt.raw}, testMMDSPolicy())
			if err == nil {
				t.Fatal("expected a non-canonical JSON field error")
			}
			want := "MMDS metadata: JSON field names must use the exact schema spelling"
			if err.Error() != want {
				t.Fatalf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestExtractMMDSRequiresTopLevelJSONObject(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `"value"`, `1`, `true`} {
		t.Run(raw, func(t *testing.T) {
			_, _, err := ExtractMMDS(map[string]string{NsMMDS: raw}, testMMDSPolicy())
			if err == nil {
				t.Fatal("expected a top-level object error")
			}
			if err.Error() != "MMDS metadata: must be a JSON object" {
				t.Fatalf("error = %q", err)
			}
		})
	}
}

func TestExtractMMDSRejectsDuplicateSecretName(t *testing.T) {
	meta := map[string]string{NsMMDS: `{
		"version":1,
		"secrets":[{"name":"key1"},{"name":"key1"}],
		"routes":[
			{"path":"/a","type":"secret","secret_name":"key1"},
			{"path":"/b","type":"secret","secret_name":"key1"}
		]
	}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for a duplicate secret name")
	}
}

func TestExtractMMDSRejectsDuplicateServiceName(t *testing.T) {
	meta := map[string]string{NsMMDS: `{
		"version":1,
		"services":[{"name":"svc1","target":"t"},{"name":"svc1","target":"t"}],
		"routes":[
			{"path":"/a","type":"service","service_name":"svc1"},
			{"path":"/b","type":"service","service_name":"svc1"}
		]
	}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for a duplicate service name")
	}
}

func TestExtractMMDSRejectsDuplicatePath(t *testing.T) {
	meta := map[string]string{NsMMDS: `{
		"version":1,
		"routes":[
			{"path":"/a","type":"static","data":"1"},
			{"path":"/a","type":"static","data":"2"}
		]
	}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for a duplicate path")
	}
}

func TestExtractMMDSRejectsBadNameFormat(t *testing.T) {
	for _, name := range []string{"", "Key1", "1key", strings.Repeat("a", 64), "key with space", "key.dot"} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"version": 1,
				"secrets": []map[string]string{{"name": name}},
				"routes":  []map[string]string{{"path": "/x", "type": "secret", "secret_name": name}},
			})
			if err != nil {
				t.Fatal(err)
			}
			meta := map[string]string{NsMMDS: string(raw)}
			if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
				t.Fatalf("expected an error for bad name %q", name)
			}
		})
	}
}

func TestExtractMMDSRejectsBadPathFormat(t *testing.T) {
	for _, path := range []string{
		"relative", "/a%20b", "/a?q=1", "/a#frag", "/a/./b", "/a/../b",
		"/a/", "/", "/a//b", "/a b", "/a\\b", "/*",
	} {
		t.Run(path, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"version": 1,
				"routes":  []map[string]string{{"path": path, "type": "static", "data": "x"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			meta := map[string]string{NsMMDS: string(raw)}
			if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
				t.Fatalf("expected an error for bad path %q", path)
			}
		})
	}
}

func TestExtractMMDSRejectsUnusedSecretSpec(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"secrets":[{"name":"key1"}],"routes":[]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for an unused secret specification")
	}
}

func TestExtractMMDSRejectsUnusedServiceSpec(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"services":[{"name":"svc1","target":"t"}],"routes":[]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for an unused service specification")
	}
}

func TestExtractMMDSRejectsReservedPathCollisionBuiltin(t *testing.T) {
	for _, path := range []string{"/latest/api/token"} {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"` + path + `","type":"static","data":"x"}]}`}
		if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
			t.Fatalf("expected an error for reserved builtin path %q", path)
		}
	}
}

func TestExtractMMDSRejectsReservedPathCollisionPolicyPrefix(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/internal/foo","type":"static","data":"x"}]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for a policy-reserved-prefix collision")
	}
}

func TestExtractMMDSRejectsUppercasePathSegment(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/Foo","type":"static","data":"x"}]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for an uppercase path segment")
	}
}

func TestExtractMMDSReservedPrefixIsSegmentAware(t *testing.T) {
	// "/internal2/foo" merely shares a textual prefix with the reserved
	// "/internal/" -- it must NOT collide, since the reserved prefix only
	// covers its own subtree.
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/internal2/foo","type":"static","data":"x"}]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err != nil {
		t.Fatalf("unexpected error for a path outside the reserved prefix's subtree: %v", err)
	}

	// The prefix's own bare path (no trailing segment) still collides.
	meta = map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/internal","type":"static","data":"x"}]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error: /internal collides with the reserved prefix /internal/")
	}
}

func TestExtractMMDSReservedRootPrefixRejectsAllRoutes(t *testing.T) {
	policy := testMMDSPolicy()
	policy.ReservedPathPrefixes = []string{"/"}
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/tenant/path","type":"static","data":"x"}]}`}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected the root reserved prefix to reject every absolute route path")
	}
}

func TestValidateMMDSReservedPathPrefixes(t *testing.T) {
	for _, prefix := range []string{
		"", "internal", "/internal?", "/internal#", "/internal%",
		// Non-canonical shapes: mmdsPathUnderPrefix only ever compares against a
		// canonicalMMDSPath'd tenant route, which never contains a wildcard, dot
		// segment, empty segment, uppercase, or non-ASCII character -- so a
		// prefix in any of these shapes can never match a real route and the
		// reservation would silently fail open.
		"/internal/*", "/internal//", "/internal/..", "/Internal", "/内部",
	} {
		t.Run("invalid_"+prefix, func(t *testing.T) {
			if err := ValidateMMDSReservedPathPrefixes([]string{prefix}); err == nil {
				t.Fatalf("expected prefix %q to be rejected", prefix)
			}
		})
	}
	for _, prefix := range []string{"/", "/internal", "/internal/"} {
		t.Run("valid_"+prefix, func(t *testing.T) {
			if err := ValidateMMDSReservedPathPrefixes([]string{prefix}); err != nil {
				t.Fatalf("prefix %q rejected: %v", prefix, err)
			}
		})
	}
}

func TestExtractMMDSRejectsYAMLSyntax(t *testing.T) {
	// The Create wire contract is JSON (see package doc); YAML-only syntax
	// (unquoted keys/values, comments) that a permissive YAML decoder would
	// have accepted must now be rejected as invalid JSON.
	meta := map[string]string{NsMMDS: "version: 1\nroutes: []\n"}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for YAML-syntax input")
	}
}

func TestExtractMMDSRejectsTrailingData(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[]}` + "\ngarbage"}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for trailing data after the JSON document")
	}
}

// TestExtractMMDSRejectsTrailingDataStartingWithClosingDelimiter guards
// against a real bug in an earlier version of this check: json.Decoder.More()
// peeks the next non-whitespace byte and treats a literal ']' or '}' as "no
// more data" unconditionally -- it doesn't track whether that byte is
// actually closing an enclosing array/object. At the top level, once the
// primary document is fully decoded, there is no enclosing structure left to
// close, so a stray ']' or '}' immediately following it was silently
// accepted instead of rejected. Verified empirically these two bytes are the
// *only* ones that reproduced the bug -- see
// TestExtractMMDSRejectsOtherTrailingDataShapes below for shapes that were
// already correctly rejected even by the old check, kept as regression
// guards in their own right.
func TestExtractMMDSRejectsTrailingDataStartingWithClosingDelimiter(t *testing.T) {
	for _, trailing := range []string{"]", "}"} {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[]}` + trailing}
		if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
			t.Fatalf("expected an error for trailing data %q after the JSON document", trailing)
		}
	}
}

// TestExtractMMDSRejectsOtherTrailingDataShapes is a broader robustness
// check across trailing-content categories the strict re-decode must reject
// regardless of shape: a bare comma before another object, a JSON number (a
// value type distinct from an object -- exercises the "second Decode
// succeeds on a value" path, not a syntax error), a byte that isn't valid
// JSON syntax at all, and a second full concatenated document.
func TestExtractMMDSRejectsOtherTrailingDataShapes(t *testing.T) {
	for _, trailing := range []string{",{}", "-5", "/x", `{"version":1,"routes":[]}`} {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[]}` + trailing}
		if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
			t.Fatalf("expected an error for trailing data %q after the JSON document", trailing)
		}
	}
}

func TestExtractMMDSRejectsOversizeNamespace(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxNamespaceBytes = 8
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","data":"hello world"}]}`}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected an error for an oversize namespace")
	}
}

// TestExtractMMDSRejectsCanonicalFormOverEscapeExpansion proves that a raw
// namespace which itself fits under MaxNamespaceBytes can still be rejected,
// because json.Marshal HTML-escapes '<'/'>'/'&' into six-byte \uXXXX
// sequences when producing the canonical form that actually gets persisted
// and re-encoded onto the routesync/mmdsrpc wire. Without re-checking the
// canonical length, a spec built almost entirely of one of these characters
// could be admitted under the raw check yet still be too large once
// canonicalized -- exactly the gap the transport frame relies on
// MaxNamespaceBytes to prevent.
func TestExtractMMDSRejectsCanonicalFormOverEscapeExpansion(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxNamespaceBytes = 300
	policy.MaxStaticBodyBytes = 300
	data := strings.Repeat("<", 200)
	raw := `{"version":1,"routes":[{"path":"/x","type":"static","data":"` + data + `"}]}`
	if len(raw) > policy.MaxNamespaceBytes {
		t.Fatalf("test setup: raw namespace (%d bytes) already exceeds the policy limit (%d bytes)", len(raw), policy.MaxNamespaceBytes)
	}
	meta := map[string]string{NsMMDS: raw}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected an error: canonical form (post JSON-escaping) exceeds MaxNamespaceBytes even though the raw input did not")
	}
}

// TestNormalizeMMDSRejectsOverProtocolCeiling proves the fixed ceiling is
// enforced by normalizeMMDS itself -- the schema/protocol-only core every
// MMDSPolicy, however permissive, is layered around -- not something
// ExtractMMDS bolts on separately. A canonical form large enough to risk
// overflowing the fixed 1 MiB routesync frame once published as a RouteEntry
// (which carries credential/token fields beyond the MMDS namespace itself)
// is rejected regardless of policy.
func TestNormalizeMMDSRejectsOverProtocolCeiling(t *testing.T) {
	data := strings.Repeat("a", 1_020_000)
	raw := `{"version":1,"routes":[{"path":"/x","type":"static","data":"` + data + `"}]}`
	meta := map[string]string{NsMMDS: raw}
	if _, _, err := normalizeMMDS(meta, meta[NsMMDS]); err == nil {
		t.Fatal("expected an error: canonical form exceeds the fixed protocol ceiling")
	}
}

// TestExtractMMDSRejectsOverProtocolCeilingEvenWhenPolicyAllowsMore proves the
// fixed protocol ceiling (maxMMDSCanonicalBytes) is a hard backstop independent
// of MMDSPolicy.MaxNamespaceBytes: an operator setting max_namespace_bytes
// unsafely close to the routesync frame limit must not be able to admit a
// canonical form that overflows RouteEntry's own publication frame.
func TestExtractMMDSRejectsOverProtocolCeilingEvenWhenPolicyAllowsMore(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxNamespaceBytes = 1 << 20 // as permissive as the raw routesync frame itself
	policy.MaxStaticBodyBytes = 1 << 20
	data := strings.Repeat("a", 1_020_000)
	raw := `{"version":1,"routes":[{"path":"/x","type":"static","data":"` + data + `"}]}`
	meta := map[string]string{NsMMDS: raw}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected an error: fixed protocol ceiling should reject even though policy allows it")
	}
}

func TestExtractMMDSRejectsOversizeStaticBody(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxStaticBodyBytes = 4
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","data":"hello world"}]}`}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected an error for an oversize static body")
	}
}

func TestExtractMMDSRejectsTooManyRoutes(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxRoutesPerSandbox = 1
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[
		{"path":"/a","type":"static","data":"1"},
		{"path":"/b","type":"static","data":"2"}
	]}`}
	if _, _, err := ExtractMMDS(meta, policy); err == nil {
		t.Fatal("expected an error for too many routes")
	}
}

func TestExtractMMDSCanonicalizesStaticShorthand(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/data","data":"{\"k\":\"v\"}"}]}`}
	spec, out, err := ExtractMMDS(meta, testMMDSPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(spec.Routes))
	}
	r := spec.Routes[0]
	if r.Type != MMDSRouteStatic {
		t.Fatalf("expected canonicalized type %q, got %q", MMDSRouteStatic, r.Type)
	}
	if r.ContentType != "application/octet-stream" {
		t.Fatalf("expected default content_type, got %q", r.ContentType)
	}

	// Re-running ExtractMMDS on the canonical output must be a no-op (byte-identical).
	spec2, out2, err := ExtractMMDS(out, testMMDSPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if out[NsMMDS] != out2[NsMMDS] {
		t.Fatalf("re-extraction changed the canonical form:\nfirst:  %s\nsecond: %s", out[NsMMDS], out2[NsMMDS])
	}
	if len(spec2.Routes) != 1 || spec2.Routes[0].Type != MMDSRouteStatic {
		t.Fatalf("unexpected second spec: %+v", spec2)
	}
}

func TestExtractMMDSValidatesStaticContentType(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","content_type":"application/json; charset=utf-8","data":"{}"}]}`}
		spec, _, err := ExtractMMDS(meta, testMMDSPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if got := spec.Routes[0].ContentType; got != "application/json; charset=utf-8" {
			t.Fatalf("content_type = %q", got)
		}
	})

	for _, tt := range []struct {
		name        string
		contentType string
		want        string
	}{
		{"empty", "", "content_type must not be empty"},
		{"invalid", "not a media type", "content_type is not a valid media type"},
		{"too long", strings.Repeat("a", maxMMDSContentTypeBytes+1), "content_type exceeds the maximum size"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"version": 1,
				"routes": []map[string]string{{
					"path": "/x", "type": "static", "content_type": tt.contentType,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = ExtractMMDS(map[string]string{NsMMDS: string(raw)}, testMMDSPolicy())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want text %q", err, tt.want)
			}
		})
	}
}

func TestExtractMMDSRejectsCrossTypeFields(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
	}{
		{"secret with data", `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"secret","secret_name":"key1","data":"d"}]}`},
		{"secret with empty data", `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"secret","secret_name":"key1","data":""}]}`},
		{"static with secret_name", `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"static","data":"d","secret_name":"key1"}]}`},
		{"static with empty service_name", `{"version":1,"routes":[{"path":"/x","type":"static","data":"d","service_name":""}]}`},
		{"static with null service_name", `{"version":1,"routes":[{"path":"/x","type":"static","data":"d","service_name":null}]}`},
		{"service with content_type", `{"version":1,"services":[{"name":"svc1","target":"t"}],"routes":[{"path":"/x","type":"service","service_name":"svc1","content_type":"text/plain"}]}`},
		{"service with empty content_type", `{"version":1,"services":[{"name":"svc1","target":"t"}],"routes":[{"path":"/x","type":"service","service_name":"svc1","content_type":""}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			meta := map[string]string{NsMMDS: tt.raw}
			if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
				t.Fatal("expected a cross-type field error")
			}
		})
	}
}

func TestExtractMMDSRejectsExplicitEmptyType(t *testing.T) {
	for _, routeType := range []string{`""`, `null`} {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":` + routeType + `,"data":"d"}]}`}
		_, _, err := ExtractMMDS(meta, testMMDSPolicy())
		if err == nil || !strings.Contains(err.Error(), "type must not be empty") {
			t.Fatalf("type %s: error = %v", routeType, err)
		}
	}
}

func TestExtractMMDSRejectsServiceTargetWhitespace(t *testing.T) {
	for _, target := range []string{" backend", "backend ", "\tbackend", "backend\n"} {
		t.Run(target, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"version":  1,
				"services": []map[string]string{{"name": "svc1", "target": target}},
				"routes":   []map[string]string{{"path": "/x", "type": "service", "service_name": "svc1"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = ExtractMMDS(map[string]string{NsMMDS: string(raw)}, testMMDSPolicy())
			if err == nil || !strings.Contains(err.Error(), "leading or trailing whitespace") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestExtractMMDSRejectsMissingReference(t *testing.T) {
	t.Run("secret_name", func(t *testing.T) {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"secret","secret_name":"nope"}]}`}
		if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
			t.Fatal("expected an error for an unspecified secret_name reference")
		}
	})
	t.Run("service_name", func(t *testing.T) {
		meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"service","service_name":"nope"}]}`}
		if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
			t.Fatal("expected an error for an unspecified service_name reference")
		}
	})
}

func TestExtractMMDSRejectsUnsupportedVersion(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":2,"routes":[]}`}
	if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
		t.Fatal("expected an error for an unsupported version")
	}
}

func TestExtractMMDSSpecWithAllTypes(t *testing.T) {
	meta := map[string]string{NsMMDS: `{
		"version": 1,
		"secrets": [{"name": "key1"}],
		"services": [{"name": "svc1", "target": "credential-broker"}],
		"routes": [
			{"path": "/secret-path", "type": "secret", "secret_name": "key1"},
			{"path": "/service-path", "type": "service", "service_name": "svc1"},
			{"path": "/static-path", "type": "static", "content_type": "application/json", "data": "{\"key\":\"value\"}"}
		]
	}`}
	spec, out, err := ExtractMMDS(meta, testMMDSPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Routes) != 3 {
		t.Fatalf("expected 3 routes, got %d", len(spec.Routes))
	}
	if out[NsMMDS] == "" {
		t.Fatal("expected canonical form written back into meta")
	}
}

func TestMergeCreateMetadataKeepsMMDSRequestScoped(t *testing.T) {
	defaults := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/template-only","type":"static","data":"d"}]}`}
	request := map[string]string{}
	out := MergeCreateMetadata(defaults, request)
	if _, present := out[NsMMDS]; present {
		t.Fatalf("expected NsMMDS to be dropped from template defaults, got %+v", out)
	}

	request2 := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/request-only","type":"static","data":"d"}]}`}
	out2 := MergeCreateMetadata(defaults, request2)
	if out2[NsMMDS] != request2[NsMMDS] {
		t.Fatalf("expected the request's own NsMMDS to be admitted, got %+v", out2)
	}
}

// TestMMDSSpecNeverEntersGuestVisibleMetadata proves the full
// sb.Metadata (as ExtractMMDS would persist it, including NsMMDS) never rides
// the rendered SANDBOX_CONFIG.metadata the guest can read: ParseSpec.Metadata
// is decoded ONLY from the NsMetadata key's own JSON content, not copied from
// the raw metadata map, so a sibling NsMMDS key is structurally excluded --
// not merely absent from this particular test's input.
func TestMMDSSpecNeverEntersGuestVisibleMetadata(t *testing.T) {
	sbMeta := map[string]string{
		NsMMDS:     `{"version":1,"secrets":[{"name":"key1"}],"services":[{"name":"svc1","target":"credential-broker"}],"routes":[{"path":"/x","type":"static","data":"d"},{"path":"/y","type":"secret","secret_name":"key1"},{"path":"/z","type":"service","service_name":"svc1"}]}`,
		NsMetadata: `{"e2b.start_cmd":"npm run start"}`,
	}
	spec, err := ParseSpec(sbMeta)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := spec.Metadata[NsMMDS]; present {
		t.Fatalf("NsMMDS leaked into SandboxSpec.Metadata: %+v", spec.Metadata)
	}
	if spec.Metadata["e2b.start_cmd"] != "npm run start" {
		t.Fatalf("unrelated guest-visible metadata was lost: %+v", spec.Metadata)
	}

	tmpl := types.TemplateID{
		Profile: types.ProfileE2B,
		Kind:    types.KindImg,
		Ref:     "manifest://" + strings.Repeat("a", 64),
	}
	p := Params{
		Sandbox:  &types.Sandbox{ID: "s1", TemplateID: tmpl.String()},
		Template: tmpl, Runtime: "/r/sandbox-runtime.bundle", Kernel: "/r/vmlinux",
		VCPU: 2, Memory: "2GiB", Spec: spec,
	}
	b, err := p.BuildYAML()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, leaked := range []string{NsMMDS, "kuasar-sandbox.mmds", "credential-broker", "secret_name", "service_name"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("rendered SANDBOX_CONFIG leaked MMDS content (%q found):\n%s", leaked, out)
		}
	}
}

func TestLookupMMDSRoute(t *testing.T) {
	raw := `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`

	route, ok := LookupMMDSRoute(raw, "/x")
	if !ok {
		t.Fatal("expected the specified route to be found")
	}
	if route.Type != MMDSRouteStatic || route.Data != "hi" {
		t.Fatalf("unexpected route: %+v", route)
	}

	if _, ok := LookupMMDSRoute(raw, "/unspecified"); ok {
		t.Fatal("expected an unspecified path to be not found")
	}
	if _, ok := LookupMMDSRoute("", "/x"); ok {
		t.Fatal("expected an absent namespace to be not found")
	}
}
