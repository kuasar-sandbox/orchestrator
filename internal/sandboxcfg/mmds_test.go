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

func TestExtractMMDSRejectsCrossTypeFields(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
	}{
		{"secret with data", `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"secret","secret_name":"key1","data":"d"}]}`},
		{"static with secret_name", `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"static","data":"d","secret_name":"key1"}]}`},
		{"service with content_type", `{"version":1,"services":[{"name":"svc1","target":"t"}],"routes":[{"path":"/x","type":"service","service_name":"svc1","content_type":"text/plain"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			meta := map[string]string{NsMMDS: tt.raw}
			if _, _, err := ExtractMMDS(meta, testMMDSPolicy()); err == nil {
				t.Fatal("expected a cross-type field error")
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
	meta := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","content_type":"text/plain","data":"hi"}]}`}

	route, ok := LookupMMDSRoute(meta, "/x")
	if !ok {
		t.Fatal("expected the specified route to be found")
	}
	if route.Type != MMDSRouteStatic || route.Data != "hi" {
		t.Fatalf("unexpected route: %+v", route)
	}

	if _, ok := LookupMMDSRoute(meta, "/unspecified"); ok {
		t.Fatal("expected an unspecified path to be not found")
	}
	if _, ok := LookupMMDSRoute(map[string]string{}, "/x"); ok {
		t.Fatal("expected an absent namespace to be not found")
	}
}

func TestMMDSSpecifiesSecretName(t *testing.T) {
	meta := map[string]string{NsMMDS: `{"version":1,"secrets":[{"name":"key1"}],"routes":[{"path":"/x","type":"secret","secret_name":"key1"}]}`}

	if !MMDSSpecifiesSecretName(meta, "key1") {
		t.Fatal("expected key1 to be specified")
	}
	if MMDSSpecifiesSecretName(meta, "key2") {
		t.Fatal("expected key2 to be unspecified")
	}
	if MMDSSpecifiesSecretName(map[string]string{}, "key1") {
		t.Fatal("expected an absent namespace to specify nothing")
	}
	if MMDSSpecifiesSecretName(map[string]string{NsMMDS: "not json"}, "key1") {
		t.Fatal("expected corrupt namespace value to specify nothing")
	}
}

func TestMMDSConfigDigest(t *testing.T) {
	meta1 := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/x","type":"static","data":"a"}]}`}
	meta2 := map[string]string{NsMMDS: `{"version":1,"routes":[{"path":"/y","type":"static","data":"b"}]}`}

	d1 := MMDSConfigDigest(meta1)
	if d1 == "" {
		t.Fatal("expected a non-empty digest")
	}
	if d1 != MMDSConfigDigest(meta1) {
		t.Fatal("digest is not deterministic")
	}
	if d1 == MMDSConfigDigest(meta2) {
		t.Fatal("distinct specifications produced the same digest")
	}
	if got := MMDSConfigDigest(map[string]string{}); got != "" {
		t.Fatalf("expected an empty digest for an absent namespace, got %q", got)
	}
}
