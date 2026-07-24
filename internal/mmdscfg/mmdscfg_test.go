package mmdscfg

import (
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func testLimits() config.MMDSEndpointsConfig {
	l := config.MMDSEndpointsConfig{Enabled: true}
	l.ApplyDefaults()
	return l
}

const docExample = `
endpoints:
  - name: credentials
    path: /latest/meta-data/credentials
    backend:
      type: relay
      url: https://identity.example.com/credentials
      auth:
        header_name: X-Upstream-Assertion
  - name: user-data
    path: /latest/user-data
    backend:
      type: store
`

func TestExtractRoundTripsDocExample(t *testing.T) {
	meta := map[string]string{
		Ns:               docExample,
		"other.metadata": "keep-me",
	}
	clean, eps, err := Extract(meta, testLimits())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}
	if _, ok := clean[Ns]; ok {
		t.Fatalf("mmds namespace leaked into clean metadata: %+v", clean)
	}
	if clean["other.metadata"] != "keep-me" {
		t.Fatalf("unrelated metadata was not preserved: %+v", clean)
	}
	if len(eps) != 2 {
		t.Fatalf("got %d endpoints, want 2", len(eps))
	}
	if eps[0].Name != "credentials" || eps[0].Backend.Type != BackendRelay {
		t.Fatalf("endpoint 0 = %+v", eps[0])
	}
	if eps[0].Backend.URL != "https://identity.example.com/credentials" {
		t.Fatalf("endpoint 0 url = %q", eps[0].Backend.URL)
	}
	if eps[0].Backend.Auth == nil || eps[0].Backend.Auth.HeaderName != "X-Upstream-Assertion" {
		t.Fatalf("endpoint 0 auth = %+v", eps[0].Backend.Auth)
	}
	if eps[1].Name != "user-data" || eps[1].Backend.Type != BackendStore {
		t.Fatalf("endpoint 1 = %+v", eps[1])
	}
}

func TestExtractAbsentNamespaceIsNoop(t *testing.T) {
	meta := map[string]string{"other.metadata": "keep-me"}
	clean, eps, err := Extract(meta, testLimits())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}
	if len(eps) != 0 {
		t.Fatalf("got %d endpoints, want 0", len(eps))
	}
	if clean["other.metadata"] != "keep-me" {
		t.Fatalf("metadata mutated: %+v", clean)
	}
}

func TestExtractDisabledRejectsPresentNamespace(t *testing.T) {
	limits := testLimits()
	limits.Enabled = false
	_, _, err := Extract(map[string]string{Ns: docExample}, limits)
	if err == nil || !strings.Contains(err.Error(), "enabled=false") {
		t.Fatalf("Extract error = %v, want enabled=false rejection", err)
	}
}

// TestExtractDisabledRejectsBlankNamespace: a present-but-whitespace-only
// namespace value must be rejected exactly like a non-blank one when
// disabled — trimming to empty must never be treated the same as the key
// being absent (which is the one case allowed to pass through silently).
func TestExtractDisabledRejectsBlankNamespace(t *testing.T) {
	limits := testLimits()
	limits.Enabled = false
	_, _, err := Extract(map[string]string{Ns: "   "}, limits)
	if err == nil || !strings.Contains(err.Error(), "enabled=false") {
		t.Fatalf("Extract error = %v, want enabled=false rejection for a blank-but-present namespace", err)
	}
}

// TestExtractBlankNamespaceIsStrippedWhenEnabled: a present-but-blank
// namespace, when enabled, is not a validation error (mirrors "no endpoints
// declared") — but unlike a genuinely absent key, the raw key must still be
// stripped from the returned metadata; returning the original map here
// would leak the (blank) key into guest-visible metadata.
func TestExtractBlankNamespaceIsStrippedWhenEnabled(t *testing.T) {
	meta := map[string]string{Ns: "   ", "other.metadata": "keep-me"}
	clean, eps, err := Extract(meta, testLimits())
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}
	if len(eps) != 0 {
		t.Fatalf("got %d endpoints, want 0", len(eps))
	}
	if _, present := clean[Ns]; present {
		t.Fatalf("clean metadata still carries %q: %+v", Ns, clean)
	}
	if clean["other.metadata"] != "keep-me" {
		t.Fatalf("metadata mutated: %+v", clean)
	}
}

func TestExtractBackendFieldLeakage(t *testing.T) {
	cases := map[string]string{
		"store with url": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: store
      url: https://example.com
`,
		"relay missing auth": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: relay
      url: https://example.com
`,
		"relay null auth": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: relay
      url: https://example.com
      auth:
`,
		"relay extra field": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: relay
      url: https://example.com
      auth:
        header_name: X-Auth
      extra: 1
`,
		"relay auth extra field": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: relay
      url: https://example.com
      auth:
        header_name: X-Auth
        extra: 1
`,
		"relay missing url": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: relay
      auth:
        header_name: X-Auth
`,
		"unknown backend type": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: proxy
`,
		"missing backend type": `
endpoints:
  - name: a
    path: /latest/a
    backend:
      url: https://example.com
`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := Extract(map[string]string{Ns: raw}, testLimits())
			if err == nil {
				t.Fatalf("Extract succeeded, want a validation error")
			}
		})
	}
}

func TestExtractPathValidationMatrix(t *testing.T) {
	base := func(path string) string {
		return "endpoints:\n  - name: a\n    path: " + path + "\n    backend:\n      type: store\n"
	}
	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid", "/latest/user-data", false},
		{"percent-escape", "/latest/user%2Ddata", true},
		{"dot-segment", "/latest/../etc", true},
		{"trailing-slash", "/latest/user-data/", true},
		{"wildcard", "/latest/*", true},
		{"under-builtin-reserved", "/latest/api/token", true},
		{"under-operator-reserved", "/internal/admin", true},
		{"non-matching-prefix-not-rejected", "/latest/api-extra", false},
		{"relative", "latest/user-data", true},
		{"backslash", "/latest\\user-data", true},
		{"query", "/latest/user-data?x=1", true},
		{"empty-segment", "/latest//user-data", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Extract(map[string]string{Ns: base(tc.path)}, testLimits())
			if tc.wantErr && err == nil {
				t.Fatalf("Extract succeeded for path %q, want error", tc.path)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Extract failed for path %q: %v", tc.path, err)
			}
		})
	}
}

func TestExtractNameBoundaries(t *testing.T) {
	doc := func(name string) string {
		return "endpoints:\n  - name: \"" + name + "\"\n    path: /latest/user-data\n    backend:\n      type: store\n"
	}
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"a", false},
		{strings.Repeat("a", 63), false},
		{strings.Repeat("a", 64), true}, // one over 63-char max
		{"A", true},                     // uppercase rejected
		{"1abc", true},                  // must start with a letter
		{"a_b-c9", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Extract(map[string]string{Ns: doc(tc.name)}, testLimits())
			if tc.wantErr && err == nil {
				t.Fatalf("Extract succeeded for name %q, want error", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Extract failed for name %q: %v", tc.name, err)
			}
		})
	}
}

func TestExtractRejectsDuplicateNameAndPath(t *testing.T) {
	dupName := `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: store
  - name: a
    path: /latest/b
    backend:
      type: store
`
	if _, _, err := Extract(map[string]string{Ns: dupName}, testLimits()); err == nil {
		t.Fatal("Extract succeeded with duplicate name, want error")
	}

	dupPath := `
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: store
  - name: b
    path: /latest/a
    backend:
      type: store
`
	if _, _, err := Extract(map[string]string{Ns: dupPath}, testLimits()); err == nil {
		t.Fatal("Extract succeeded with duplicate path, want error")
	}
}

func TestExtractEndpointCountLimit(t *testing.T) {
	limits := testLimits()
	limits.MaxEndpointsPerSandbox = 2

	var sb strings.Builder
	sb.WriteString("endpoints:\n")
	for i := 0; i < 2; i++ {
		sb.WriteString("  - name: ep" + string(rune('a'+i)) + "\n    path: /latest/ep" + string(rune('a'+i)) + "\n    backend:\n      type: store\n")
	}
	if _, _, err := Extract(map[string]string{Ns: sb.String()}, limits); err != nil {
		t.Fatalf("Extract failed at the boundary (2 endpoints, limit 2): %v", err)
	}

	sb.WriteString("  - name: epc\n    path: /latest/epc\n    backend:\n      type: store\n")
	if _, _, err := Extract(map[string]string{Ns: sb.String()}, limits); err == nil {
		t.Fatal("Extract succeeded one over max_endpoints_per_sandbox, want error")
	}
}

func TestExtractRejectsUnknownTopLevelField(t *testing.T) {
	raw := `
unexpected_field: true
endpoints:
  - name: a
    path: /latest/a
    backend:
      type: store
`
	if _, _, err := Extract(map[string]string{Ns: raw}, testLimits()); err == nil {
		t.Fatal("Extract succeeded with an unknown top-level field, want error")
	}
}

func TestExtractRejectsEmptyEndpointsList(t *testing.T) {
	raw := "endpoints: []\n"
	if _, _, err := Extract(map[string]string{Ns: raw}, testLimits()); err == nil {
		t.Fatal("Extract succeeded with an empty endpoints list, want error")
	}
}
