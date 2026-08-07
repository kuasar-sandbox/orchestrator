package sandboxcfg

import (
	"strings"
	"testing"
)

func testMMDSPolicy() MMDSPolicy {
	return MMDSPolicy{
		Enabled: true, MaxRoutesPerSandbox: 32, MaxNamespaceBytes: 64 * 1024,
		MaxStaticBodyBytes: 16 * 1024, MaxSecretValueBytes: 16 * 1024,
		ReservedPathPrefixes: []string{"/latest/api/", "/internal/"},
		Services:             map[string]string{"external-mmds": "unix:///run/kuasar/mmds.sock"},
	}
}

func strptr(value string) *string { return &value }

func TestExtractMMDSHeaderAndMetadataTopLevelMerge(t *testing.T) {
	tests := []struct {
		name       string
		metadata   string
		header     *string
		wantRoutes int
		wantSecret string
	}{
		{
			name:       "header only routes",
			header:     strptr(`{"routes":[{"path":"/header","data":"h"}]}`),
			wantRoutes: 1,
		},
		{
			name:       "metadata only routes",
			metadata:   `{"routes":[{"path":"/metadata","data":"m"}]}`,
			wantRoutes: 1,
		},
		{
			name:       "header secrets metadata routes",
			metadata:   `{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`,
			header:     strptr(`{"secrets":{"key":"header-value"}}`),
			wantRoutes: 1, wantSecret: "header-value",
		},
		{
			name:       "header routes metadata secrets",
			metadata:   `{"secrets":{"key":"metadata-value"}}`,
			header:     strptr(`{"routes":[{"path":"/secret","type":"secret","secret":"key"}]}`),
			wantRoutes: 1, wantSecret: "metadata-value",
		},
		{
			name:       "same key header overrides",
			metadata:   `{"secrets":{"key":"metadata-value"},"routes":[{"path":"/metadata"}]}`,
			header:     strptr(`{"secrets":{"key":"header-value"},"routes":[{"path":"/header","type":"secret","secret":"key"}]}`),
			wantRoutes: 1, wantSecret: "header-value",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var meta map[string]string
			if tc.metadata != "" {
				meta = map[string]string{NsMMDS: tc.metadata}
			}
			doc, persisted, err := ExtractMMDS(meta, tc.header, testMMDSPolicy())
			if err != nil {
				t.Fatalf("ExtractMMDS: %v", err)
			}
			if len(doc.Routes) != tc.wantRoutes {
				t.Fatalf("route count = %d, want %d", len(doc.Routes), tc.wantRoutes)
			}
			if tc.wantSecret != "" && string(doc.SecretValues["key"]) != tc.wantSecret {
				t.Fatal("effective secret value mismatch")
			}
			if raw := persisted[NsMMDS]; strings.Contains(raw, "header-value") || strings.Contains(raw, "metadata-value") || strings.Contains(raw, `"secrets"`) {
				t.Fatal("persisted metadata contains initial secret")
			}
		})
	}
}

func TestExtractMMDSExplicitEmptyTopLevelOverride(t *testing.T) {
	meta := map[string]string{NsMMDS: `{
		"secrets":{"key":"metadata-value"},
		"routes":[{"path":"/secret","type":"secret","secret":"key"}]
	}`}

	doc, _, err := ExtractMMDS(meta, strptr(`{"secrets":{}}`), testMMDSPolicy())
	if err != nil {
		t.Fatalf("empty secrets override: %v", err)
	}
	if len(doc.SecretValues) != 0 || len(doc.Routes) != 1 {
		t.Fatalf("effective counts: secrets=%d routes=%d", len(doc.SecretValues), len(doc.Routes))
	}

	doc, persisted, err := ExtractMMDS(
		map[string]string{NsMMDS: `{"routes":[{"path":"/metadata"}]}`},
		strptr(`{"routes":[]}`), testMMDSPolicy(),
	)
	if err != nil {
		t.Fatalf("empty routes override: %v", err)
	}
	if len(doc.Routes) != 0 || persisted[NsMMDS] != `{"routes":[]}` {
		t.Fatal("explicit empty routes did not override metadata routes")
	}
}

func TestExtractMMDSRejectsStrictJSONFailures(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"unknown top field", `{"routes":[],"version":1}`},
		{"unknown route field", `{"routes":[{"path":"/x","backend":"secret"}]}`},
		{"wrong case field", `{"Routes":[]}`},
		{"duplicate top key", `{"routes":[],"routes":[]}`},
		{"duplicate nested key", `{"routes":[{"path":"/x","path":"/y"}]}`},
		{"trailing value", `{"routes":[]} {"routes":[]}`},
		{"trailing garbage", `{"routes":[]}]`},
		{"malformed", `{"routes":[`},
		{"null routes", `{"routes":null}`},
		{"null secrets", `{"secrets":null}`},
		{"null secret value", `{"secrets":{"key":null}}`},
		{"null route data", `{"routes":[{"path":"/x","data":null}]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ExtractMMDS(map[string]string{NsMMDS: tc.raw}, nil, testMMDSPolicy()); err == nil {
				t.Fatal("accepted invalid MMDS JSON document")
			}
		})
	}
}

func TestExtractMMDSRejectsInvalidRouteFieldCombinations(t *testing.T) {
	tests := []string{
		`{"routes":[{"path":"/x","secret":"key"}]}`,
		`{"routes":[{"path":"/x","service":"external-mmds"}]}`,
		`{"routes":[{"path":"/x","type":"secret"}]}`,
		`{"routes":[{"path":"/x","type":"secret","secret":"key","data":""}]}`,
		`{"routes":[{"path":"/x","type":"secret","secret":"key","service":""}]}`,
		`{"routes":[{"path":"/x","type":"service","service":"external-mmds","content_type":""}]}`,
		`{"routes":[{"path":"/x","type":"service","service":"missing"}]}`,
		`{"routes":[{"path":"/x","type":""}]}`,
	}
	for _, raw := range tests {
		if _, _, err := ExtractMMDS(map[string]string{NsMMDS: raw}, nil, testMMDSPolicy()); err == nil {
			t.Error("accepted invalid route field combination")
		}
	}
}

func TestExtractMMDSRejectsDuplicateAndReservedPaths(t *testing.T) {
	for _, raw := range []string{
		`{"routes":[{"path":"/same"},{"path":"/same"}]}`,
		`{"routes":[{"path":"/"}]}`,
		`{"routes":[{"path":"/latest/api/token"}]}`,
		`{"routes":[{"path":"/internal/data"}]}`,
		`{"routes":[{"path":"/a/../b"}]}`,
		`{"routes":[{"path":"/a//b"}]}`,
		`{"routes":[{"path":"/a%2fb"}]}`,
		`{"routes":[{"path":"/a\\b"}]}`,
		`{"routes":[{"path":"/a/"}]}`,
		`{"routes":[{"path":"/a*"}]}`,
		`{"routes":[{"path":"/a b"}]}`,
		`{"routes":[{"path":"/路径"}]}`,
	} {
		if _, _, err := ExtractMMDS(map[string]string{NsMMDS: raw}, nil, testMMDSPolicy()); err == nil {
			t.Error("accepted invalid path document")
		}
	}
}

func TestExtractMMDSPersistsMinimalRoutes(t *testing.T) {
	raw := `{
		"secrets":{"key":"value"},
		"routes":[
			{"path":"/explicit","type":"static","data":"x"},
			{"path":"/implicit","data":"y"},
			{"path":"/secret","type":"secret","secret":"key"}
		]
	}`
	doc, meta, err := ExtractMMDS(map[string]string{NsMMDS: raw}, nil, testMMDSPolicy())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"routes":[{"path":"/explicit","data":"x"},{"path":"/implicit","data":"y"},{"path":"/secret","type":"secret","secret":"key"}]}`
	if meta[NsMMDS] != want {
		t.Fatalf("persisted routes = %s, want %s", meta[NsMMDS], want)
	}
	if doc.Routes[0].Type != "" || doc.Routes[1].Type != "" {
		t.Fatalf("static defaults were persisted: %+v", doc.Routes)
	}
	for _, route := range doc.Routes {
		if route.ContentType != "" {
			t.Fatalf("content type default was persisted: %+v", route)
		}
		if got := MMDSRuntimeContentType(route); got != "text/plain" {
			t.Fatalf("runtime content type = %q", got)
		}
	}
}

func TestExtractMMDSInitialSecretValidation(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxSecretValueBytes = 3
	for _, raw := range []string{
		`{"secrets":{"unused":"x"},"routes":[]}`,
		`{"secrets":{"key":"four"},"routes":[{"path":"/x","type":"secret","secret":"key"}]}`,
	} {
		if _, _, err := ExtractMMDS(map[string]string{NsMMDS: raw}, nil, policy); err == nil {
			t.Error("accepted invalid initial secret document")
		}
	}
	if _, _, err := ExtractMMDS(map[string]string{NsMMDS: `{"routes":[{"path":"/x","type":"secret","secret":"key"}]}`}, nil, policy); err != nil {
		t.Fatalf("unresolved secret route rejected: %v", err)
	}
}

func TestExtractMMDSImportSecretsRejectsAnyRoutesKey(t *testing.T) {
	routes := []MMDSRoute{{Path: "/x", Type: MMDSRouteSecret, Secret: "key"}}
	for _, input := range []struct {
		meta   map[string]string
		header *string
	}{
		{meta: map[string]string{NsMMDS: `{"routes":[]}`}},
		{header: strptr(`{"routes":[]}`)},
		{meta: map[string]string{NsMMDS: `{"routes":[]}`}, header: strptr(`{"secrets":{"key":"v"}}`)},
	} {
		if _, err := ExtractMMDSImportSecrets(input.meta, input.header, routes, testMMDSPolicy()); err == nil {
			t.Fatal("import accepted a request routes key")
		}
	}
	values, err := ExtractMMDSImportSecrets(
		map[string]string{NsMMDS: `{"secrets":{"key":"metadata"}}`},
		strptr(`{"secrets":{"key":"header"}}`), routes, testMMDSPolicy(),
	)
	if err != nil || string(values["key"]) != "header" {
		t.Fatalf("import secret merge mismatch: err=%v", err)
	}
}

func TestExtractMMDSPayloadLimitAppliesToEachSource(t *testing.T) {
	policy := testMMDSPolicy()
	policy.MaxNamespaceBytes = 16
	if _, _, err := ExtractMMDS(map[string]string{NsMMDS: strings.Repeat(" ", 17)}, nil, policy); err == nil {
		t.Fatal("accepted oversized metadata")
	}
	if _, _, err := ExtractMMDS(nil, strptr(strings.Repeat(" ", 17)), policy); err == nil {
		t.Fatal("accepted oversized header")
	}
}
