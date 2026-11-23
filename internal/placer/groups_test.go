package placer

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const testAPISecret = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testManifestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func testGroupTemplate() string {
	return "bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte("manifest://"+strings.Repeat("a", 64)))
}

func TestFileGroupSourceCompleteRecordAndPagination(t *testing.T) {
	template := testGroupTemplate()
	source := testGroupSource(t,
		clusterstate.SandboxGroupRecord{Group: "/b"},
		clusterstate.SandboxGroupRecord{Group: "/a", TemplateRef: template, ProjectID: "project", TargetPort: 123,
			ManifestKey:   clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
			APISecret:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
			NodeSelectors: []map[string]string{{"pool": "p"}},
			Config:        map[string]string{sandboxcfg.NsResource: ` { "startup" : { "memory" : "1GiB" }, "capacity" : { "memory" : "8GiB" } } `},
		})
	page, err := source.Range(t.Context(), "", 1)
	if err != nil || len(page.Groups) != 1 || page.Groups[0] != "/a" || page.NextCursor == "" {
		t.Fatalf("page: %+v %v", page, err)
	}
	record, ok, err := source.Read(t.Context(), page.Groups[0])
	if err != nil || !ok || record.TemplateRef != template || record.ProjectID != "project" || record.TargetPort != 123 ||
		record.NodeSelectors[0]["pool"] != "p" || record.APISecret.Value != testAPISecret || record.ManifestKey.Value != testManifestKey {
		t.Fatalf("incomplete import record: %v", err)
	}
	if strings.HasPrefix(record.Config[sandboxcfg.NsResource], " ") {
		t.Fatal("resource input was not normalized")
	}
	next, err := source.Range(t.Context(), page.NextCursor, 1)
	if err != nil || len(next.Groups) != 1 || next.Groups[0] != "/b" || next.NextCursor != "" {
		t.Fatalf("next: %+v %v", next, err)
	}
	if _, err := source.Range(t.Context(), "-1", 1); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	replacement := clusterstate.SandboxGroupRecord{Group: "/a", TargetPort: 456, Metadata: map[string]string{"revision": "new"}}
	writeGroupFile(t, source.dir, "group-1.json", replacement)
	got, ok, err := source.Read(t.Context(), "/a")
	if err != nil || !ok || got.TargetPort != 456 || got.TemplateRef != "" || got.Metadata["revision"] != "new" {
		t.Fatalf("mixed record revisions: %v", err)
	}
}

func TestFileGroupInputRejectsAliasesAndUnusedOrRequestFields(t *testing.T) {
	for name, rec := range map[string]clusterstate.SandboxGroupRecord{
		"alias":                           {Group: "g", TemplateRef: "tmpl"},
		"default config without template": {Group: "g", Config: map[string]string{"x": "y"}},
		"shuffle_labels":                  {Group: "g", ShuffleLabels: map[string]string{"pool": "p"}},
		"request placement":               {Group: "g", TemplateRef: testGroupTemplate(), Config: map[string]string{"kuasar-sandbox.placement": "{}"}},
		"invalid resource":                {Group: "g", TemplateRef: testGroupTemplate(), Config: map[string]string{sandboxcfg.NsResource: `{"capacity":{"memory":"-1GiB"}}`}},
		"malformed annotation":            {Group: "g", TemplateRef: "bare-snp-" + base64.RawURLEncoding.EncodeToString([]byte("manifest://"+strings.Repeat("a", 64)+"@runtime:runtime.bundle:digest:invalid"))},
	} {
		t.Run(name, func(t *testing.T) {
			src := testGroupSource(t, rec)
			if _, err := src.Range(t.Context(), "", 1); err == nil {
				t.Fatal("invalid import input accepted")
			}
		})
	}
}

func TestFileGroupCredentialShorthandAndStableDerivation(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "group.json"), []byte(`{"group":"/g","manifest_key":"`+testManifestKey+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileGroupSource("source", dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, ok, err := src.Read(t.Context(), "/g")
	if err != nil || !ok {
		t.Fatalf("read: %v", err)
	}
	cmd, err := importCommand(rec)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := hex.DecodeString(testManifestKey)
	expected := apikey.DeriveAPISecret(root)
	parsed, err := apikey.Parse(cmd.APIKey)
	if err != nil || !apikey.Verify(parsed, expected) || cmd.KeyPut.APISecret != hex.EncodeToString(expected) {
		t.Fatal("derived owner changed")
	}
	rec.APISecret = clusterstate.Secret{Value: testAPISecret}
	explicit, err := importCommand(rec)
	if err != nil || explicit.KeyPut.APISecret != testAPISecret {
		t.Fatal("explicit identity did not win")
	}
	for _, bad := range []string{"", strings.ToUpper(testAPISecret), testAPISecret[:62]} {
		rec.APISecret = clusterstate.Secret{Type: clusterstate.SecretInline, Value: bad}
		if _, err := importCommand(rec); err == nil {
			t.Fatal("invalid explicit root silently derived")
		}
	}
}

func TestConfiguredGroupSourcesRemainIndependentAndOptional(t *testing.T) {
	empty, err := NewConfiguredGroupInputs(nil)
	if err != nil || len(empty.Sources) != 0 {
		t.Fatal("empty imports require a provider")
	}
	one := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/one"})
	two := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/two"})
	input, err := NewConfiguredGroupInputs([]clustercfg.GroupSourceConfig{
		{SourceID: "one", SourceType: "file", Path: one.dir}, {SourceID: "two", SourceType: "file", Path: two.dir},
	})
	if err != nil || len(input.Sources) != 2 {
		t.Fatalf("sources: %v", err)
	}
	for i, want := range []string{"/one", "/two"} {
		page, err := input.Sources[i].Importer.Range(t.Context(), "", 1)
		if err != nil || len(page.Groups) != 1 || page.Groups[0] != want {
			t.Fatalf("mixed source: %+v %v", page, err)
		}
	}
}

func TestFileGroupSourceRejectsDuplicateGroupInSource(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/g"}, clusterstate.SandboxGroupRecord{Group: "/g"})
	if _, err := src.Range(t.Context(), "", 1); err == nil {
		t.Fatal("duplicate definition accepted")
	}
}

func testGroupSource(t *testing.T, groups ...clusterstate.SandboxGroupRecord) *fileGroupSource {
	t.Helper()
	dir := t.TempDir()
	for i, group := range groups {
		writeGroupFile(t, dir, "group-"+strconv.Itoa(i)+".json", group)
	}
	source, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func writeGroupFile(t *testing.T, dir, name string, group clusterstate.SandboxGroupRecord) {
	t.Helper()
	raw, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func putNodeList(t *testing.T, svc *Service, n clusterstate.NodeListEntry) {
	t.Helper()
	if n.Version.IsZero() {
		n.Version = nodeVersionForTest(1, 0)
	}
	raw, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	sink := svc.nodes.source("test")
	sink.reset()
	sink.put(n.NodeID, raw)
	sink.bookmark()
}
