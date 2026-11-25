package placer

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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

func TestFileGroupSnapshotKeepsDetachedRecordsUntilNextPass(t *testing.T) {
	source := testGroupSource(t,
		clusterstate.SandboxGroupRecord{Group: "/b"},
		clusterstate.SandboxGroupRecord{
			Group: "/a", TemplateRef: testGroupTemplate(), TargetPort: 123,
			Config:        map[string]string{"app": "old"},
			Metadata:      map[string]string{"revision": "old"},
			NodeSelectors: []map[string]string{{"pool": "old"}},
		})
	snapshot, err := source.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	page, err := snapshot.Range(t.Context(), "", 1)
	if err != nil || !reflect.DeepEqual(page.Groups, []string{"/a"}) || page.NextCursor != "1" {
		t.Fatalf("first snapshot page: %+v, %v", page, err)
	}
	record, found, err := snapshot.Read(t.Context(), "/a")
	if err != nil || !found {
		t.Fatalf("snapshot read: found=%v, %v", found, err)
	}
	// Neither returned slices/maps nor a later file edit may alter this pass.
	page.Groups[0] = "/changed"
	record.Config["app"] = "changed"
	record.Metadata["revision"] = "changed"
	record.NodeSelectors[0]["pool"] = "changed"
	record.NodeSelectors = append(record.NodeSelectors, map[string]string{"pool": "extra"})
	writeGroupFile(t, source.dir, "group-1.json", clusterstate.SandboxGroupRecord{Group: "/a", TargetPort: 456})
	if err := os.Remove(filepath.Join(source.dir, "group-0.json")); err != nil {
		t.Fatal(err)
	}
	again, found, err := snapshot.Read(t.Context(), "/a")
	if err != nil || !found || again.TargetPort != 123 || again.Config["app"] != "old" ||
		again.Metadata["revision"] != "old" || len(again.NodeSelectors) != 1 || again.NodeSelectors[0]["pool"] != "old" {
		t.Fatalf("snapshot changed: %+v, found=%v, %v", again, found, err)
	}
	all, err := snapshot.Range(t.Context(), "", 0)
	if err != nil || !reflect.DeepEqual(all.Groups, []string{"/a", "/b"}) || all.NextCursor != "" {
		t.Fatalf("snapshot names changed: %+v, %v", all, err)
	}
	last, err := snapshot.Range(t.Context(), page.NextCursor, 1)
	if err != nil || !reflect.DeepEqual(last.Groups, []string{"/b"}) || last.NextCursor != "" {
		t.Fatalf("next snapshot page: %+v, %v", last, err)
	}
	if _, found, err := snapshot.Read(t.Context(), "/missing"); err != nil || found {
		t.Fatalf("missing snapshot record: found=%v, %v", found, err)
	}
	next, err := source.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	updated, found, err := next.Read(t.Context(), "/a")
	if err != nil || !found || updated.TargetPort != 456 || updated.TemplateRef != "" {
		t.Fatalf("next pass missed replacement: %+v, found=%v, %v", updated, found, err)
	}
	if _, found, err := next.Read(t.Context(), "/b"); err != nil || found {
		t.Fatalf("next pass retained removed input: found=%v, %v", found, err)
	}
}

func TestFileGroupSnapshotValidationAndCancellation(t *testing.T) {
	duplicate := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/g"}, clusterstate.SandboxGroupRecord{Group: "/g"})
	if _, err := duplicate.snapshot(t.Context()); err == nil {
		t.Fatal("snapshot accepted duplicate definitions")
	}
	source := testGroupSource(t, clusterstate.SandboxGroupRecord{Group: "/g"})
	snapshot, err := source.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Range(t.Context(), "-1", 1); err == nil {
		t.Fatal("snapshot accepted invalid cursor")
	}
	if page, err := snapshot.Range(t.Context(), "1", 1); err != nil || len(page.Groups) != 0 || page.NextCursor != "" {
		t.Fatalf("snapshot end cursor: %+v, %v", page, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := source.snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot cancellation: %v", err)
	}
	if _, err := snapshot.Range(ctx, "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("range cancellation: %v", err)
	}
	if _, _, err := snapshot.Read(ctx, "/g"); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source.dir, "group-0.json"), []byte(`{"group":"/g","unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := source.snapshot(t.Context()); err == nil {
		t.Fatal("snapshot skipped strict input validation")
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
