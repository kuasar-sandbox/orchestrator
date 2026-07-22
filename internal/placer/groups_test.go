package placer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
)

const testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testManifestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestFileGroupSourceProviderMethods(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group: "/g", KeyRevision: 1,
		ManifestKey:           clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		AuthKey:               clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
		TemplateRef:           "tmpl",
		AllowTemplateOverride: true,
		NodeSelectors:         []map[string]string{{"pool": "p"}},
	})

	ctx := context.Background()
	g, found, err := src.Get(ctx, "/g")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if g.TemplateRef != "tmpl" || !g.AllowTemplateOverride {
		t.Fatalf("group=%+v", g)
	}
	hint, found, err := src.GetPlacementHint(ctx, "/g")
	if err != nil || !found || len(hint.NodeSelectors) != 1 || hint.NodeSelectors[0]["pool"] != "p" {
		t.Fatalf("hint=%+v found=%v err=%v", hint, found, err)
	}
	key, found, err := src.GetManifestKey(ctx, "/g")
	if err != nil || !found || key.Value != testManifestKey {
		t.Fatalf("key=%+v found=%v err=%v", key, found, err)
	}
	authKey, found, err := src.GetAuthKey(ctx, "/g")
	if err != nil || !found || authKey.Value != testAuthKey {
		t.Fatalf("auth=%+v found=%v err=%v", authKey, found, err)
	}
}

func TestFileGroupSourceAcceptsSecretShorthand(t *testing.T) {
	dir := t.TempDir()
	raw := `{"group":"/g","key_revision":1,"manifest_key":"` + testManifestKey + `","auth_key":"` + testAuthKey + `","node_selectors":[{"pool":"p"}]}`
	if err := os.WriteFile(filepath.Join(dir, "g.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	key, found, err := src.GetManifestKey(context.Background(), "/g")
	if err != nil || !found || key.Type != clusterstate.SecretInline || key.Value != testManifestKey {
		t.Fatalf("key shorthand=%+v found=%v err=%v", key, found, err)
	}
}

func TestFileGroupSourceRejectsKeyMaterialWithoutRevision(t *testing.T) {
	dir := t.TempDir()
	writeGroupFile(t, dir, "g.json", clusterstate.SandboxGroupRecord{
		Group: "/g", AuthKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
	})
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := src.GetRecord(context.Background(), "/g"); err == nil {
		t.Fatal("key-bearing group without key_revision was accepted")
	}
}

func testGroupSource(t *testing.T, groups ...clusterstate.SandboxGroupRecord) *fileGroupSource {
	t.Helper()
	dir := t.TempDir()
	for i, group := range groups {
		name := strings.Trim(strings.ReplaceAll(group.Group, "/", "_"), "_")
		if name == "" {
			name = "group"
		}
		writeGroupFile(t, dir, name+"-"+string(rune('a'+i))+".json", group)
	}
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestConfiguredGroupInputsReadsIndependentSources(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeGroupFile(t, dirA, "a.json", clusterstate.SandboxGroupRecord{Group: "/a"})
	writeGroupFile(t, dirB, "b.json", clusterstate.SandboxGroupRecord{Group: "/b"})

	inputs, err := NewConfiguredGroupInputs([]clustercfg.GroupSourceConfig{
		{SourceID: "source-a", SourceType: "file", Path: dirA},
		{SourceID: "source-b", SourceType: "file", Path: dirB},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"/a", "/b"} {
		if _, found, err := inputs.Provider.Get(context.Background(), group); err != nil || !found {
			t.Fatalf("provider lookup %s found=%v err=%v", group, found, err)
		}
	}
}

func TestConfiguredGroupInputsRejectsDuplicateGroupAcrossSources(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeGroupFile(t, dirA, "a.json", clusterstate.SandboxGroupRecord{Group: "/dup"})
	writeGroupFile(t, dirB, "b.json", clusterstate.SandboxGroupRecord{Group: "/dup"})

	inputs, err := NewConfiguredGroupInputs([]clustercfg.GroupSourceConfig{
		{SourceID: "source-a", SourceType: "file", Path: dirA},
		{SourceID: "source-b", SourceType: "file", Path: dirB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := inputs.Provider.Get(context.Background(), "/dup"); err == nil {
		t.Fatal("duplicate group across sources should fail provider lookup")
	}
}

func TestFileGroupSourceRejectsDuplicateGroupInSource(t *testing.T) {
	dir := t.TempDir()
	writeGroupFile(t, dir, "a.json", clusterstate.SandboxGroupRecord{Group: "/dup"})
	writeGroupFile(t, dir, "b.json", clusterstate.SandboxGroupRecord{Group: "/dup"})
	src, err := NewFileGroupSource("source-a", dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := src.Get(context.Background(), "/dup"); err == nil {
		t.Fatal("duplicate group inside one source should fail lookup")
	}
}

func writeGroupFile(t *testing.T, dir, name string, group clusterstate.SandboxGroupRecord) {
	t.Helper()
	raw, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
