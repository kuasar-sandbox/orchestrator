package placer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

const testAPISecret = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testManifestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestFileGroupSourceProviderMethods(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		APISecret:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
		TemplateRef:   "tmpl",
		NodeSelectors: []map[string]string{{"pool": "p"}},
	})

	ctx := context.Background()
	g, found, err := src.Get(ctx, "/g")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if g.TemplateRef != "tmpl" {
		t.Fatalf("group=%+v", g)
	}
	hint, found, err := src.GetPlacementHint(ctx, "/g")
	if err != nil || !found || len(hint.NodeSelectors) != 1 || hint.NodeSelectors[0]["pool"] != "p" {
		t.Fatalf("hint=%+v found=%v err=%v", hint, found, err)
	}
	key, found, err := src.GetKey(ctx, "/g")
	if err != nil || !found || key.Value != testManifestKey {
		t.Fatalf("key=%+v found=%v err=%v", key, found, err)
	}
	apiSecret, found, err := src.GetAPISecret(ctx, "/g")
	if err != nil || !found || apiSecret.Value != testAPISecret {
		t.Fatalf("api secret=%+v found=%v err=%v", apiSecret, found, err)
	}
	page, err := src.Range(ctx, "", 10)
	if err != nil || len(page.Groups) != 1 || page.Groups[0] != "/g" {
		t.Fatalf("range=%+v err=%v", page, err)
	}
}

func TestFileGroupSourceValidatesAndCanonicalizesResourcePatch(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group:       "/g",
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		Config: map[string]string{
			sandboxcfg.NsResource: ` { "startup" : { "memory" : "1GiB" }, "capacity" : { "memory" : "8GiB" } } `,
		},
	})
	group, found, err := src.Get(context.Background(), "/g")
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if got, want := group.Config[sandboxcfg.NsResource], `{"capacity":{"memory":"8GiB"},"startup":{"memory":"1GiB"}}`; got != want {
		t.Fatalf("canonical resource = %q, want %q", got, want)
	}

	bad := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group:       "/bad",
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		Config: map[string]string{
			sandboxcfg.NsResource: `{"overhead":{"memory":"64MiB"}}`,
		},
	})
	if _, _, err := bad.Get(context.Background(), "/bad"); err == nil || !strings.Contains(err.Error(), "not request-configurable") {
		t.Fatalf("invalid group resource error = %v", err)
	}
}

func TestMergeConfigUsesResourceLeavesAndDoesNotHideInvalidGroup(t *testing.T) {
	group := map[string]string{
		sandboxcfg.NsResource: `{"capacity":{"memory":"8GiB"},"allocatable":{"cpu":0.5}}`,
		sandboxcfg.NsNetwork:  `{"hostname":"group"}`,
	}
	create := map[string]string{
		sandboxcfg.NsResource: `{"allocatable":{"memory":"512MiB"},"startup":{"memory":"1GiB"}}`,
		sandboxcfg.NsNetwork:  `{"hostname":"create"}`,
	}
	got, err := mergeConfig(group, create)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"capacity":{"memory":"8GiB"},"allocatable":{"cpu":0.5,"memory":"512MiB"},"startup":{"memory":"1GiB"}}`; got[sandboxcfg.NsResource] != want {
		t.Fatalf("resource config = %s, want %s", got[sandboxcfg.NsResource], want)
	}
	if got[sandboxcfg.NsNetwork] != create[sandboxcfg.NsNetwork] {
		t.Fatalf("non-resource namespace did not retain whole-value precedence: %+v", got)
	}

	group[sandboxcfg.NsResource] = `{"allocatable":{"deflate_on_oom":false}}`
	if _, err := mergeConfig(group, create); err == nil || !strings.Contains(err.Error(), "node-managed") {
		t.Fatalf("valid create config hid invalid group resource: %v", err)
	}
}

func TestAnswerMarksCrossLayerResourceConflictAsInvalidConfig(t *testing.T) {
	svc := testServiceWithGroups(t, clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		APISecret:   clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
		TemplateRef: "tmpl",
		Config: map[string]string{
			sandboxcfg.NsResource: `{"capacity":{"memory":"128MiB"}}`,
		},
	})
	putNodeList(t, svc, clusterstate.NodeListEntry{NodeID: "n1"})
	res := svc.answer(context.Background(), &routesync.PlaceReq{
		Group: "/g", RouteKey: "rk", SandboxID: "sb-1", ExcludeNodeIDs: []string{"n1"},
		Config: map[string]string{
			sandboxcfg.NsResource: `{"allocatable":{"memory":"256MiB"}}`,
		},
	})
	if !res.InvalidConfig || res.Error == "" || res.NodeID != "" {
		t.Fatalf("cross-layer resource conflict = %+v", res)
	}
}

func TestFileGroupSourceAcceptsSecretShorthand(t *testing.T) {
	dir := t.TempDir()
	raw := `{"group":"/g","manifest_key":"` + testManifestKey + `","api_secret":"` + testAPISecret + `","node_selectors":[{"pool":"p"}]}`
	if err := os.WriteFile(filepath.Join(dir, "g.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	key, found, err := src.GetKey(context.Background(), "/g")
	if err != nil || !found || key.Type != clusterstate.SecretInline || key.Value != testManifestKey {
		t.Fatalf("key shorthand=%+v found=%v err=%v", key, found, err)
	}
}

func TestFileGroupSourceDerivesMissingAPISecret(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group:       "/g",
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
	})

	got, found, err := src.GetAPISecret(context.Background(), "/g")
	if err != nil || !found {
		t.Fatalf("GetAPISecret found=%v err=%v", found, err)
	}
	manifestKey, err := hex.DecodeString(testManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	want := hex.EncodeToString(apikey.DeriveAPISecret(manifestKey))
	if got.Type != clusterstate.SecretInline || got.Value != want {
		t.Fatalf("GetAPISecret=%+v, want inline %q", got, want)
	}
}

func TestFileGroupSourceExplicitAPISecretWins(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group:       "/g",
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		APISecret:   clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
	})

	got, found, err := src.GetAPISecret(context.Background(), "/g")
	if err != nil || !found || got.Value != testAPISecret {
		t.Fatalf("GetAPISecret=%+v found=%v err=%v", got, found, err)
	}
}

func TestCredentialRootsRequireLowercaseHex32(t *testing.T) {
	badValues := []string{
		testManifestKey[:62],
		strings.ToUpper(testManifestKey),
		testManifestKey[:63] + "g",
	}
	for _, value := range badValues {
		if _, err := normalizeManifestKey(clusterstate.Secret{Type: clusterstate.SecretInline, Value: value}); err == nil {
			t.Fatalf("normalizeManifestKey(%q) unexpectedly succeeded", value)
		}
		if _, err := materializeAPISecret(clusterstate.Secret{}, clusterstate.Secret{Type: clusterstate.SecretInline, Value: value}); err == nil {
			t.Fatalf("materializeAPISecret(%q) unexpectedly succeeded", value)
		}
	}
}

func TestVerifyAPIKeyUsesAPISecret(t *testing.T) {
	apiSecret, err := hex.DecodeString(testAPISecret)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := apikey.Mint(apiSecret)
	if err != nil {
		t.Fatal(err)
	}
	if !verifyAPIKey(testAPISecret, encoded) {
		t.Fatal("API key signed by APISecret was rejected")
	}
	if verifyAPIKey(testManifestKey, encoded) {
		t.Fatal("ManifestKey must not authenticate an API key")
	}
}

func TestCredentialPatchesUseFullFingerprints(t *testing.T) {
	manifestFP, _, _, _, err := manifestKeyPatch("/g", clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey})
	if err != nil {
		t.Fatal(err)
	}
	apiFP, _, _, _, err := apiSecretPatch("/g", clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifestFP) != 64 || len(apiFP) != 64 {
		t.Fatalf("fingerprint lengths manifest=%d api=%d, want 64", len(manifestFP), len(apiFP))
	}
}

func TestCredentialPairPatchDerivesMissingAPISecret(t *testing.T) {
	apiFP, apiType, apiValue, _, manifestFP, manifestType, manifestValue, _, err := credentialPairPatch(
		"/g",
		clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		clusterstate.Secret{},
	)
	if err != nil {
		t.Fatal(err)
	}
	manifestKey, err := hex.DecodeString(testManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	wantAPISecret := hex.EncodeToString(apikey.DeriveAPISecret(manifestKey))
	if apiFP == "" || manifestFP == "" || apiType != clusterstate.SecretInline || manifestType != clusterstate.SecretInline || apiValue != wantAPISecret || manifestValue != testManifestKey {
		t.Fatalf("credential pair mismatch: api=(%q,%q,%q) manifest=(%q,%q,%q)", apiFP, apiType, apiValue, manifestFP, manifestType, manifestValue)
	}
}

func TestCredentialPairPatchRejectsHalfPair(t *testing.T) {
	_, _, _, _, _, _, _, _, err := credentialPairPatch(
		"/g",
		clusterstate.Secret{},
		clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
	)
	if err == nil {
		t.Fatal("APISecret without ManifestKey unexpectedly formed a credential pair")
	}
}

func TestCredentialPairPatchRejectsIncompleteExplicitCarriers(t *testing.T) {
	fullFingerprint := strings.Repeat("a", 64)
	tests := []struct {
		name        string
		manifestKey clusterstate.Secret
		apiSecret   clusterstate.Secret
	}{
		{
			name:        "APISecret ref without value",
			manifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
			apiSecret:   clusterstate.Secret{Type: clusterstate.SecretRef, Fingerprint: fullFingerprint},
		},
		{
			name:        "ManifestKey ref without value",
			manifestKey: clusterstate.Secret{Type: clusterstate.SecretRef, Fingerprint: fullFingerprint},
		},
		{
			name:        "APISecret inline without value",
			manifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
			apiSecret:   clusterstate.Secret{Type: clusterstate.SecretInline},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, _, _, _, _, _, err := credentialPairPatch("/g", tt.manifestKey, tt.apiSecret); err == nil {
				t.Fatal("incomplete explicit carrier unexpectedly formed a credential pair")
			}
		})
	}
}

func TestVerifyKeyDerivesMissingAPISecretFromManifestKey(t *testing.T) {
	manifestKey, err := hex.DecodeString(testManifestKey)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := apikey.Mint(apikey.DeriveAPISecret(manifestKey))
	if err != nil {
		t.Fatal(err)
	}
	svc := NewRemoteLinksWithGroups(nil, derivingAPISecretProvider{}, nil, clustercfg.PlacementConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/verify?group=/g", nil)
	req.Header.Set("X-API-KEY", encoded)
	rec := httptest.NewRecorder()

	svc.serveVerifyKey(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("serveVerifyKey status=%d body=%q, want 200", rec.Code, rec.Body.String())
	}
}

type derivingAPISecretProvider struct{}

func (derivingAPISecretProvider) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{Group: "/g"}, true, nil
}

func (derivingAPISecretProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, true, nil
}

func (derivingAPISecretProvider) GetKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey}, true, nil
}

func (derivingAPISecretProvider) GetAPISecret(context.Context, string) (clusterstate.Secret, bool, error) {
	return clusterstate.Secret{}, false, nil
}

func TestAnswerIncludesGroupMaterial(t *testing.T) {
	svc := testServiceWithGroups(t, clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		APISecret:   clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
		TemplateRef: "tmpl", Config: map[string]string{"a": "1"}, NodeSelectors: []map[string]string{{"pool": "p"}},
	})
	putNodeList(t, svc, clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}})

	res := svc.answer(context.Background(), &routesync.PlaceReq{Group: "/g", RouteKey: "rk", SandboxID: "sb-1", Config: map[string]string{"b": "2"}})
	if res.NoNode || res.Error != "" {
		t.Fatalf("answer failed: %+v", res)
	}
	if res.NodeID != "n1" || res.TemplateRef != "tmpl" || res.Config["a"] != "1" || res.Config["b"] != "2" {
		t.Fatalf("placement material mismatch: %+v", res)
	}
	if res.APISecretFingerprint == "" {
		t.Fatalf("placement omitted the full APISecret fingerprint: %+v", res)
	}
}

func TestFileRemovalStopsNewPlacement(t *testing.T) {
	dir := t.TempDir()
	writeGroupFile(t, dir, "g.json", clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		APISecret:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAPISecret},
		TemplateRef:   "tmpl",
		NodeSelectors: []map[string]string{{"pool": "p"}},
	})
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewRemoteLinksWithGroups(nil, src, testImportSources("test", src), clustercfg.PlacementConfig{Candidates: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	putNodeList(t, svc, clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}})

	if res := svc.answer(context.Background(), &routesync.PlaceReq{Group: "/g", RouteKey: "rk"}); res.NoNode || res.Error != "" {
		t.Fatalf("initial placement failed: %+v", res)
	}
	if err := os.Remove(filepath.Join(dir, "g.json")); err != nil {
		t.Fatal(err)
	}
	if res := svc.answer(context.Background(), &routesync.PlaceReq{Group: "/g", RouteKey: "rk"}); !res.NoNode || res.Error != "" {
		t.Fatalf("removed group placement=%+v, want NoNode", res)
	}
}

func testServiceWithGroups(t *testing.T, groups ...clusterstate.SandboxGroupRecord) *Service {
	t.Helper()
	src := testGroupSource(t, groups...)
	return NewRemoteLinksWithGroups(nil, src, testImportSources("test", src), clustercfg.PlacementConfig{Candidates: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

func TestConfiguredGroupInputsKeepsSourcesIndependent(t *testing.T) {
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
	if len(inputs.Sources) != 2 || inputs.Sources[0].SourceID != "source-a" || inputs.Sources[1].SourceID != "source-b" {
		t.Fatalf("sources not preserved independently: %+v", inputs.Sources)
	}
	if _, ok := inputs.Provider.(clusterstate.SandboxGroupImporter); ok {
		t.Fatal("configured provider unexpectedly exposes a merged Range importer")
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
	if _, err := src.Range(context.Background(), "", 10); err == nil {
		t.Fatal("duplicate group inside one source should fail Range")
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

func putNodeList(t *testing.T, svc *Service, n clusterstate.NodeListEntry) {
	t.Helper()
	raw, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	sink := svc.nodes.source("test")
	sink.reset()
	sink.put(n.NodeID, raw)
	sink.bookmark()
}
