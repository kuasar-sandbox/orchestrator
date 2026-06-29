package scaler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testManifestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestFileGroupSourceProviderMethods(t *testing.T) {
	src := testGroupSource(t, clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		AuthKey:       clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
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
	authKey, found, err := src.GetAuthKey(ctx, "/g")
	if err != nil || !found || authKey.Value != testAuthKey {
		t.Fatalf("auth=%+v found=%v err=%v", authKey, found, err)
	}
	page, err := src.Range(ctx, "", 10)
	if err != nil || len(page.Groups) != 1 || page.Groups[0] != "/g" {
		t.Fatalf("range=%+v err=%v", page, err)
	}
}

func TestFileGroupSourceAcceptsSecretShorthand(t *testing.T) {
	dir := t.TempDir()
	raw := `{"group":"/g","manifest_key":"` + testManifestKey + `","auth_key":"` + testAuthKey + `","node_selectors":[{"pool":"p"}]}`
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

func TestAnswerIncludesGroupMaterial(t *testing.T) {
	svc := testServiceWithGroups(t, clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		AuthKey:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
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
	want, err := clusterstate.DeriveAccessToken(testAuthKey, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.AccessToken != want || res.KeyFingerprint == "" {
		t.Fatalf("token/key mismatch: %+v want token %q", res, want)
	}
}

func TestFileRemovalStopsNewPlacement(t *testing.T) {
	dir := t.TempDir()
	writeGroupFile(t, dir, "g.json", clusterstate.SandboxGroupRecord{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		AuthKey:       clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
		TemplateRef:   "tmpl",
		NodeSelectors: []map[string]string{{"pool": "p"}},
	})
	src, err := NewFileGroupSource("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewRemoteLinksWithGroups(nil, src, src, clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	return NewRemoteLinksWithGroups(nil, src, src, clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testGroupSource(t *testing.T, groups ...clusterstate.SandboxGroupRecord) GroupSource {
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
