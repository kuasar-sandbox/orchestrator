package scaler

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const testAuthKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
const testManifestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestImportGroupsAcceptsSecretShorthand(t *testing.T) {
	raw := `{"type":"group","group":{"group":"/g","manifest_key":"` + testManifestKey + `","auth_key":"` + testAuthKey + `","node_selectors":[{"pool":"p"}]}}`
	groups, sum, err := importGroups(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Groups != 1 || len(groups) != 1 {
		t.Fatalf("summary=%+v groups=%d", sum, len(groups))
	}
	if groups[0].ManifestKey.Type != clusterstate.SecretInline || groups[0].AuthKey.Type != clusterstate.SecretInline {
		t.Fatalf("secret shorthand not normalized: %+v", groups[0])
	}
}

func TestAnswerIncludesGroupMaterial(t *testing.T) {
	svc := NewRemote("127.0.0.1:1", nil, clustercfg.PlacementConfig{Candidates: 1}, 30, slog.New(slog.NewTextHandler(io.Discard, nil)))
	putNodeList(t, svc, clusterstate.NodeListEntry{NodeID: "n1", Labels: map[string]string{"pool": "p"}})
	svc.ImportGroups([]clusterstate.SandboxGroupRecord{{
		Group: "/g", ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: testManifestKey},
		AuthKey:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: testAuthKey},
		TemplateRef: "tmpl", Config: map[string]string{"a": "1"}, NodeSelectors: []map[string]string{{"pool": "p"}},
	}})

	res := svc.answer(&routesync.PlaceReq{Group: "/g", RouteKey: "rk", SandboxID: "sb-1", Config: map[string]string{"b": "2"}})
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
