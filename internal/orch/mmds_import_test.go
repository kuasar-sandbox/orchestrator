package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func mmdsMigrationFixture(t *testing.T) (*Orchestrator, string, string, *types.Sandbox, string) {
	t.Helper()
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := mmdsFeatureConfig()
	cfg.Sandbox.Boot.Runtime = runtimePath
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	o := testOrchCfgAt(t, cfg, filepath.Join(dir, "node.db"))
	manifestKey := strings.Repeat("6", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{APISecret: apiSecret, ManifestKey: manifestKey}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "source", manifestKey, "manifest://"+strings.Repeat("b", 64))
	source.Metadata = map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"key"},{"path":"/later","type":"secret","secret":"later"}]}`}
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	return o, apiSecret, apiKey, source, token
}

func TestStandaloneImportUsesTokenRoutesAndRequestSecretsOnly(t *testing.T) {
	o, _, apiKey, _, token := mmdsMigrationFixture(t)
	header := `{"secrets":{"key":"import-value"}}`
	target, err := o.prepareStandaloneTargetWithMMDS(context.Background(), "target", apiKey, token, map[string]string{"ordinary": "ignored"}, &header)
	if err != nil {
		t.Fatal(err)
	}
	if target.State != types.StatePaused {
		t.Fatalf("target state = %q", target.State)
	}
	raw := target.Metadata[sandboxcfg.NsMMDS]
	if raw != `{"routes":[{"path":"/secret","type":"secret","secret":"key"},{"path":"/later","type":"secret","secret":"later"}]}` || strings.Contains(raw, "import-value") {
		t.Fatal("imported routes mismatch or contain secret material")
	}
	if _, exists := target.Metadata["ordinary"]; exists {
		t.Fatal("request metadata was imported")
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(context.Background(), store.MMDSRouteSecretOwnerSandbox, target.ID, sandboxcfg.MMDSRoutesDigest(raw))
	if err != nil || !found || string(values["key"]) != "import-value" {
		t.Fatalf("imported values metadata mismatch: found=%t err=%v", found, err)
	}
}

func TestStandaloneImportRejectsAnyRequestRoutesKey(t *testing.T) {
	for _, tt := range []struct {
		name     string
		metadata map[string]string
		header   *string
	}{
		{name: "metadata", metadata: map[string]string{sandboxcfg.NsMMDS: `{"routes":[]}`}},
		{name: "header", header: stringPointer(`{"routes":[]}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o, _, apiKey, _, token := mmdsMigrationFixture(t)
			_, err := o.prepareStandaloneTargetWithMMDS(context.Background(), "target", apiKey, token, tt.metadata, tt.header)
			if !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("error = %v", err)
			}
			if target, getErr := o.st.Get(context.Background(), "target"); getErr != nil || target != nil {
				t.Fatalf("failed import target cleanup mismatch: found=%t err=%v", target != nil, getErr)
			}
		})
	}
}

func TestStandaloneImportFailureLeavesNoSecretRow(t *testing.T) {
	o, _, apiKey, _, token := mmdsMigrationFixture(t)
	header := `{"secrets":{"undeclared":"value"}}`
	if _, err := o.prepareStandaloneTargetWithMMDS(context.Background(), "target", apiKey, token, nil, &header); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("error = %v", err)
	}
	if target, err := o.st.Get(context.Background(), "target"); err != nil || target != nil {
		t.Fatalf("failed import target cleanup mismatch: found=%t err=%v", target != nil, err)
	}
	if _, _, found, err := o.st.GetMMDSRouteSecretValues(context.Background(), store.MMDSRouteSecretOwnerSandbox, "target", "unused"); err != nil || found {
		t.Fatalf("failed import left secret row: found=%t err=%v", found, err)
	}
}

func TestStandaloneImportRejectsInvalidTokenRoutesWithoutRows(t *testing.T) {
	o, _, apiKey, source, _ := mmdsMigrationFixture(t)
	source.Metadata[sandboxcfg.NsMMDS] = `{"routes":[{"path":"/invalid/"}]}`
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.prepareStandaloneTargetWithMMDS(context.Background(), "target", apiKey, token, nil, nil); !errors.Is(err, migrationtoken.ErrInvalidPayload) {
		t.Fatalf("error = %v", err)
	}
	if target, err := o.st.Get(context.Background(), "target"); err != nil || target != nil {
		t.Fatalf("invalid token target cleanup mismatch: found=%t err=%v", target != nil, err)
	}
}

func TestConnectExistingTargetIgnoresMMDSInputAndToken(t *testing.T) {
	o, apiSecret, apiKey, source, _ := mmdsMigrationFixture(t)
	source.ID = "existing"
	source.State = types.StateRunning
	source.RunID = "run-existing"
	source.ServiceSecret, source.EnvdAccessToken, source.TrafficAccessToken, source.ForwardAccessToken = "", "", "", ""
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	raw := source.Metadata[sandboxcfg.NsMMDS]
	if err := o.st.InsertSandboxWithMMDSRouteSecretValues(
		context.Background(), source, sandboxcfg.MMDSRoutesDigest(raw), store.MMDSRouteSecretValues{"key": []byte("old")},
	); err != nil {
		t.Fatal(err)
	}
	o.cache(source)
	malformedHeader := "not-json"
	got, err := o.ConnectWithMMDS(
		context.Background(), source.ID, apiKey, strings.Repeat("x", migrationtoken.MaxWireSize+1), 0,
		map[string]string{sandboxcfg.NsMMDS: `{"routes":[],"secrets":{"key":"new"}}`}, &malformedHeader,
	)
	if err != nil || got == nil || got.ID != source.ID {
		t.Fatalf("ConnectWithMMDS mismatch: found=%t err=%v", got != nil, err)
	}
	if got.APISecret != apiSecret {
		t.Fatal("existing target identity changed")
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(context.Background(), store.MMDSRouteSecretOwnerSandbox, source.ID, sandboxcfg.MMDSRoutesDigest(raw))
	if err != nil || !found || string(values["key"]) != "old" {
		t.Fatalf("existing values changed: found=%t err=%v", found, err)
	}
}

func stringPointer(value string) *string { return &value }
