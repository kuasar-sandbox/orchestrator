package workspacedisk

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestSecretPluginStorageBindsExplicitTenant(t *testing.T) {
	raw := bytes.Repeat([]byte{0xaa}, 32)
	broker := testsecret.Start(t, map[string][]byte{"tenant/Manifest_v1": raw})
	ref := "@test/tenant/Manifest_v1"
	// Neither a process-wide tenant nor the shared config may override the
	// explicit tenant selected for this operation.
	ambient := strings.Repeat("b", 64)
	t.Setenv("MANIFEST_KEY", ambient)
	cfgPath := filepath.Join(t.TempDir(), "manifest.yaml")
	configText := "manifest: {key: '" + strings.Repeat("c", 64) + "'}\ncrypto: {chunk: aes, manifest: aes}\n"
	if err := os.WriteFile(cfgPath, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}
	storage, err := StorageContext(t.Context(), cfgPath, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	bound, err := storage.CustomerKeyFunc()()
	if err != nil || !bytes.Equal(bound[:], raw) {
		t.Fatalf("explicit plugin tenant not bound: %v", err)
	}
	if broker.Count("tenant/Manifest_v1") != 1 {
		t.Fatal("storage resolved more than once during construction")
	}
	broker.Set("tenant/Manifest_v1", bytes.Repeat([]byte{0xdd}, 32))
	again, err := storage.CustomerKeyFunc()()
	if err != nil || again != bound || broker.Count("tenant/Manifest_v1") != 1 {
		t.Fatalf("existing storage changed its root after broker update: %v", err)
	}
	next, err := StorageContext(t.Context(), cfgPath, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	newKey, err := next.CustomerKeyFunc()()
	if err != nil || newKey == bound {
		t.Fatalf("new storage did not resolve independently: %v", err)
	}
	legacy, err := StorageContext(t.Context(), cfgPath, hex.EncodeToString(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	legacyKey, err := legacy.CustomerKeyFunc()()
	if err != nil || legacyKey != bound {
		t.Fatalf("native and plugin roots diverged: %v", err)
	}
	broker.Set("tenant/Manifest_v1", nil)
	if failed, err := StorageContext(t.Context(), cfgPath, ref); err == nil {
		failed.Close()
		t.Fatal("missing plugin root fell back to configured material")
	}
	if os.Getenv("MANIFEST_KEY") != ambient {
		t.Fatal("tenant key resolution rewrote the process environment")
	}
	after, err := os.ReadFile(cfgPath)
	if err != nil || string(after) != configText {
		t.Fatalf("key resolution rewrote manifest config: %v", err)
	}
}
