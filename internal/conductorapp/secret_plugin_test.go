package conductorapp

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestRuntimeResolvesPluginsBeforeUseAndPreservesOverrides(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "@test/encryption")
	der, signer, _ := testCertificate(t)
	certPath := filepath.Join(t.TempDir(), "server.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{9}, 32)
	broker := testsecret.Start(t, map[string][]byte{
		"encryption": key,
		"tls":        pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(signer)}),
		"access":     []byte("static-access"),
		"secret":     []byte("static-secret"),
	})
	cfg := &publicconfig.Conductor{
		EncryptionKey: "@test/missing-config-key",
		API:           publicconfig.APIConfig{TLS: publicconfig.TLSConfig{Cert: certPath, Key: "@test/tls"}},
		Builder: publicconfig.BuilderConfig{FilesStorage: &publicconfig.FilesStorageConfig{
			Region: "us-east-1", Bucket: "test-bucket", PresignExpiry: "1h", AccessKey: "@test/access", SecretKey: "@test/secret",
		}},
	}
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.APITLS == nil || runtime.Files == nil {
		t.Fatal("configured plugin material was not resolved")
	}
	record, err := runtime.SecretBox.EncryptString("@test/tenant-manifest")
	if err != nil {
		t.Fatal(err)
	}
	native, err := secretbox.New([][]byte{key})
	if err != nil {
		t.Fatal(err)
	}
	if value, err := native.DecryptString(record); err != nil || value != "@test/tenant-manifest" {
		t.Fatalf("runtime secretbox material mismatch: %q, %v", value, err)
	}
	if cfg.EncryptionKey != "@test/missing-config-key" || cfg.API.TLS.Key != "@test/tls" || cfg.Builder.FilesStorage.SecretKey != "@test/secret" {
		t.Fatal("runtime rewrote plugin references with material")
	}
	if broker.Count("missing-config-key") != 0 {
		t.Fatal("environment encryption key did not override configuration")
	}
	broker.Set("encryption", nil)
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{}); err == nil {
		t.Fatal("failed encryption plugin did not stop runtime construction")
	}
	beforeTLS, beforeAccess := broker.Count("tls"), broker.Count("access")
	provider := &countingCredentialsProvider{value: filestore.Credentials{AccessKeyID: "provider-access", SecretAccessKey: "provider-secret"}}
	runtime, err = ResolveRuntime(context.Background(), cfg, Bindings{
		EncryptionKeys: func(context.Context) ([][]byte, error) { return [][]byte{key}, nil },
		TLSMaterial: func(context.Context, TLSPurpose) (TLSMaterial, error) {
			return TLSMaterial{CertificateChain: [][]byte{der}, PrivateKey: signer}, nil
		},
		ObjectStoreCredentials: provider,
	})
	if err != nil || runtime == nil || provider.calls != 1 {
		t.Fatalf("application providers lost priority: %v", err)
	}
	if broker.Count("tls") != beforeTLS || broker.Count("access") != beforeAccess {
		t.Fatal("ignored plugin configuration was resolved despite application providers")
	}
	broker.Set("encryption", key)
	broker.Set("access", nil)
	runtime, err = ResolveRuntime(context.Background(), cfg, Bindings{})
	if err != nil || runtime.Files != nil {
		t.Fatalf("optional files-store failure semantics changed: %v", err)
	}
}
