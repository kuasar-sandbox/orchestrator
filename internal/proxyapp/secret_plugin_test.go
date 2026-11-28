package proxyapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestRuntimeResolvesPluginPrivateKey(t *testing.T) {
	der, signer, certificate := testProxyCertificate(t)
	certPath := filepath.Join(t.TempDir(), "proxy.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	broker := testsecret.Start(t, map[string][]byte{
		"proxy-key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(signer)}),
	})
	cfg := testProxyConfig(t)
	cfg.TLS.Cert, cfg.TLS.Key = certPath, "@test/proxy-key"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ResolveRuntime(ctx, cfg, Bindings{}); !errors.Is(err, context.Canceled) || broker.Count("proxy-key") != 0 {
		t.Fatalf("cancelled initialization resolved key: %v", err)
	}
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.DataTLS == nil || runtime.DataTLS.MinVersion != tls.VersionTLS12 ||
		runtime.DataTLS.ClientAuth != tls.NoClientCert ||
		len(runtime.DataTLS.NextProtos) != 2 || runtime.DataTLS.NextProtos[0] != "h2" || runtime.DataTLS.NextProtos[1] != "http/1.1" ||
		!bytes.Equal(runtime.DataTLS.Certificates[0].Certificate[0], der) {
		t.Fatal("plugin material changed proxy TLS policy or certificate")
	}
	broker.Set("proxy-key", nil)
	message := []byte("bound proxy TLS material")
	digest := sha256.Sum256(message)
	signature, err := runtime.DataTLS.Certificates[0].PrivateKey.(crypto.Signer).Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := certificate.CheckSignature(x509.SHA256WithRSA, message, signature); err != nil || broker.Count("proxy-key") != 1 {
		t.Fatalf("existing runtime did not retain usable key material: %v", err)
	}
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{}); err == nil {
		t.Fatal("missing plugin key did not fail initialization")
	}
	before := broker.Count("proxy-key")
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{
		TLSMaterial: func(context.Context) (TLSMaterial, error) {
			return TLSMaterial{CertificateChain: [][]byte{der}, PrivateKey: signer}, nil
		},
	}); err != nil || broker.Count("proxy-key") != before {
		t.Fatalf("application provider did not override the plugin reference: %v", err)
	}
	if cfg.TLS.Cert != certPath || cfg.TLS.Key != "@test/proxy-key" {
		t.Fatal("resolved material replaced TLS configuration")
	}
}
