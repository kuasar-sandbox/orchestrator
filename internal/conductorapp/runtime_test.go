package conductorapp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
)

func TestResolveRuntimeProviderPriorityAndFailure(t *testing.T) {
	cfg := &publicconfig.Conductor{EncryptionKey: "not valid hex"}
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{
		EncryptionKeys: func(context.Context) ([][]byte, error) {
			return [][]byte{make([]byte, 32)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.SecretBox == nil || runtime.Logger == nil {
		t.Fatalf("runtime=%+v", runtime)
	}
	if _, err := runtime.SecretBox.Encrypt([]byte("secret")); err != nil {
		t.Fatal(err)
	}

	cfg.EncryptionKey = strings.Repeat("f", 64)
	_, err = ResolveRuntime(context.Background(), cfg, Bindings{
		EncryptionKeys: func(context.Context) ([][]byte, error) {
			return nil, errors.New("kms unavailable")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "kms unavailable") {
		t.Fatalf("provider error=%v", err)
	}
}

func TestResolveRuntimeRejectsMissingEncryptionMaterial(t *testing.T) {
	t.Setenv("NODE_CONFIG_ENCRYPTION_KEY", "")
	_, err := ResolveRuntime(context.Background(), &publicconfig.Conductor{}, Bindings{})
	if err == nil || !strings.Contains(err.Error(), "encryption keys") {
		t.Fatalf("missing encryption material error = %v", err)
	}
}

func TestResolveRuntimeTLSProviderKeepsCorePolicy(t *testing.T) {
	der, signer, certificate := testCertificate(t)
	clientPool := x509.NewCertPool()
	clientPool.AddCert(certificate)
	rootPool := x509.NewCertPool()
	rootPool.AddCert(certificate)
	cfg := &publicconfig.Conductor{
		EncryptionKey: strings.Repeat("1", 64),
		Cluster:       publicconfig.ClusterConfig{NodeLink: publicconfig.ClusterNodeLink{Endpoint: "registry:7443"}},
	}
	var mu sync.Mutex
	var purposes []TLSPurpose
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{
		TLSMaterial: func(_ context.Context, purpose TLSPurpose) (TLSMaterial, error) {
			mu.Lock()
			purposes = append(purposes, purpose)
			mu.Unlock()
			switch purpose {
			case TLSPurposeAPI:
				return TLSMaterial{CertificateChain: [][]byte{der}, PrivateKey: signer, ClientCAs: clientPool}, nil
			case TLSPurposeNodeLinkClient:
				return TLSMaterial{CertificateChain: [][]byte{der}, PrivateKey: signer, RootCAs: rootPool}, nil
			default:
				return TLSMaterial{}, errors.New("unexpected purpose")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(purposes) != 2 || purposes[0] != TLSPurposeAPI || purposes[1] != TLSPurposeNodeLinkClient {
		t.Fatalf("purposes=%v", purposes)
	}
	if runtime.APITLS == nil || runtime.APITLS.MinVersion != tls.VersionTLS12 || runtime.APITLS.ClientAuth != tls.RequireAndVerifyClientCert ||
		len(runtime.APITLS.NextProtos) != 2 || runtime.APITLS.NextProtos[0] != "h2" || runtime.APITLS.NextProtos[1] != "http/1.1" {
		t.Fatalf("api TLS policy=%+v", runtime.APITLS)
	}
	if runtime.APITLS.ClientCAs == clientPool {
		t.Fatal("API ClientCAs was not cloned")
	}
	if runtime.NodeLinkTLS == nil || runtime.NodeLinkTLS.MinVersion != tls.VersionTLS12 || len(runtime.NodeLinkTLS.NextProtos) != 1 || runtime.NodeLinkTLS.NextProtos[0] != "h2" {
		t.Fatalf("node-link TLS policy=%+v", runtime.NodeLinkTLS)
	}
	if runtime.NodeLinkTLS.RootCAs == rootPool {
		t.Fatal("node-link RootCAs was not cloned")
	}
	der[0] ^= 0xff
	if runtime.APITLS.Certificates[0].Certificate[0][0] == der[0] {
		t.Fatal("certificate bytes were not cloned")
	}
}

func TestResolveRuntimeTLSProviderErrorDoesNotUseFiles(t *testing.T) {
	cfg := &publicconfig.Conductor{
		EncryptionKey: strings.Repeat("2", 64),
		API:           publicconfig.APIConfig{TLS: publicconfig.TLSConfig{Cert: "/would/fallback.crt", Key: "/would/fallback.key"}},
	}
	_, err := ResolveRuntime(context.Background(), cfg, Bindings{
		TLSMaterial: func(context.Context, TLSPurpose) (TLSMaterial, error) {
			return TLSMaterial{}, errors.New("hsm unavailable")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "hsm unavailable") {
		t.Fatalf("error=%v", err)
	}
}

func TestResolveRuntimeRejectsInvalidTLSProviderMaterial(t *testing.T) {
	der, _, _ := testCertificate(t)
	_, wrongSigner, _ := testCertificate(t)
	cfg := &publicconfig.Conductor{EncryptionKey: strings.Repeat("2", 64)}
	for name, material := range map[string]TLSMaterial{
		"mismatched signer": {CertificateChain: [][]byte{der}, PrivateKey: wrongSigner},
		"bad chain":         {CertificateChain: [][]byte{der, []byte("not a certificate")}, PrivateKey: wrongSigner},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ResolveRuntime(context.Background(), cfg, Bindings{
				TLSMaterial: func(context.Context, TLSPurpose) (TLSMaterial, error) { return material, nil },
			})
			if err == nil {
				t.Fatal("invalid TLS material accepted")
			}
		})
	}
}

func TestResolveRuntimeObjectStoreProviderIsPrimedAndRequired(t *testing.T) {
	cfg := &publicconfig.Conductor{
		EncryptionKey: strings.Repeat("3", 64),
		Builder: publicconfig.BuilderConfig{FilesStorage: &publicconfig.FilesStorageConfig{
			Region: "us-east-1", Bucket: "test-bucket", PresignExpiry: "1h",
			AccessKey: "static-fallback", SecretKey: "static-fallback-secret",
		}},
	}
	provider := &countingCredentialsProvider{value: filestore.Credentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{ObjectStoreCredentials: provider})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Files == nil || provider.calls != 1 {
		t.Fatalf("files=%v provider calls=%d", runtime.Files, provider.calls)
	}

	provider.err = errors.New("vault unavailable")
	provider.calls = 0
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{ObjectStoreCredentials: provider}); err == nil || !strings.Contains(err.Error(), "vault unavailable") {
		t.Fatalf("provider error=%v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls=%d", provider.calls)
	}

	cfg.Builder.FilesStorage = nil
	if _, err := ResolveRuntime(context.Background(), cfg, Bindings{ObjectStoreCredentials: provider}); err == nil || !strings.Contains(err.Error(), "requires builder.files_storage") {
		t.Fatalf("unused provider error=%v", err)
	}
}

type countingCredentialsProvider struct {
	value filestore.Credentials
	err   error
	calls int
}

func (p *countingCredentialsProvider) Retrieve(context.Context) (filestore.Credentials, error) {
	p.calls++
	return p.value, p.err
}

func testCertificate(t *testing.T) ([]byte, *rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "custom.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return der, key, certificate
}
