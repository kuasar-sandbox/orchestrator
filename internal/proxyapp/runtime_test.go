package proxyapp

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
	"testing"
	"time"
)

func TestResolveRuntimeTLSProviderKeepsCorePolicy(t *testing.T) {
	der, signer, certificate := testProxyCertificate(t)
	clientPool := x509.NewCertPool()
	clientPool.AddCert(certificate)
	runtime, err := ResolveRuntime(context.Background(), testProxyConfig(t), Bindings{
		TLSMaterial: func(context.Context) (TLSMaterial, error) {
			return TLSMaterial{CertificateChain: [][]byte{der}, PrivateKey: signer, ClientCAs: clientPool}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Logger == nil || runtime.DataTLS == nil || runtime.DataTLS.MinVersion != tls.VersionTLS12 ||
		runtime.DataTLS.ClientAuth != tls.RequireAndVerifyClientCert ||
		len(runtime.DataTLS.NextProtos) != 2 || runtime.DataTLS.NextProtos[0] != "h2" || runtime.DataTLS.NextProtos[1] != "http/1.1" {
		t.Fatalf("runtime=%+v TLS=%+v", runtime, runtime.DataTLS)
	}
	if runtime.DataTLS.ClientCAs == clientPool {
		t.Fatal("ClientCAs was not cloned")
	}
	der[0] ^= 0xff
	if runtime.DataTLS.Certificates[0].Certificate[0][0] == der[0] {
		t.Fatal("certificate bytes were not cloned")
	}
}

func TestResolveRuntimeProviderIsAuthoritative(t *testing.T) {
	cfg := testProxyConfig(t)
	cfg.TLS.Cert = "/would/fallback.crt"
	cfg.TLS.Key = "/would/fallback.key"
	runtime, err := ResolveRuntime(context.Background(), cfg, Bindings{
		TLSMaterial: func(context.Context) (TLSMaterial, error) { return TLSMaterial{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.DataTLS != nil {
		t.Fatal("empty authoritative material did not disable TLS")
	}
	_, err = ResolveRuntime(context.Background(), cfg, Bindings{
		TLSMaterial: func(context.Context) (TLSMaterial, error) { return TLSMaterial{}, errors.New("hsm unavailable") },
	})
	if err == nil || !strings.Contains(err.Error(), "hsm unavailable") {
		t.Fatalf("provider error=%v", err)
	}
}

func TestResolveRuntimeRejectsInvalidProviderMaterial(t *testing.T) {
	der, _, _ := testProxyCertificate(t)
	_, wrongSigner, _ := testProxyCertificate(t)
	for name, material := range map[string]TLSMaterial{
		"mismatch": {CertificateChain: [][]byte{der}, PrivateKey: wrongSigner},
		"partial":  {CertificateChain: [][]byte{der}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ResolveRuntime(context.Background(), testProxyConfig(t), Bindings{
				TLSMaterial: func(context.Context) (TLSMaterial, error) { return material, nil },
			})
			if err == nil {
				t.Fatal("invalid material accepted")
			}
		})
	}
}

func testProxyCertificate(t *testing.T) ([]byte, *rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "proxy.test"},
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
