package proxyapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
)

// TLSMaterial contains only process-local key and trust material. The proxy
// core retains protocol, ALPN, and client-auth policy.
type TLSMaterial struct {
	CertificateChain [][]byte
	PrivateKey       crypto.Signer
	ClientCAs        *x509.CertPool
}

// Bindings are process-local adapters populated by the public proxy App.
type Bindings struct {
	Logger          *slog.Logger
	TLSMaterial     func(context.Context) (TLSMaterial, error)
	MasterExtension proxyextension.MasterExtension
	WorkerExtension proxyextension.WorkerExtension
}

// Runtime is fully resolved before a master creates shared state/listeners or
// a worker advertises readiness.
type Runtime struct {
	Logger          *slog.Logger
	DataTLS         *tls.Config
	MasterExtension proxyextension.MasterExtension
	WorkerExtension proxyextension.WorkerExtension
}

// ResolveRuntime applies authoritative provider precedence while keeping TLS
// policy inside the core. A provider error never falls back to configured
// files.
func ResolveRuntime(ctx context.Context, cfg *publicconfig.Proxy, bindings Bindings) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("proxy runtime: config is required")
	}
	logger := bindings.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	var dataTLS *tls.Config
	if bindings.TLSMaterial != nil {
		material, err := bindings.TLSMaterial(ctx)
		if err != nil {
			return nil, fmt.Errorf("proxy runtime: TLS provider: %w", err)
		}
		certificate, err := certificateFromMaterial(material)
		if err != nil {
			return nil, fmt.Errorf("proxy runtime: TLS provider: %w", err)
		}
		if certificate != nil || material.ClientCAs != nil {
			if certificate == nil {
				return nil, fmt.Errorf("proxy runtime: TLS provider: server certificate is required")
			}
			dataTLS = serverTLSConfig(certificate, clonePool(material.ClientCAs))
		}
	} else if cfg.TLS.Cert != "" || cfg.TLS.Key != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.TLS.Cert, cfg.TLS.Key)
		if err != nil {
			return nil, fmt.Errorf("proxy runtime: data TLS: %w", err)
		}
		dataTLS = serverTLSConfig(&certificate, nil)
	}
	return &Runtime{
		Logger: logger, DataTLS: dataTLS,
		MasterExtension: bindings.MasterExtension, WorkerExtension: bindings.WorkerExtension,
	}, nil
}

func certificateFromMaterial(material TLSMaterial) (*tls.Certificate, error) {
	if len(material.CertificateChain) == 0 && material.PrivateKey == nil {
		return nil, nil
	}
	if len(material.CertificateChain) == 0 || material.PrivateKey == nil {
		return nil, fmt.Errorf("certificate chain and private signer must be provided together")
	}
	chain := make([][]byte, len(material.CertificateChain))
	for index, certificate := range material.CertificateChain {
		if len(certificate) == 0 {
			return nil, fmt.Errorf("certificate chain contains an empty certificate")
		}
		if _, err := x509.ParseCertificate(certificate); err != nil {
			return nil, fmt.Errorf("parse certificate %d: %w", index, err)
		}
		chain[index] = append([]byte(nil), certificate...)
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return nil, fmt.Errorf("parse leaf certificate: %w", err)
	}
	leafKey, err := x509.MarshalPKIXPublicKey(leaf.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal leaf public key: %w", err)
	}
	signerKey, err := x509.MarshalPKIXPublicKey(material.PrivateKey.Public())
	if err != nil {
		return nil, fmt.Errorf("marshal signer public key: %w", err)
	}
	if !bytes.Equal(leafKey, signerKey) {
		return nil, fmt.Errorf("private signer does not match leaf certificate")
	}
	return &tls.Certificate{Certificate: chain, PrivateKey: material.PrivateKey, Leaf: leaf}, nil
}

func serverTLSConfig(certificate *tls.Certificate, clientCAs *x509.CertPool) *tls.Config {
	config := &tls.Config{
		Certificates: []tls.Certificate{*certificate}, MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}
	if clientCAs != nil {
		config.ClientCAs = clientCAs
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config
}

func clonePool(pool *x509.CertPool) *x509.CertPool {
	if pool == nil {
		return nil
	}
	return pool.Clone()
}
