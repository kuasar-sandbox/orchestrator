package conductorapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/filestore"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
)

// TLSPurpose identifies one core-owned TLS policy whose material may be
// supplied by a custom App.
type TLSPurpose string

const (
	TLSPurposeAPI            TLSPurpose = "api"
	TLSPurposeNodeLinkClient TLSPurpose = "node-link-client"
)

// TLSMaterial contains only process-local key and trust material. Policy such
// as protocol versions, ALPN, client authentication, and verification remains
// owned by the conductor core.
type TLSMaterial struct {
	CertificateChain [][]byte
	PrivateKey       crypto.Signer
	RootCAs          *x509.CertPool
	ClientCAs        *x509.CertPool
}

// Bindings are the internal adapters populated by the public conductor App.
// Nil providers retain the built-in file/environment behavior.
type Bindings struct {
	Logger                 *slog.Logger
	TLSMaterial            func(context.Context, TLSPurpose) (TLSMaterial, error)
	EncryptionKeys         func(context.Context) ([][]byte, error)
	ObjectStoreCredentials filestore.CredentialsProvider
}

// Runtime is the immutable, resolved process material consumed by Run.
type Runtime struct {
	Logger      *slog.Logger
	SecretBox   *secretbox.Box
	Files       *filestore.Store
	APITLS      *tls.Config
	NodeLinkTLS *tls.Config
}

// ResolveRuntime resolves every provider before the durable store, listeners,
// systemd launcher, or workers are touched. A configured provider is
// authoritative: its error is returned and no declarative fallback is used.
func ResolveRuntime(ctx context.Context, cfg *publicconfig.Conductor, bindings Bindings) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("conductor runtime: config is required")
	}
	logger := bindings.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	var box *secretbox.Box
	var err error
	if bindings.EncryptionKeys != nil {
		keys, providerErr := bindings.EncryptionKeys(ctx)
		if providerErr != nil {
			return nil, fmt.Errorf("conductor runtime: encryption key provider: %w", providerErr)
		}
		box, err = secretbox.New(keys)
	} else {
		box, err = secretbox.NewFromColonHex(configresolve.EncryptionKeySpec(cfg))
	}
	if err != nil {
		return nil, fmt.Errorf("conductor runtime: encryption keys: %w", err)
	}

	apiTLS, nodeLinkTLS, err := resolveTLS(ctx, cfg, bindings.TLSMaterial)
	if err != nil {
		return nil, err
	}

	var files *filestore.Store
	if cfg.Builder.FilesStorage == nil {
		if bindings.ObjectStoreCredentials != nil {
			return nil, fmt.Errorf("conductor runtime: object-store credentials provider requires builder.files_storage")
		}
	} else if bindings.ObjectStoreCredentials != nil {
		files, err = filestore.NewWithCredentials(ctx, cfg.Builder.FilesStorage, bindings.ObjectStoreCredentials)
		if err != nil {
			return nil, fmt.Errorf("conductor runtime: object-store credentials provider: %w", err)
		}
	} else {
		// Preserve the established built-in behavior: a broken optional COPY
		// store disables COPY and is reported, while provider failures above are
		// always fatal and never fall back.
		files, err = filestore.New(cfg.Builder.FilesStorage)
		if err != nil {
			logger.Warn("builder.files_storage init failed; COPY steps will be rejected", "err", err)
			files = nil
		}
	}

	return &Runtime{Logger: logger, SecretBox: box, Files: files, APITLS: apiTLS, NodeLinkTLS: nodeLinkTLS}, nil
}

func resolveTLS(ctx context.Context, cfg *publicconfig.Conductor, provider func(context.Context, TLSPurpose) (TLSMaterial, error)) (*tls.Config, *tls.Config, error) {
	if provider == nil {
		var apiTLS *tls.Config
		if cfg.API.TLS.Cert != "" || cfg.API.TLS.Key != "" {
			cert, err := tls.LoadX509KeyPair(cfg.API.TLS.Cert, cfg.API.TLS.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("conductor runtime: api tls: %w", err)
			}
			apiTLS = serverTLSConfig(&cert, nil)
		}
		var nodeLinkTLS *tls.Config
		if cfg.Cluster.NodeLink.TLS.Cert != "" {
			client, err := (clustercfg.TLS{
				Cert: cfg.Cluster.NodeLink.TLS.Cert,
				Key:  cfg.Cluster.NodeLink.TLS.Key,
				CA:   cfg.Cluster.NodeLink.TLS.CA,
			}).ClientConfig("")
			if err != nil {
				return nil, nil, fmt.Errorf("conductor runtime: cluster node-link tls: %w", err)
			}
			nodeLinkTLS = client
		}
		return apiTLS, nodeLinkTLS, nil
	}

	apiMaterial, err := provider(ctx, TLSPurposeAPI)
	if err != nil {
		return nil, nil, fmt.Errorf("conductor runtime: TLS provider for %s: %w", TLSPurposeAPI, err)
	}
	apiCert, err := certificateFromMaterial(apiMaterial)
	if err != nil {
		return nil, nil, fmt.Errorf("conductor runtime: TLS provider for %s: %w", TLSPurposeAPI, err)
	}
	var apiTLS *tls.Config
	if apiCert != nil || apiMaterial.ClientCAs != nil {
		if apiCert == nil {
			return nil, nil, fmt.Errorf("conductor runtime: TLS provider for %s: server certificate is required", TLSPurposeAPI)
		}
		apiTLS = serverTLSConfig(apiCert, clonePool(apiMaterial.ClientCAs))
	}

	var nodeLinkTLS *tls.Config
	if cfg.Cluster.NodeLink.Endpoint != "" {
		nodeMaterial, err := provider(ctx, TLSPurposeNodeLinkClient)
		if err != nil {
			return nil, nil, fmt.Errorf("conductor runtime: TLS provider for %s: %w", TLSPurposeNodeLinkClient, err)
		}
		nodeCert, err := certificateFromMaterial(nodeMaterial)
		if err != nil {
			return nil, nil, fmt.Errorf("conductor runtime: TLS provider for %s: %w", TLSPurposeNodeLinkClient, err)
		}
		if nodeCert != nil || nodeMaterial.RootCAs != nil {
			nodeLinkTLS = &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, RootCAs: clonePool(nodeMaterial.RootCAs)}
			if nodeCert != nil {
				nodeLinkTLS.Certificates = []tls.Certificate{*nodeCert}
			}
		}
	}
	return apiTLS, nodeLinkTLS, nil
}

func certificateFromMaterial(material TLSMaterial) (*tls.Certificate, error) {
	if len(material.CertificateChain) == 0 && material.PrivateKey == nil {
		return nil, nil
	}
	if len(material.CertificateChain) == 0 || material.PrivateKey == nil {
		return nil, fmt.Errorf("certificate chain and private signer must be provided together")
	}
	chain := make([][]byte, len(material.CertificateChain))
	for i, certificate := range material.CertificateChain {
		if len(certificate) == 0 {
			return nil, fmt.Errorf("certificate chain contains an empty certificate")
		}
		if _, err := x509.ParseCertificate(certificate); err != nil {
			return nil, fmt.Errorf("parse certificate %d: %w", i, err)
		}
		chain[i] = append([]byte(nil), certificate...)
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
		Certificates: []tls.Certificate{*certificate},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2", "http/1.1"},
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
