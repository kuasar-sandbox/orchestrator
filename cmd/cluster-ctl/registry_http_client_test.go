package main

import (
	"crypto/tls"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

func TestAuthenticatedHTTPClientHasBoundedAttempts(t *testing.T) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	client := boundedAuthenticatedHTTPClient(tlsConfig, 35*time.Second)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", client.Transport)
	}
	if client.Timeout != 36*time.Second {
		t.Fatalf("client timeout = %s", client.Timeout)
	}
	if transport.DialContext == nil || transport.TLSHandshakeTimeout <= 0 ||
		transport.ResponseHeaderTimeout != 35*time.Second || transport.ResponseHeaderTimeout >= client.Timeout {
		t.Fatalf("unbounded transport: %+v", transport)
	}
	if transport.TLSClientConfig != tlsConfig || !transport.ForceAttemptHTTP2 {
		t.Fatal("authenticated transport lost TLS identity or HTTP/2 support")
	}
}

func TestRegistryPeerTimeoutCoversMaximumPermitDrain(t *testing.T) {
	storage := clustercfg.ConsensusStorage{OperationTimeout: "7s"}
	want := time.Duration(raftstore.MaximumServePermitMillis)*time.Millisecond + 7*time.Second
	if got := registryPeerResponseTimeout(storage); got != want {
		t.Fatalf("Registry peer timeout = %s, want %s", got, want)
	}
}

func TestRouterBackgroundFailureOverridesCleanHTTPShutdown(t *testing.T) {
	want := errors.New("Permit refresh failed")
	background := make(chan error, 1)
	background <- want
	if got := routerBackgroundError(background, nil); !errors.Is(got, want) {
		t.Fatalf("Router background error = %v", got)
	}
	fallback := errors.New("listen failed")
	if got := routerBackgroundError(make(chan error), fallback); !errors.Is(got, fallback) {
		t.Fatalf("Router fallback error = %v", got)
	}
}

func TestUnixListenerRefusesToRemoveRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.sock")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if listener, err := listenClusterEndpoint(path); err == nil {
		listener.Close()
		t.Fatal("regular file was accepted as a stale Unix socket")
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "keep" {
		t.Fatalf("regular listener path was changed: %q, %v", raw, err)
	}
}

func TestCertificateAuthenticatedServerRejectsUnixListener(t *testing.T) {
	server, err := newClusterHTTPServer("registry", filepath.Join(t.TempDir(), "registry.sock"),
		clustercfg.TLS{Cert: "cert", Key: "key", CA: "ca"}, http.NotFoundHandler())
	if err == nil {
		server.listener.Close()
		t.Fatal("certificate-authenticated Unix listener was accepted")
	}
}
