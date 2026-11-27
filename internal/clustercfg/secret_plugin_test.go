package clustercfg

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/testsecret"
)

func TestPluginTLSMutualHandshakeAndBoundMaterial(t *testing.T) {
	dir := t.TempDir()
	ca, caKey, caPath, _ := genCert(t, dir, "ca", nil, nil, true)
	_, _, serverCert, serverKey := genCert(t, dir, "server", ca, caKey, false)
	_, _, clientCert, clientKey := genCert(t, dir, "client", ca, caKey, false)
	serverPEM, err := os.ReadFile(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientPEM, err := os.ReadFile(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	broker := testsecret.Start(t, map[string][]byte{"server": serverPEM, "client": clientPEM})
	for _, path := range []string{serverKey, clientKey} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	serverConfig := TLS{Cert: serverCert, Key: "@test/server", CA: caPath}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := serverConfig.ServerConfigContext(cancelled); !errors.Is(err, context.Canceled) || broker.Count("server") != 0 {
		t.Fatalf("cancelled TLS setup resolved a key: %v", err)
	}
	serverTLS, err := serverConfig.ServerConfigContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := (TLS{Cert: clientCert, Key: "@test/client", CA: caPath}).ClientConfigContext(context.Background(), "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if serverTLS.ClientAuth != tls.RequireAndVerifyClientCert || clientTLS.ServerName != "localhost" || serverTLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("plugin material changed TLS policy")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	clientTLS.NextProtos = []string{"http/1.1"}
	transport := &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	checkHandshake := func() {
		t.Helper()
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatal(response.Status)
		}
	}
	checkHandshake()
	broker.Set("server", []byte("invalid PEM"))
	checkHandshake()
	if broker.Count("server") != 1 || broker.Count("client") != 1 {
		t.Fatal("TLS handshake fetched key material again")
	}
	if _, err := serverConfig.ServerConfig(); err == nil {
		t.Fatal("invalid plugin PEM was accepted")
	}
	broker.Set("server", clientPEM)
	if _, err := serverConfig.ServerConfig(); err == nil {
		t.Fatal("mismatched certificate and plugin private key were accepted")
	}
	broker.Set("server", nil)
	if _, err := serverConfig.ServerConfig(); err == nil {
		t.Fatal("missing plugin key did not fail")
	}
	if serverConfig.Key != "@test/server" {
		t.Fatal("resolved key replaced configuration reference")
	}
}
