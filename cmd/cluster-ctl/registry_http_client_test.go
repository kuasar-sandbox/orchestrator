package main

import (
	"crypto/tls"
	"net/http"
	"testing"
	"time"
)

func TestAuthenticatedHTTPClientHasBoundedAttempts(t *testing.T) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	client := boundedAuthenticatedHTTPClient(tlsConfig)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", client.Transport)
	}
	if client.Timeout <= 0 || client.Timeout > 10*time.Second {
		t.Fatalf("client timeout = %s", client.Timeout)
	}
	if transport.DialContext == nil || transport.TLSHandshakeTimeout <= 0 ||
		transport.ResponseHeaderTimeout <= 0 || transport.ResponseHeaderTimeout >= client.Timeout {
		t.Fatalf("unbounded transport: %+v", transport)
	}
	if transport.TLSClientConfig != tlsConfig || !transport.ForceAttemptHTTP2 {
		t.Fatal("authenticated transport lost TLS identity or HTTP/2 support")
	}
}
