package cluster

import (
	"strings"
	"testing"
)

func TestCanonicalServiceEndpoints(t *testing.T) {
	for _, endpoint := range []string{"node-1:8443", "127.0.0.1:443", "[2001:db8::1]:8443"} {
		if err := ValidateTCPDataEndpoint(endpoint); err != nil {
			t.Fatalf("TCP endpoint %q: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"", "https://node-1:8443", "node-1:08443", "node-1:0", "node-1:8443/", " node-1:8443"} {
		if err := ValidateTCPDataEndpoint(endpoint); err == nil {
			t.Fatalf("noncanonical TCP endpoint %q was accepted", endpoint)
		}
	}
	if err := ValidateTCPDataEndpoint(strings.Repeat("a", maxCanonicalEndpointBytes) + ":443"); err == nil {
		t.Fatal("oversized TCP endpoint was accepted")
	}
	if err := ValidateCanonicalHTTPSBaseEndpoint("https://registry-a:9443"); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{
		"http://registry-a:9443", "https://registry-a:9443/", "https://registry-a:9443/path",
		"https://registry-a:9443?query=1", "https://user@registry-a:9443", "https://REGISTRY-A:9443",
		"https://registry-a:09443",
	} {
		if err := ValidateCanonicalHTTPSBaseEndpoint(endpoint); err == nil {
			t.Fatalf("noncanonical HTTPS endpoint %q was accepted", endpoint)
		}
	}
	if err := ValidateCanonicalHTTPSBaseEndpoint("https://" + strings.Repeat("a", maxCanonicalEndpointBytes)); err == nil {
		t.Fatal("oversized HTTPS endpoint was accepted")
	}
}
