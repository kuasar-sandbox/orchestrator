package transportauth

import (
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestVerifyRequestRequiresExactVerifiedRole(t *testing.T) {
	identity, _ := url.Parse("spiffe://kuasar.internal/router/router-a")
	certificate := &x509.Certificate{URIs: []*url.URL{identity}}
	request := httptest.NewRequest("GET", "https://registry.internal", nil)
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	if err := VerifyRequest(request, RoleRouter); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRequest(request, RoleRegistry); err == nil {
		t.Fatal("Router certificate was accepted as Registry")
	}
	request.TLS.VerifiedChains = nil
	if err := VerifyRequest(request, RoleRouter); err == nil {
		t.Fatal("unverified peer certificate was accepted")
	}
}
